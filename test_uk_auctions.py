"""Auction capacity, Telegram pagination and signed one-tap intake; no outside calls."""
import re
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
import pytest
from test_coordinator_uk import scanner, forbid_unmocked_network


class StopWorker(BaseException):
    pass


class AuctionDB:
    """Small DB stand-in exercising bound SQL and committed state across connections."""
    closed = False

    def __init__(self):
        self.exact = {}
        self.pending = []
        self.queued = []
        self.commits = 0
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.commits += 1


class Cursor:
    def __init__(self, db):
        self.db = db
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        sql = ' '.join(sql.split())
        self.db.statements.append((sql, params))
        if sql.startswith('INSERT INTO auction_reminders'):
            self.db.exact[str(params[0])] = tuple(params)
            return
        if sql.startswith('DELETE FROM auction_pending'):
            self.db.pending = [r for r in self.db.pending if r[0] != params[0]]
            return
        if 'FROM auction_reminders WHERE item_id=' in sql:
            row = self.db.exact.get(str(params[0]))
            self.rows = [] if row is None else [(row[3], *row[4:8], row[2], row[1])]
            return
        if 'FROM auction_reminders' in sql:
            records = sorted(self.db.exact.values(), key=lambda row: row[3])
            if 'reminder_60_sent' in sql:
                self.rows = [(*r[:4], *r[4:8]) for r in records]
            elif 'last_status_check' in sql:
                self.rows = [(*r[:4], None) for r in records]
            else:
                self.rows = [(*r[:4], r[8], r[9]) for r in records]
        elif 'FROM auction_pending' in sql:
            self.rows = list(self.db.pending)
        elif 'FROM auction_link_queue' in sql:
            self.rows = list(self.db.queued)
        else:
            raise AssertionError('Unexpected test query: ' + sql)
        if 'LIMIT %s' in sql and params[0] is not None:
            self.rows = self.rows[:params[0]]
        fixed = re.search(r'LIMIT (\d+)', sql)
        if fixed:
            self.rows = self.rows[:int(fixed.group(1))]

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


def save_many(scanner, monkeypatch, count, remaining=8 * 3600):
    db = AuctionDB()
    monkeypatch.setattr(scanner, 'get_db_connection', lambda *args: db)
    monkeypatch.setattr(scanner, 'wake_auction_workers', Mock())
    now = datetime.now(timezone.utc)
    for i in range(count):
        item_id = str(123456789000 + i)
        scanner.save_auction_reminder(item_id, f'https://www.ebay.co.uk/itm/{item_id}',
                                      'Camera <&> ' + '🎁' * 100, now + timedelta(seconds=remaining + i))
    return db


@pytest.mark.parametrize('count', [30, 60, 240])
def test_committed_auction_capacity_and_complete_list(scanner, monkeypatch, count):
    db = save_many(scanner, monkeypatch, count)
    assert db.commits == count
    assert len(scanner.list_active_auctions()) == count
    assert len(scanner.get_auctions_for_status_check()) == count
    assert len(scanner.list_active_auctions(limit=7)) == 7
    assert len(scanner.get_auctions_for_status_check(limit=7)) == 7
    sent = Mock(return_value=True)
    monkeypatch.setattr(scanner, 'send_telegram_message', sent)
    monkeypatch.setattr(scanner.time, 'sleep', lambda *_: None)
    assert scanner.send_auction_list()
    assert sent.call_count > 1
    deleted_ids = []
    titles = []
    for call in sent.call_args_list:
        text = call.args[0]
        assert len(text.encode('utf-16-le')) // 2 < 4096
        assert text.count('<b>') == text.count('</b>')
        assert 'Camera &lt;&amp;&gt;' in text
        assert call.kwargs['disable_preview']
        callbacks = [b['callback_data'] for row in call.kwargs['reply_markup']['inline_keyboard'] for b in row]
        page_ids = [c.split(':')[1] for c in callbacks if c.startswith('aucdel:')]
        assert all(f'/itm/{item_id}' in text for item_id in page_ids)
        deleted_ids.extend(page_ids)
        titles.extend(re.findall(r'\n(\d+)\) <b>', text))
    assert deleted_ids == list(db.exact)
    assert titles == [str(i) for i in range(1, count + 1)]
    all_callbacks = [b['callback_data'] for c in sent.call_args_list
                     for row in c.kwargs['reply_markup']['inline_keyboard'] for b in row]
    assert all_callbacks.count('aucdelall:ask') == 1


def test_mixed_pending_queue_lists_and_transition_duplicates(scanner, monkeypatch):
    db = save_many(scanner, monkeypatch, 30)
    now = datetime.now(timezone.utc)
    db.pending = [(str(223456789000 + i), '', 'Pending & camera', now, now + timedelta(hours=2),
                   '2h', '', now) for i in range(30)]
    db.queued = [(str(323456789000 + i), '', now, now) for i in range(30)]
    # Read snapshots may overlap during queue -> pending -> exact transitions.
    first = next(iter(db.exact))
    db.pending.append((first, '', 'duplicate', now, now, '', '', now))
    db.queued.append((first, '', now, now))
    db.queued.append((db.pending[0][0], '', now, now))
    assert len(scanner.list_pending_auctions()) == 31
    assert len(scanner.list_queued_auction_links()) == 32
    sent = Mock(return_value=True)
    monkeypatch.setattr(scanner, 'send_telegram_message', sent)
    monkeypatch.setattr(scanner.time, 'sleep', lambda *_: None)
    assert scanner.send_auction_list()
    callbacks = [b['callback_data'] for c in sent.call_args_list
                 for row in c.kwargs['reply_markup']['inline_keyboard'] for b in row]
    ids = [c[7:] for c in callbacks if c.startswith('aucdel:')]
    assert len(ids) == len(set(ids)) == 90
    assert all('Всего: 90' in c.args[0] for c in sent.call_args_list)


def test_list_delivery_failure_preserves_auctions(scanner, monkeypatch):
    db = save_many(scanner, monkeypatch, 30)
    sent = Mock(side_effect=[True, False, True])
    monkeypatch.setattr(scanner, 'send_telegram_message', sent)
    monkeypatch.setattr(scanner.time, 'sleep', lambda *_: None)
    assert scanner.send_auction_list() is False
    assert len(db.exact) == 30 and 'Аукционы сохранены' in sent.call_args.args[0]


@pytest.mark.parametrize('count,remaining', [(30, 45 * 60), (240, 45 * 60), (30, 4 * 60)])
def test_scheduler_reaches_all_saved_auctions(scanner, monkeypatch, count, remaining):
    db = save_many(scanner, monkeypatch, count, remaining=remaining)
    monkeypatch.setattr(scanner, 'db_ready_event', Mock())
    event = Mock()
    event.wait.side_effect = StopWorker
    monkeypatch.setattr(scanner, 'auction_reminder_wakeup_event', event)
    sent = Mock(return_value=True)
    marked, deleted = Mock(), Mock(return_value=True)
    monkeypatch.setattr(scanner, 'send_telegram_message', sent)
    monkeypatch.setattr(scanner, '_mark_reminders_sent', marked)
    monkeypatch.setattr(scanner, 'delete_auction_reminder', deleted)
    monkeypatch.setattr(scanner, '_delete_expired_auctions', Mock())
    with pytest.raises(StopWorker):
        scanner.auction_reminder_worker()
    assert sent.call_count == count
    urls = [c.kwargs['preview_url'] for c in sent.call_args_list]
    assert len(set(urls)) == count
    if remaining < 5 * 60:
        assert deleted.call_count == count and marked.call_count == 0
    else:
        assert marked.call_count == count and deleted.call_count == 0


def test_status_worker_reaches_later_due_items_with_same_network_budget(scanner, monkeypatch):
    now = datetime.now(timezone.utc)
    rows = [(str(123456789000+i), '', 'Camera', now + timedelta(hours=23),
             now if i < 100 else None) for i in range(150)]
    read = Mock(return_value=rows)
    monkeypatch.setattr(scanner, 'get_auctions_for_status_check', read)
    monkeypatch.setattr(scanner, 'get_due_pending_auctions', Mock(return_value=[]))
    monkeypatch.setattr(scanner, '_connection_outage_snapshot', lambda: (0, None, True))
    monkeypatch.setattr(scanner, 'db_ready_event', Mock())
    for name in ('main_discovery_active_event', 'memory_pressure_event', 'auction_user_job_active_event'):
        monkeypatch.setattr(scanner, name, Mock(is_set=Mock(return_value=False)))
    event = Mock()
    event.wait.side_effect = StopWorker
    monkeypatch.setattr(scanner, 'auction_status_wakeup_event', event)
    monkeypatch.setattr(scanner, '_cleanup_stale_pending_auctions', Mock())
    monkeypatch.setattr(scanner.time, 'sleep', lambda *_: None)
    verify = Mock()
    monkeypatch.setattr(scanner, 'verify_saved_auction', verify)
    with pytest.raises(StopWorker):
        scanner.auction_status_worker()
    read.assert_called_once_with()
    assert verify.call_count == scanner.AUCTION_STATUS_BATCH
    assert verify.call_args_list[0].args[0] == '123456789100'
    assert all(c.kwargs['max_reserve_proxies'] == 1 for c in verify.call_args_list)


@pytest.mark.parametrize('auction,bin_price,offer', [(False, False, False), (False, True, True),
                                                  (True, False, False), (True, True, True)])
def test_add_button_only_for_auction_formats(scanner, auction, bin_price, offer):
    item = dict(id='123456789001', auction=auction, has_buy_it_now=bin_price, best_offer=offer)
    keyboard = scanner.new_item_auction_keyboard(item)
    if not auction:
        assert keyboard is None
    else:
        button = keyboard['inline_keyboard'][0][0]
        assert button['text'] == '➕ Добавить'
        assert len(button['callback_data'].encode()) <= 64


def make_callback(scanner, item_id='123456789001'):
    keyboard = scanner.new_item_auction_keyboard(dict(id=item_id, auction=True))
    return dict(id='test-callback', data=keyboard['inline_keyboard'][0][0]['callback_data'],
                message=dict(message_id=99, chat=dict(id=scanner.TELEGRAM_CHAT_ID), reply_markup=keyboard))


@pytest.mark.parametrize('invalid', ['other_chat', 'changed_id', 'changed_signature', 'unsigned'])
def test_forged_or_wrong_chat_button_cannot_enqueue(scanner, monkeypatch, invalid):
    callback = make_callback(scanner)
    if invalid == 'other_chat':
        callback['message']['chat']['id'] = 'other-chat'
    elif invalid == 'changed_id':
        callback['data'] = callback['data'].replace('123456789001', '123456789002')
    elif invalid == 'changed_signature':
        callback['data'] = callback['data'][:-16] + '0' * 16
    else:
        callback['data'] = 'aucadd:123456789001'
    intake = Mock()
    monkeypatch.setattr(scanner, 'submit_auction_link', intake)
    monkeypatch.setattr(scanner, 'answer_callback_query', Mock())
    scanner.handle_telegram_callback(callback)
    intake.assert_not_called()


def test_button_intake_uses_durable_queue_and_duplicate_guard(scanner, monkeypatch):
    callback = make_callback(scanner)
    item_id, url = '123456789001', 'https://www.ebay.co.uk/itm/123456789001'
    queued = Mock(side_effect=[('key', item_id, url, None, True, None),
                              ('key', item_id, url, 101, False, {'state': 'queued'})])
    monkeypatch.setattr(scanner, 'enqueue_auction_link', queued)
    status, activate, duplicate = Mock(), Mock(), Mock()
    monkeypatch.setattr(scanner, 'send_telegram_message', Mock(return_value=101))
    monkeypatch.setattr(scanner, 'set_auction_link_status_message', status)
    monkeypatch.setattr(scanner, 'activate_auction_link_job', activate)
    monkeypatch.setattr(scanner, 'send_duplicate_auction_notice', duplicate)
    monkeypatch.setattr(scanner, 'answer_callback_query', Mock())
    edit = Mock()
    monkeypatch.setattr(scanner, 'edit_message_reply_markup', edit)
    scanner.handle_telegram_callback(callback)
    scanner.handle_telegram_callback(callback)
    assert queued.call_args_list[0].args == (url,)
    status.assert_called_once_with('key', 101)
    activate.assert_called_once_with('key')
    duplicate.assert_called_once()
    button = edit.call_args.args[1]['inline_keyboard'][0][0]
    assert button['text'] == '✅ Добавлен' and button['callback_data'] == callback['data']


def test_send_failure_does_not_lose_saved_button_job(scanner, monkeypatch):
    monkeypatch.setattr(scanner, 'enqueue_auction_link', Mock(return_value=(
        'key', '123456789001', 'https://www.ebay.co.uk/itm/123456789001', None, True, None)))
    monkeypatch.setattr(scanner, 'send_telegram_message', Mock(return_value=None))
    status, activate = Mock(), Mock()
    monkeypatch.setattr(scanner, 'set_auction_link_status_message', status)
    monkeypatch.setattr(scanner, 'activate_auction_link_job', activate)
    assert scanner.submit_auction_link('https://www.ebay.co.uk/itm/123456789001') == '123456789001'
    status.assert_not_called()
    activate.assert_called_once_with('key')


def test_database_failure_does_not_ack_button_as_saved(scanner, monkeypatch):
    monkeypatch.setattr(scanner, 'enqueue_auction_link', Mock(side_effect=RuntimeError('DB unavailable')))
    answer = Mock()
    monkeypatch.setattr(scanner, 'answer_callback_query', answer)
    with pytest.raises(RuntimeError):
        scanner.handle_telegram_callback(make_callback(scanner))
    answer.assert_not_called()


def test_more_than_twenty_pasted_links_use_same_intake(scanner, monkeypatch):
    urls = [f'https://www.ebay.co.uk/itm/{123456789000+i}' for i in range(30)]
    response = Mock(status_code=200)
    response.json.return_value = {'result': [dict(update_id=1, message=dict(
        chat=dict(id=scanner.TELEGRAM_CHAT_ID), text='\n'.join(urls)))]}
    monkeypatch.setattr(scanner.requests, 'get', Mock(side_effect=[response, StopWorker]))
    monkeypatch.setattr(scanner, 'get_bot_state', Mock(return_value='0'))
    monkeypatch.setattr(scanner, 'set_bot_state', Mock())
    intake = Mock()
    monkeypatch.setattr(scanner, 'submit_auction_link', intake)
    with pytest.raises(StopWorker):
        scanner.telegram_listener()
    assert [call.args[0] for call in intake.call_args_list] == urls


def test_real_parser_to_telegram_payload_only_adds_auction_buttons(scanner, monkeypatch):
    extras = ['', '<span>Buy It Now</span><span>Best Offer</span>',
              '<span class="s-item__bid-count">0 bids</span>',
              '<span class="s-item__bid-count">2 bids</span><span>Buy It Now</span><span>Best Offer</span>']
    html = '<html><ul id="srp-river-results">' + ''.join(
        f'<li class="s-item"><a class="s-item__link" href="https://www.ebay.co.uk/itm/{123456789000+i}">'
        '<h3 class="s-item__title">New listing Kit &amp; Camera &lt;Lens&gt;</h3></a>'
        '<span class="s-item__price">£20.00</span><span class="s-item__shipping">£4.00 postage</span>'
        + extra + '</li>' for i, extra in enumerate(extras)) + '</ul></html>'
    monkeypatch.setattr(scanner, 'fetch_ebay_html_with_retry', lambda: html)
    monkeypatch.setattr(scanner, '_memory_maintenance', Mock())
    monkeypatch.setattr(scanner, 'claim_new_seen_ids', lambda ids: set(ids))
    monkeypatch.setattr(scanner.time, 'sleep', lambda *_: None)
    transport = Mock(return_value={'ok': True, 'result': {'message_id': 101}})
    monkeypatch.setattr(scanner, '_telegram_post', transport)
    assert scanner.check_and_send_new_items() == 'ok'
    payloads = [c.args[1] for c in transport.call_args_list]
    assert len(payloads) == 4
    assert all('<b>Kit Camera Lens</b>' in p['text'] for p in payloads)
    assert ['reply_markup' in p for p in payloads] == [False, False, True, True]
    assert 'Аукцион / Buy It Now' in payloads[-1]['text']
