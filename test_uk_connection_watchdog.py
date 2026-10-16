"""Offline coverage for the outage alert; never send a real Telegram message."""

import threading
from html.parser import HTMLParser
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from test_coordinator_uk import scanner, forbid_unmocked_network


class StopWatchdog(BaseException):
    """Exit the infinite worker without its normal error handler swallowing it."""


@pytest.fixture
def watchdog(scanner, monkeypatch):
    clock = [1000.0]
    send = Mock(return_value=True)
    monkeypatch.setattr(scanner, 'time', SimpleNamespace(monotonic=lambda: clock[0], sleep=None))
    monkeypatch.setattr(scanner, 'connection_state_lock', threading.Lock())
    monkeypatch.setattr(scanner, 'connection_watch_started_at', 400.0)
    monkeypatch.setattr(scanner, 'last_ebay_success_at', None)
    monkeypatch.setattr(scanner, 'connection_alert_sent', False)
    monkeypatch.setattr(scanner, 'CONNECTION_ALERT_AFTER', 600)
    monkeypatch.setattr(scanner, 'CONNECTION_WATCHDOG_INTERVAL', 15)
    monkeypatch.setattr(scanner, 'is_paused', False)
    monkeypatch.setattr(scanner, 'send_telegram_message', send)
    monkeypatch.setattr(scanner, 'pipeline_metrics', Mock())
    monkeypatch.setattr(scanner, 'auction_status_wakeup_event', Mock())
    return SimpleNamespace(app=scanner, clock=clock, send=send)


def run_ticks(state, monkeypatch, ticks=1):
    sleeps = []

    def sleep(interval):
        sleeps.append(interval)
        if len(sleeps) >= ticks:
            raise StopWatchdog()

    monkeypatch.setattr(state.app.time, 'sleep', sleep)
    with pytest.raises(StopWatchdog):
        state.app.connection_watchdog()
    assert sleeps == [state.app.CONNECTION_WATCHDOG_INTERVAL] * ticks


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.hrefs.append(dict(attrs).get('href'))


@pytest.mark.parametrize('had_success', [False, True])
def test_alert_links_to_exact_normalized_search_and_disables_preview(watchdog, monkeypatch, had_success):
    app = watchdog.app
    raw_url = (
        'https://www.ebay.co.uk/sch/i.html?'
        '_nkw=Canon+%22EOS%22+%26+film&_udlo=15&_udhi=350&LH_BIN=1&'
        'LH_PrefLoc=2&LH_PrefLoc=3&_ipg=240&_sop=1#manual"\'&'
    )
    search_url = app.normalize_ebay_search_url(raw_url)
    monkeypatch.setattr(app, 'EBAY_SEARCH_URL', search_url)
    if had_success:
        app.last_ebay_success_at = 400.0

    run_ticks(watchdog, monkeypatch, ticks=2)

    watchdog.send.assert_called_once()
    message = watchdog.send.call_args.args[0]
    links = Links()
    links.feed(message)
    assert links.hrefs == [search_url]
    assert '&amp;' in message and '&quot;' in message and '&#x27;' in message
    params = parse_qs(urlsplit(links.hrefs[0]).query)
    assert params['_nkw'] == ['Canon "EOS" & film']
    assert params['_udlo'] == ['15'] and params['_udhi'] == ['350']
    assert params['LH_BIN'] == ['1']
    assert params['LH_PrefLoc'] == ['1'] and params['_ipg'] == ['60'] and params['_sop'] == ['10']
    assert ('после запуска' in message) is not had_success
    assert watchdog.send.call_args.kwargs == {'disable_preview': True}


@pytest.mark.parametrize('had_success', [False, True])
def test_threshold_and_single_alert_per_outage(watchdog, had_success):
    app = watchdog.app
    app.connection_watch_started_at = 1000.0
    app.last_ebay_success_at = 1000.0 if had_success else None
    watchdog.clock[0] = 1599.999
    assert app._claim_connection_outage_alert() is None
    assert app.connection_alert_sent is False

    watchdog.clock[0] = 1600.0
    claim = app._claim_connection_outage_alert()
    assert claim is not None and claim[0] == 600.0 and claim[1] is had_success
    assert app._claim_connection_outage_alert() is None
    watchdog.clock[0] += 3600
    assert app._claim_connection_outage_alert() is None


def test_pause_suppresses_alert_without_consuming_it(watchdog, monkeypatch):
    app = watchdog.app
    app.is_paused = True
    run_ticks(watchdog, monkeypatch, ticks=2)
    watchdog.send.assert_not_called()
    assert app.connection_alert_sent is False
    app.is_paused = False
    run_ticks(watchdog, monkeypatch)
    watchdog.send.assert_called_once()


@pytest.mark.parametrize('recovery', ['success', 'resume'])
def test_recovery_restarts_the_full_threshold_and_rearms(watchdog, recovery):
    app = watchdog.app
    assert app._claim_connection_outage_alert() is not None
    watchdog.clock[0] = 1200.0
    if recovery == 'success':
        app.record_ebay_success()
    else:
        app.reset_connection_watch_after_manual_resume()
    assert app.connection_alert_sent is False
    assert app._claim_connection_outage_alert() is None
    watchdog.clock[0] = 1799.999
    assert app._claim_connection_outage_alert() is None
    watchdog.clock[0] = 1800.0
    assert app._claim_connection_outage_alert() is not None


@pytest.mark.parametrize('failure', [False, RuntimeError('offline send failure')])
def test_failed_send_retries_then_latches_only_after_success(watchdog, monkeypatch, failure):
    watchdog.send.side_effect = [failure, True]
    run_ticks(watchdog, monkeypatch, ticks=3)
    assert watchdog.send.call_count == 2
    assert watchdog.app.connection_alert_sent is True


@pytest.mark.parametrize('recovery', ['success', 'resume'])
def test_recovery_during_successful_send_does_not_relatch(watchdog, monkeypatch, recovery):
    app = watchdog.app

    def send(message, **kwargs):
        watchdog.clock[0] = 1001.0
        if recovery == 'success':
            app.record_ebay_success()
        else:
            app.reset_connection_watch_after_manual_resume()
        return True

    watchdog.send.side_effect = send
    run_ticks(watchdog, monkeypatch)
    assert app.connection_alert_sent is False
    assert app._claim_connection_outage_alert() is None
    watchdog.clock[0] = 1601.0
    assert app._claim_connection_outage_alert() is not None


@pytest.mark.parametrize('recovery', ['success', 'resume'])
@pytest.mark.parametrize('failure', [False, RuntimeError('old offline send failed')])
def test_old_send_failure_cannot_clear_a_newer_outage_claim(watchdog, monkeypatch, recovery, failure):
    app = watchdog.app

    def send(message, **kwargs):
        watchdog.clock[0] = 1001.0
        if recovery == 'success':
            app.record_ebay_success()
        else:
            app.reset_connection_watch_after_manual_resume()
        # Simulate a newer outage being claimed while the old network call is pending.
        watchdog.clock[0] = 1601.0
        assert app._claim_connection_outage_alert() is not None
        if isinstance(failure, Exception):
            raise failure
        return failure

    watchdog.send.side_effect = send
    run_ticks(watchdog, monkeypatch)
    watchdog.send.assert_called_once()
    assert app.connection_alert_sent is True
    assert app._claim_connection_outage_alert() is None


def test_claim_observes_recovery_that_precedes_lock_acquisition(watchdog, monkeypatch):
    app = watchdog.app
    actual_lock = threading.Lock()

    class RecoverBeforeAcquire:
        recover = True

        def __enter__(self):
            if self.recover:
                self.recover = False
                watchdog.clock[0] = 1001.0
                app.record_ebay_success()
            actual_lock.acquire()
            return self

        def __exit__(self, *exc):
            actual_lock.release()

    monkeypatch.setattr(app, 'connection_state_lock', RecoverBeforeAcquire())
    run_ticks(watchdog, monkeypatch)
    watchdog.send.assert_not_called()
    assert app.connection_alert_sent is False

