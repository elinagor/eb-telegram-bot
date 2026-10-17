"""Offline delivery/selection checks; no production proxy or report requests."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

import coordinator_feed as feed_module
from test_coordinator_uk import (
    Response, feed, scanner, payload, forbid_unmocked_network,
)


def public_rows(count):
    return [dict(id=f'{i + 1:064x}', proxy=f'http://8.8.{i // 250}.{i % 250 + 1}:8080',
                 managed=False, health_state='healthy', strong=i % 2 == 0)
            for i in range(count)]


@pytest.mark.parametrize('count', [20, 250, 500])
def test_all_delivered_candidates_reach_the_selection_pool(feed, scanner, monkeypatch, count):
    client, clock, transport = feed
    rows = public_rows(count)
    transport.get.return_value = Response(payload(clock[0], rows))
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager()
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *a, **kw: None)
    assert manager.refresh_coordinator() == count
    expected = {r['proxy'] for r in rows}
    assert manager.coordinator_current == expected
    assert set(manager.proxies) == expected
    assert set(manager.all_proxies) == expected

    selected, tried = [], set()
    for _ in range((count + 9) // 10):
        batch = manager.get_candidate_batch(10, tried_hosts=tried, allow_premium=False)
        selected.extend(batch)
        tried.update(scanner._proxy_host(p) for p in batch)
    assert len(selected) == count
    assert set(selected) == expected
    assert manager.get_candidate_batch(10, tried_hosts=tried, allow_premium=False) == []
    transport.get.assert_called_once()
    assert transport.get.call_args.kwargs['params'] == {'market': 'uk', 'limit': 500}


def test_duplicate_ports_are_received_but_do_not_duplicate_exit_ip_probes(feed, scanner, monkeypatch):
    client, clock, transport = feed
    rows = public_rows(17)
    rows.extend(dict(rows[i], id=f'{100 + i:064x}',
                     proxy=rows[i]['proxy'].replace(':8080', ':3128')) for i in range(3))
    transport.get.return_value = Response(payload(clock[0], rows))
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager()
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *a, **kw: None)
    assert manager.refresh_coordinator() == 20
    assert len(manager.coordinator_current) == 20
    selected = manager.get_candidate_batch(30, allow_premium=False)
    assert len(selected) == 17
    assert len({scanner._proxy_host(p) for p in selected}) == 17


def test_refresh_publishes_new_candidates_before_any_feedback_wait(feed, scanner, monkeypatch):
    client, clock, transport = feed
    client.refresh()
    client.record_result('http://8.8.8.8:8080', 'success')
    transport.post.side_effect = AssertionError('feedback must not run during feed publication')
    clock[0] += 61
    replacement = public_rows(20)
    transport.get.return_value = Response(payload(clock[0], replacement))
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager()
    assert manager.refresh_coordinator() == 20
    assert manager.coordinator_current == {r['proxy'] for r in replacement}
    transport.post.assert_not_called()
    assert len(client.pending) == 1  # Explicit worker flush retains the original report.


@pytest.mark.parametrize('kind', ['feed', 'feedback'])
def test_slow_background_acknowledgement_within_budget_is_accepted(feed, monkeypatch, kind):
    client, clock, transport = feed
    client.refresh()
    client.record_result('http://8.8.8.8:8080', 'success')
    clock[0] += 61
    elapsed = [100.0]
    monkeypatch.setattr(feed_module, 'time', SimpleNamespace(monotonic=lambda: elapsed[0]))

    def delayed_response(*args, **kwargs):
        delay = 4.2  # Previously exceeded both the feed and report read timeouts.
        if kwargs['timeout'][1] < delay:
            raise requests.exceptions.ReadTimeout('synthetic slow acknowledgement')
        elapsed[0] += delay
        if kind == 'feed':
            return Response(payload(clock[0], public_rows(20)))
        return Response(dict(ok=True, accepted=1))

    if kind == 'feed':
        transport.get.side_effect = delayed_response
        assert len(client.refresh()[0]) == 20
        assert client.failures == 0
    else:
        transport.post.side_effect = delayed_response
        client.flush_feedback()
        assert client.accepted_feedback == 1


def test_feedback_timeout_preserves_feed_and_only_later_results_are_sent(feed):
    client, clock, transport = feed
    before = client.refresh()
    uri = 'http://8.8.8.8:8080'
    client.record_result(uri, 'blocked')
    transport.post.side_effect = requests.exceptions.ReadTimeout('synthetic lost response')
    client.flush_feedback()
    assert client.snapshot() == before
    assert not client.pending
    client.flush_feedback()
    assert transport.post.call_count == 1
    clock[0] += 31
    transport.post.side_effect = None
    transport.post.return_value = Response(dict(ok=True, accepted=1))
    client.record_result(uri, 'success')
    client.flush_feedback()
    assert client.accepted_feedback == 1
    assert [r['result'] for r in transport.post.call_args.kwargs['json']['reports']] == ['success']


@pytest.mark.parametrize('kind', ['feed', 'feedback'])
def test_drip_response_still_obeys_total_read_deadline(feed, monkeypatch, kind):
    client, clock, transport = feed
    before = client.refresh()
    client.record_result('http://8.8.8.8:8080', 'success')
    clock[0] += 61
    elapsed = [100.0]
    monkeypatch.setattr(feed_module, 'time', SimpleNamespace(monotonic=lambda: elapsed[0]))
    response = Mock(status_code=200)

    def drip(**kwargs):
        for _ in range(20):
            elapsed[0] += 1
            yield b' '

    response.iter_content.side_effect = drip
    if kind == 'feed':
        transport.get.return_value = response
        assert client.refresh() == before
        assert client.failures == 1
    else:
        transport.post.return_value = response
        client.flush_feedback()
        assert client.accepted_feedback == 0
        assert not client.pending
    assert elapsed[0] <= 108.0
    response.close.assert_called_once()


def test_unavailable_feed_retains_its_recent_cache_then_expires(feed, scanner, monkeypatch):
    client, clock, transport = feed
    transport.get.return_value = Response(payload(clock[0], public_rows(20)))
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager()
    assert manager.refresh_coordinator() == 20
    clock[0] += 61
    transport.get.side_effect = requests.exceptions.ReadTimeout('synthetic feed timeout')
    assert manager.refresh_coordinator() == 20
    clock[0] += 120
    assert manager.refresh_coordinator() == 0
    assert not manager.coordinator_current
