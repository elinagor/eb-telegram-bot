"""Offline regression tests: never connect to eBay, Telegram or production DB."""

import json

import threading

import time

from datetime import datetime, timezone

from unittest.mock import Mock

import pytest

from concurrent.futures import Future, ThreadPoolExecutor

from proxy_runtime import PipelineMetrics, ProbeLimiter, SpareSession, adaptive_timeout

from coordinator_feed import CoordinatorFeed

@pytest.fixture(autouse=True)
def forbid_unmocked_network(monkeypatch):
    import requests
    import psycopg2
    from curl_cffi import requests as curl_requests
    def forbidden(*args, **kwargs):
        raise AssertionError('Offline test attempted an unmocked external operation')
    monkeypatch.setattr(requests, 'get', forbidden)
    monkeypatch.setattr(requests, 'post', forbidden)
    monkeypatch.setattr(requests.Session, 'request', forbidden)
    monkeypatch.setattr(curl_requests.Session, 'request', forbidden)
    monkeypatch.setattr(psycopg2, 'connect', forbidden)

def test_reserve_warming_prefers_fresh_coordinator_over_tcp_only(scanner):
    manager = scanner.ProxyManager()
    primary = 'http://8.8.8.8:8080'
    tcp_only = 'http://1.1.1.1:80'
    manager.proxies = [tcp_only, primary]
    manager.coordinator_current = frozenset([primary])
    manager.coordinator_valid_until = time.time() + 120
    manager.preflight_ok_until[tcp_only] = time.time() + 60
    assert manager.get_quality_preflight_candidates(1) == [primary]
    assert primary not in manager.quality_ok_until
    assert primary not in manager.last_success_at

def test_reserve_warming_keeps_real_success_first(scanner):
    manager = scanner.ProxyManager()
    primary = 'http://8.8.8.8:8080'
    proven = 'http://1.1.1.1:80'
    manager.proxies = [primary, proven]
    manager.coordinator_current = frozenset([primary])
    manager.coordinator_valid_until = time.time() + 120
    manager.last_success_at[proven] = time.time()
    assert manager.get_quality_preflight_candidates(1) == [proven]

def test_reserve_warming_expired_feed_keeps_legacy_tcp_priority(scanner):
    manager = scanner.ProxyManager()
    primary = 'http://8.8.8.8:8080'
    tcp_only = 'http://1.1.1.1:80'
    manager.proxies = [primary, tcp_only]
    manager.coordinator_current = frozenset([primary])
    manager.coordinator_valid_until = time.time() - 1
    manager.preflight_ok_until[tcp_only] = time.time() + 60
    assert manager.get_quality_preflight_candidates(1) == [tcp_only]

def test_reserve_warming_respects_quarantine_and_host_diversity(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    blocked = 'http://8.8.8.8:8080'
    tls_bad = 'http://8.8.4.4:8080'
    managed = 'http://1.1.1.1:8080'
    twins = ['http://9.9.9.9:80', 'socks5://9.9.9.9:1080']
    legacy = 'http://4.2.2.2:8080'
    manager.proxies = [blocked, tls_bad, managed] + twins + [legacy]
    manager.coordinator_current = frozenset(manager.proxies)
    manager.coordinator_valid_until = time.time() + 120
    manager.host_bad_until['8.8.8.8'] = time.time() + 300
    manager.quality_bad_until[tls_bad] = time.time() + 45
    monkeypatch.setattr(scanner.provider_manager, 'source_fast', lambda p: 'webshare' if p == managed else 'free')
    selected = manager.get_quality_preflight_candidates(12)
    assert len(selected) == 2 and legacy in selected
    assert sum(p in twins for p in selected) == 1
    assert not any(p in selected for p in (blocked, tls_bad, managed))

class Response:
    def __init__(self, data=None, status=200, raw=None):
        self.status_code = status
        self.body = raw if raw is not None else json.dumps(data).encode()
        self.closed = False

    def iter_content(self, chunk_size=8192):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        self.closed = True

def row(ip='8.8.8.8', key='a', **extra):
    return dict(id=key * 64, proxy='http://%s:8080' % ip,
                managed=False, health_state='healthy', strong=True, **extra)

def payload(now, rows=None, market='uk'):
    return dict(market=market, generated_at=datetime.fromtimestamp(
        now, timezone.utc).isoformat(), proxies=[row()] if rows is None else rows)

@pytest.fixture
def feed():
    clock = [time.time()]
    transport = Mock()
    transport.get.return_value = Response(payload(clock[0]))
    transport.post.return_value = Response(dict(ok=True, accepted=1))
    client = CoordinatorFeed('https://feed.example', 'test-only',
                             clock=lambda: clock[0], transport=transport)
    return client, clock, transport

def test_snapshot_retry_and_expiry_fail_open(feed, caplog):
    client, clock, transport = feed
    assert client.refresh()[0] == ('http://8.8.8.8:8080',)
    assert transport.get.call_args.kwargs['allow_redirects'] is False
    assert transport.get.return_value.closed
    client.refresh()
    assert transport.get.call_count == 1
    clock[0] += 61
    transport.get.return_value = Response(status=502)
    assert client.refresh()[0]
    client.refresh()
    assert transport.get.call_count == 2
    clock[0] += 120
    assert client.refresh()[0] == ()
    assert 'test-only' not in caplog.text
    assert '502' in caplog.text

def test_background_refresh_never_blocks_discovery(feed):
    client, clock, transport = feed
    client.refresh()
    clock[0] += 61
    entered, release = threading.Event(), threading.Event()
    def slow(*args, **kwargs):
        entered.set()
        release.wait(2)
        return Response(payload(clock[0]))
    transport.get.side_effect = slow
    worker = threading.Thread(target=client.refresh)
    worker.start()
    try:
        assert entered.wait(1)
        start = time.monotonic()
        assert client.refresh()[0]
        assert time.monotonic() - start < 0.2
    finally:
        release.set()
        worker.join(2)

@pytest.mark.parametrize('change', ['market', 'stale', 'future', 'managed', 'degraded',
                                    'auth', 'private', 'invalid_id', 'oversize'])
def test_bad_feed_cannot_replace_valid_snapshot(feed, change):
    client, clock, transport = feed
    before = client.refresh()
    clock[0] += 61
    data = payload(clock[0])
    if change == 'market':
        data['market'] = 'us'
    elif change == 'stale':
        data = payload(clock[0] - 181)
    elif change == 'future':
        data = payload(clock[0] + 61)
    elif change == 'managed':
        data['proxies'][0]['managed'] = True
    elif change == 'degraded':
        data['proxies'][0]['health_state'] = 'degraded'
    elif change == 'auth':
        data['proxies'][0]['proxy'] = 'http://user:secret@8.8.8.8:8080'
    elif change == 'private':
        data['proxies'][0]['proxy'] = 'http://127.0.0.1:8080'
    elif change == 'invalid_id':
        data['proxies'][0]['id'] = 'bad'
    transport.get.return_value = Response(data, raw=b'x' * (1024 * 1024 + 1)
                                          if change == 'oversize' else None)
    assert client.refresh() == before
    assert transport.get.return_value.closed

def test_empty_valid_snapshot_clears_priority(feed):
    client, clock, transport = feed
    client.refresh()
    clock[0] += 61
    transport.get.return_value = Response(payload(clock[0], []))
    assert client.refresh()[0] == ()

@pytest.mark.parametrize('origin', ['', 'http://feed.example', 'https://x/path',
                                   'https://user:pass@x', 'https://[invalid'])
def test_bad_config_is_optional(origin):
    transport = Mock()
    client = CoordinatorFeed(origin, 'test-only', transport=transport)
    assert not client.enabled
    assert client.refresh()[0] == ()
    transport.get.assert_not_called()

def test_feedback_queues_real_results_without_network_and_flushes(feed):
    client, clock, transport = feed
    client.refresh()
    client.record_result('http://8.8.8.8:8080', 'success')
    client.record_result('http://8.8.8.8:8080', 'blocked')
    client.record_result('http://1.1.1.1:80', 'success')
    transport.post.assert_not_called()
    assert len(client.pending) == 1
    client.flush_feedback()
    sent = transport.post.call_args.kwargs['json']
    assert sent == dict(market='uk', reports=[dict(proxy_key='a' * 64, result='success'),
                                            dict(proxy_key='a' * 64, result='blocked')])
    assert client.accepted_feedback == 1  # Count the server's acknowledgement, not guesses.
    assert transport.post.return_value.closed

def test_feedback_retains_success_and_only_latest_negative(feed):
    client, clock, transport = feed
    client.refresh()
    for result in ['success', 'success', 'blocked', 'proxy_timeout', 'proxy_ssl']:
        client.record_result('http://8.8.8.8:8080', result)
    client.flush_feedback()
    assert [r['result'] for r in transport.post.call_args.kwargs['json']['reports']] == ['success', 'ssl']

def test_feedback_later_real_success_supersedes_old_failure(feed):
    client, clock, transport = feed
    client.refresh()
    for result in ['success', 'blocked', 'success']:
        client.record_result('http://8.8.8.8:8080', result)
    client.flush_feedback()
    assert [r['result'] for r in transport.post.call_args.kwargs['json']['reports']] == ['success']

def test_feedback_pairs_stay_ordered_bounded_and_never_split(feed):
    client, clock, transport = feed
    for i in range(200):
        uri = 'http://8.8.%d.%d:80' % (i // 255, i % 255)
        client.ids[uri] = '%064x' % i
        client.record_result(uri, 'success')
        client.record_result(uri, 'blocked')
    assert len(client.pending) == 128
    assert sum(len(events) for events in client.pending.values()) == 256
    client.flush_feedback()
    reports = transport.post.call_args.kwargs['json']['reports']
    assert len(reports) == 64 and len(client.pending) == 96
    for before, after in zip(reports[::2], reports[1::2]):
        assert before['proxy_key'] == after['proxy_key']
        assert before['result'] == 'success' and after['result'] == 'blocked'

def test_feedback_bounded_and_not_replayed_after_ambiguous_failure(feed):
    client, clock, transport = feed
    for i in range(200):
        uri = 'http://8.8.%d.%d:80' % (i // 255, i % 255)
        client.ids[uri] = '%064x' % i
        client.record_result(uri, 'proxy_timeout')
    assert len(client.pending) == 128
    transport.post.side_effect = TimeoutError('contains test-only secret')
    client.flush_feedback()
    assert len(client.pending) == 64
    client.flush_feedback()
    assert transport.post.call_count == 1

@pytest.fixture
def scanner(monkeypatch):
    # No prod env file is present. Import starts neither workers nor DB requests.
    monkeypatch.setenv('COORDINATOR_BASE_URL', '')
    monkeypatch.setenv('COORDINATOR_UK_TOKEN', '')
    monkeypatch.setenv('EBAY_SEARCH_URL', 'https://www.ebay.co.uk/sch/i.html')
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'test-only')
    monkeypatch.setenv('TELEGRAM_CHAT_ID', 'test-only')
    monkeypatch.setenv('DATABASE_URL', 'postgresql://test:test@localhost/test')
    monkeypatch.setenv('PROXY_LIST', 'https://legacy.example')
    import app
    monkeypatch.setattr(app.provider_manager, 'candidates', lambda **kwargs: [])
    monkeypatch.setattr(app.provider_manager, 'source_fast', lambda p: 'free')
    return app

def test_coordinator_merge_preserves_legacy_and_local_ebay_cooldown(scanner, feed, monkeypatch):
    client, clock, transport = feed
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager('https://legacy.example')
    legacy = 'http://1.1.1.1:80'
    primary = 'http://8.8.8.8:8080'
    manager.proxies = manager.all_proxies = [legacy]
    manager.host_bad_until['8.8.8.8'] = clock[0] + 300
    manager.refresh_coordinator()
    assert primary in manager.all_proxies
    assert manager.proxies == [legacy]
    assert manager.host_bad_until['8.8.8.8'] > clock[0]
    assert primary not in manager.quality_ok_until
    assert primary not in manager.last_success_at

def test_primary_candidates_keep_known_good_and_parallel_legacy_slots(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    legacy = ['http://1.1.1.%d:80' % i for i in range(1, 5)]
    manager.proxies = manager.all_proxies = primary + legacy
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    manager.last_refresh = time.time()
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *args, **kwargs: False)
    # Simulate legacy endpoints with an already successful neutral preflight.
    manager.quality_ok_until = {p: time.time() + 100 for p in legacy}
    batch = manager.get_candidate_batch(4, allow_premium=False)
    assert len(batch) == 4 and len(set(batch)) == 4
    assert all(p in primary for p in batch[:2])
    assert any(p in legacy for p in batch[2:])
    manager.last_success_at[legacy[0]] = time.time()
    assert manager.get_candidate_batch(4, allow_premium=False)[0] == legacy[0]
    manager.host_bad_until['8.8.8.1'] = time.time() + 100
    assert primary[0] not in manager.get_candidate_batch(4, allow_premium=False)
    manager.coordinator_valid_until = 0
    assert all(p in legacy for p in manager.get_candidate_batch(4, allow_premium=False))

def test_immediate_reserve_uses_primary_within_existing_budget(scanner):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    legacy = ['http://1.1.1.%d:80' % i for i in range(1, 8)]
    manager.proxies = primary + legacy
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    # Remote neutral health alone is not a locally ready TLS reserve.
    manager.quality_ok_until = {p: time.time() + 100 for p in primary + legacy}
    reserve = manager.get_warm_standby_candidates(8)
    assert all(p in primary for p in reserve[:2])
    assert len(reserve) <= scanner.WARM_STANDBY_FREE_QUALITY_LIMIT
    manager.host_bad_until['8.8.8.1'] = time.time() + 100
    assert primary[0] not in manager.get_warm_standby_candidates(8)

def test_legacy_parser_retains_http_and_socks_rejects_malformed(scanner, monkeypatch):
    response = Response(raw=(b'8.8.8.8:80\nsocks5://1.1.1.1:1080\n'
                             b'https://9.9.9.9:443\nhttp://127.0.0.1:80\n'
                             b'http://user:secret@8.8.8.8:80\n<html>\n'
                             b'8.8.8.8:80\nhttp://8.8.4.4:99999\n'))
    monkeypatch.setattr(scanner.requests, 'get', Mock(return_value=response))
    manager = scanner.ProxyManager('https://legacy.example')
    assert set(manager.fetch_proxies_from_api()) == {
        'http://8.8.8.8:80', 'socks5://1.1.1.1:1080', 'https://9.9.9.9:443'}
    assert response.closed

def test_managed_backup_slots_and_webshare_guard_remain(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    premium = ['http://9.9.9.%d:80' % i for i in range(1, 5)]
    webshare = ['http://1.1.1.%d:80' % i for i in range(1, 5)]
    def source(p):
        return ('proxyscrape_premium' if p in premium else
                'webshare' if p in webshare else 'free')
    monkeypatch.setattr(scanner.provider_manager, 'source_fast', source)
    monkeypatch.setattr(scanner.provider_manager, 'candidates',
                        lambda include_premium=True, include_webshare=False:
                        (premium if include_premium else []) +
                        (webshare if include_webshare else []))
    monkeypatch.setattr(scanner.provider_manager, 'usable_webshare_account_count', lambda: 4)
    monkeypatch.setattr(scanner.provider_manager, 'webshare_account_index_fast',
                        lambda p: webshare.index(p) if p in webshare else None)
    monkeypatch.setattr(scanner.provider_manager, 'managed_proxy_available', lambda p: True)
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *args, **kwargs: False)
    manager.proxies = primary
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    batch = manager.get_candidate_batch(4, allow_webshare=True)
    assert all(p in primary for p in batch[:2])
    assert sum(p in webshare for p in batch) <= 1
    assert any(p in premium for p in batch)
    locked = manager.get_candidate_batch(4, allow_webshare=False)
    assert not any(p in webshare for p in locked)

def test_feed_refill_does_not_discard_recent_working_proxy(scanner, feed, monkeypatch):
    client, clock, transport = feed
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager('https://legacy.example')
    proven = 'http://9.9.9.9:80'
    manager.proxies = manager.all_proxies = [proven]
    manager.last_success_at[proven] = time.time()
    manager.refresh_coordinator()
    monkeypatch.setattr(manager, 'fetch_proxies_from_api', lambda **kwargs: ['http://1.1.1.1:80'])
    monkeypatch.setattr(threading.current_thread(), 'name', 'proxy-metadata-worker')
    manager.refresh_proxies(force=True)
    assert proven in manager.proxies
    assert 'http://1.1.1.1:80' in manager.proxies
    assert 'http://8.8.8.8:8080' in manager.proxies

def test_remote_metadata_is_finite_copied_and_expires(feed):
    client, clock, transport = feed
    transport.get.return_value = Response(payload(clock[0], [row(
        score=17, neutral_latency_ms=450, market_quality=.82, market_failure_streak=1)]))
    client.refresh()
    uri = client.rows[0]
    meta = client.metadata_snapshot()
    assert meta[uri]['market_quality'] == .82 and meta[uri]['latency_ms'] == 450
    meta[uri]['score'] = 999
    assert client.metadata_snapshot()[uri]['score'] == 17
    assert client.was_seen(uri)
    clock[0] += 181
    assert client.metadata_snapshot() == {} and client.was_seen(uri)

def test_invalid_remote_numbers_never_poison_ranking(feed):
    client, clock, transport = feed
    transport.get.return_value = Response(payload(clock[0], [row(
        score=float('nan'), neutral_latency_ms=float('inf'), market_quality='nonsense',
        market_failure_streak=-100)]))
    client.refresh()
    meta = client.metadata_snapshot()[client.rows[0]]
    assert meta['score'] == 0 and meta['latency_ms'] is None and meta['market_quality'] is None
    assert meta['failure_streak'] == 0

def test_feedback_uses_measured_latency_and_rejects_nan(feed):
    client, clock, transport = feed
    client.refresh()
    uri = client.rows[0]
    client.record_result(uri, 'success', latency_ms=1234.56)
    client.record_result(uri, 'blocked', latency_ms=float('nan'))
    clock[0] += 31
    client.flush_feedback()
    reports = transport.post.call_args.kwargs['json']['reports']
    assert reports[0]['latency_ms'] == 1234.6
    assert 'latency_ms' not in reports[1] and reports[1]['result'] == 'blocked'

def test_unchecked_coordinator_does_not_consume_ready_reserve(scanner):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:80' % i for i in range(1, 4)]
    ready = ['http://1.1.1.%d:80' % i for i in range(1, 9)]
    manager.proxies = primary + ready
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    manager.quality_ok_until = {p: time.time() + 100 for p in ready}
    selected = manager.get_warm_standby_candidates(8)
    assert len(selected) == scanner.WARM_STANDBY_FREE_QUALITY_LIMIT
    assert all(p in ready and manager.quality_state(p) == 'ok' for p in selected)

def test_warm_reserve_real_socks_success_stays_first(scanner):
    manager = scanner.ProxyManager()
    proven = 'socks5://9.9.9.9:1080'
    primary = ['http://8.8.8.%d:80' % i for i in range(1, 5)]
    legacy = 'http://1.1.1.1:80'
    manager.proxies = primary + [legacy, proven]
    manager.last_success_at[proven] = time.time()
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    manager.quality_ok_until = {p: time.time() + 100 for p in primary + [legacy]}
    selected = manager.get_warm_standby_candidates(8)
    assert selected[0] == proven
    assert legacy in selected and len(set(scanner._proxy_host(p) for p in selected)) == len(selected)

def test_metadata_ranking_does_not_override_ebay_success(scanner):
    manager = scanner.ProxyManager()
    slow, fast, proven = 'http://8.8.8.1:80', 'http://8.8.8.2:80', 'socks5://9.9.9.9:1080'
    manager.proxies = [slow, fast, proven]
    manager.coordinator_current = frozenset([slow, fast])
    manager.coordinator_valid_until = time.time() + 180
    manager.coordinator_metadata = {slow: dict(rank=20, latency_ms=2500, failure_streak=2, market_quality=.2),
                                    fast: dict(rank=0, latency_ms=200, failure_streak=0, market_quality=.9)}
    assert manager.get_candidate_batch(1, allow_premium=False) == [fast]
    manager.last_success_at[proven] = time.time()
    assert manager.get_candidate_batch(1, allow_premium=False) == [proven]
    manager.host_bad_until['8.8.8.2'] = time.time() + 300
    assert fast not in manager.get_candidate_batch(4, allow_premium=False)

def test_proactive_rechecks_do_not_starve_new_candidates(scanner):
    manager = scanner.ProxyManager()
    due = ['http://8.8.8.%d:80' % i for i in range(1, 10)]
    fresh = ['http://1.1.1.%d:80' % i for i in range(1, 10)]
    not_due = 'http://9.9.9.9:80'
    manager.proxies = due + fresh + [not_due]
    manager.quality_ok_until = {p: time.time() + 20 for p in due}
    manager.quality_ok_until[not_due] = time.time() + 110
    selected = manager.get_quality_preflight_candidates(8)
    assert sum(p in due for p in selected) == 4
    assert sum(p in fresh for p in selected) == 4
    assert not_due not in selected

def test_limiter_covers_other_executor_and_same_ip_until_completion():
    limiter = ProbeLimiter(2)
    gate = threading.Event()
    def block():
        gate.wait(2)
    with ThreadPoolExecutor(2) as first, ThreadPoolExecutor(2) as second:
        a = limiter.submit(first, block, identity='first-ip')
        assert limiter.submit(second, block, identity='first-ip') is None
        b = limiter.submit(second, block, identity='second-ip')
        assert limiter.count() == 2
        assert limiter.submit(second, block, identity='third-ip') is None
        gate.set()
        a.result(2)
        b.result(2)
    assert limiter.count() == 0

def test_limiter_releases_failed_submit_and_cancelled_queue():
    limiter = ProbeLimiter(1)
    executor = Mock()
    executor.submit.side_effect = RuntimeError('shutdown')
    with pytest.raises(RuntimeError):
        limiter.submit(executor, lambda: None, identity='ip')
    assert limiter.count() == 0
    executor.submit.side_effect = None
    queued = Future()
    executor.submit.return_value = queued
    limiter.submit(executor, lambda: None, identity='ip')
    assert queued.cancel() and limiter.count() == 0
    assert 'ip' not in limiter.identities

def test_metrics_require_evidence_bound_memory_and_measure_outage_separately():
    now = [1000]
    metrics = PipelineMetrics(clock=lambda: now[0])
    for _ in range(3):
        metrics.record_request('p', 'coordinator', 'success', 1.5, 'fixed')
    assert metrics.successful_latency('p') == 1.5
    assert metrics.coordinator_fraction() == .75
    for _ in range(12):
        metrics.record_request('a', 'coordinator', 'blocked', 4, 'discovery')
        metrics.record_request('b', 'legacy', 'success', 1, 'discovery')
    assert metrics.coordinator_fraction() == .5
    metrics.success()
    now[0] += 15
    metrics.success(outage=3)
    snap = metrics.snapshot_if_due()
    assert snap['gap_p95'] == 15 and snap['outage_p95'] == 3
    for i in range(1500):
        metrics.record_request(str(i), 'legacy', 'success', 1, 'discovery')
    assert len(metrics.latencies) == 1000 and len(metrics.requests) == 512
    now[0] += 1000
    assert metrics.coordinator_fraction() == .75
