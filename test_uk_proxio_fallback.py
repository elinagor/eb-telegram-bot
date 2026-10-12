"""Offline provider regressions: quota-aware API and separately cached public JSON."""
import json
import threading
import time
from unittest.mock import Mock

import pytest

from test_coordinator_uk import scanner, forbid_unmocked_network


def proxy_row(ip='8.8.8.8', **extra):
    row = dict(ip=ip, port=8080, protocols=['HTTPS'], anonymity='Elite',
               reliability=95, latency_s=.7, uptime=.95)
    row.update(extra)
    return row


class ProviderResponse:
    def __init__(self, rows=None, status=200, raw=None, **usage):
        self.status_code = status
        self.headers = {'Retry-After': '60'}
        data = dict(proxies=[proxy_row()] if rows is None else rows, **usage)
        self.body = json.dumps(data).encode() if raw is None else raw
        self.closed = False

    def json(self):
        return json.loads(self.body)

    def iter_content(self, chunk_size=8192):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        self.closed = True


@pytest.fixture
def proxio(scanner, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(scanner.time, 'time', lambda: clock[0])
    monkeypatch.setattr(scanner, 'PROXIO_API_KEYS', ('test-only-key',))
    monkeypatch.setattr(scanner, 'PROXIO_FREE_ENABLED', True)
    monkeypatch.setattr(scanner, 'PROXIO_DOWNLOAD_ENABLED', True)
    monkeypatch.setattr(scanner, 'PROXIO_FREE_REFRESH', 300)
    monkeypatch.setattr(scanner, 'PROXIO_DOWNLOAD_REFRESH', 300)
    monkeypatch.setattr(scanner, 'PROXIO_API_RETRY_KEYS', 2)
    monkeypatch.setattr(scanner, 'HPROXY_FREE_ENABLED', False)
    monkeypatch.setattr(scanner, 'PROXMINT_FREE_ENABLED', False)
    return scanner.ExternalFreeSourceManager(), clock


def test_working_api_stays_primary_and_uses_reported_account_totals(scanner, proxio, monkeypatch):
    manager, clock = proxio
    get = Mock(return_value=ProviderResponse(calls_today=23, daily_limit=100))
    monkeypatch.setattr(scanner.requests, 'get', get)
    result = manager._fetch_proxio()
    assert result['rows'] == ['http://8.8.8.8:8080']
    assert manager.proxio_key_state[0]['calls'] == 23
    assert manager.proxio_key_state[0]['daily_limit'] == 100
    assert manager.proxio_download_active is False
    assert get.call_count == 1 and get.call_args.args[0].endswith('/api/list')
    assert not manager._source_due('proxio', clock[0] + 299, force=True)
    assert manager._source_due('proxio', clock[0] + 300)


def test_daily_429_parks_api_until_utc_but_public_refresh_continues(scanner, proxio, monkeypatch):
    manager, clock = proxio
    first_download = ProviderResponse()
    next_download = ProviderResponse(rows=[proxy_row('1.1.1.1')])
    get = Mock(side_effect=[ProviderResponse(status=429), first_download, next_download])
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows'] == ['http://8.8.8.8:8080']
    assert manager.proxio_key_state[0]['disabled_until'] >= clock[0] + manager._seconds_until_next_utc_day()
    assert manager.proxio_next_retry_at > clock[0]
    assert not manager._source_due('proxio', clock[0] + 299)
    clock[0] += 300
    assert manager._source_due('proxio', clock[0])
    assert manager._fetch_proxio()['rows'] == ['http://1.1.1.1:8080']
    assert [call.args[0] for call in get.call_args_list] == [
        'https://proxio.io/api/list', 'https://proxio.io/download', 'https://proxio.io/download']
    for call in get.call_args_list[1:]:
        assert 'headers' not in call.kwargs
        assert call.kwargs['params']['format'] == 'json'
        assert call.kwargs['params']['limit'] <= 1000
        assert call.kwargs['allow_redirects'] is False
    assert first_download.closed and next_download.closed


def test_key_rejections_try_at_most_two_keys_then_download(scanner, proxio, monkeypatch):
    monkeypatch.setattr(scanner, 'PROXIO_API_KEYS', ('test-key-a', 'test-key-b', 'test-key-c'))
    manager = scanner.ExternalFreeSourceManager()
    get = Mock(side_effect=[ProviderResponse(status=401), ProviderResponse(status=429), ProviderResponse()])
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows']
    assert get.call_count == 3
    assert sum(call.args[0].endswith('/api/list') for call in get.call_args_list) == 2
    assert manager.proxio_key_state[2]['calls'] == 0


def test_shared_provider_error_does_not_fan_out_keys_or_log_secret(scanner, proxio, monkeypatch, caplog):
    monkeypatch.setattr(scanner, 'PROXIO_API_KEYS', ('test-secret-sentinel', 'another-key'))
    manager = scanner.ExternalFreeSourceManager()
    get = Mock(side_effect=[TimeoutError('contains test-secret-sentinel'), ProviderResponse()])
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows']
    assert get.call_count == 2 and manager.proxio_key_state[1]['calls'] == 0
    assert 'test-secret-sentinel' not in caplog.text


@pytest.mark.parametrize('used, limit', [(80, 100), (40, 50)])
def test_provider_quota_counts_include_other_clients_and_stop_before_headroom(scanner, proxio, monkeypatch, used, limit):
    manager, _clock = proxio
    get = Mock(return_value=ProviderResponse(calls_today=used, daily_limit=limit))
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows']
    assert manager.proxio_key_state[0]['calls'] == used
    assert manager._proxio_next_key() is None
    assert get.call_count == 1


def test_reported_usage_never_rolls_local_count_back(scanner, proxio):
    manager, _clock = proxio
    manager.proxio_key_state[0]['calls'] = 60
    manager._proxio_update_usage(0, dict(calls_today=2, daily_limit=1000))
    assert manager.proxio_key_state[0]['calls'] == 60
    assert manager._proxio_daily_budget(manager.proxio_key_state[0]) == 80
    manager._proxio_update_usage(0, dict(calls_today='nan', daily_limit='inf'))
    assert manager.proxio_key_state[0]['calls'] == 60


def test_keyless_operation_never_calls_authenticated_api(scanner, proxio, monkeypatch):
    monkeypatch.setattr(scanner, 'PROXIO_API_KEYS', ())
    manager = scanner.ExternalFreeSourceManager()
    get = Mock(return_value=ProviderResponse())
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._source_enabled('proxio')
    assert manager._fetch_proxio()['rows']
    assert manager.proxio_key_state == []
    assert get.call_count == 1 and get.call_args.args[0].endswith('/download')


def test_api_recovery_takes_priority_without_duplicate_download(scanner, proxio, monkeypatch):
    manager, clock = proxio
    get = Mock(side_effect=[TimeoutError(), ProviderResponse(), ProviderResponse(calls_today=3, daily_limit=100)])
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows']
    assert manager.proxio_download_active
    clock[0] += 300
    assert manager._fetch_proxio()['rows']
    assert not manager.proxio_download_active
    assert [call.args[0] for call in get.call_args_list] == [
        'https://proxio.io/api/list', 'https://proxio.io/download', 'https://proxio.io/api/list']


def test_public_snapshot_refreshes_every_five_minutes_between_healthy_api_calls(scanner, proxio, monkeypatch):
    manager, clock = proxio
    monkeypatch.setattr(scanner, 'PROXIO_FREE_REFRESH', 1200)
    get = Mock(return_value=ProviderResponse(calls_today=3, daily_limit=100))
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio()['rows']
    for _ in range(3):
        assert not manager._source_due('proxio', clock[0] + 299, force=True)
        clock[0] += 300
        assert manager._source_due('proxio', clock[0])
        assert manager._fetch_proxio()['rows']
    assert [call.args[0] for call in get.call_args_list] == [
        'https://proxio.io/api/list', 'https://proxio.io/download',
        'https://proxio.io/download', 'https://proxio.io/download']
    assert manager.proxio_key_state[0]['calls'] == 3
    clock[0] += 300
    assert manager._fetch_proxio()['rows']
    assert get.call_args.args[0].endswith('/api/list')
    assert not manager._source_due('proxio', clock[0] + 299)


@pytest.mark.parametrize('response', [
    ProviderResponse(status=503), ProviderResponse(raw=b'not json'),
    ProviderResponse(raw=b'x' * (1024 * 1024 + 1)),
    ProviderResponse(rows=[proxy_row(anonymity='Transparent')]),
])
def test_bad_download_preserves_previous_snapshot_and_backs_off(scanner, proxio, monkeypatch, response):
    manager, clock = proxio
    old = ['http://1.1.1.1:8080']
    manager.snapshots['proxio'] = list(old)
    manager.proxio_next_retry_at = clock[0] + 86400
    manager.proxio_download_active = True
    monkeypatch.setattr(scanner.requests, 'get', Mock(return_value=response))
    monkeypatch.setattr(threading.current_thread(), 'name', 'proxy-metadata-worker')
    assert manager.refresh_all() is False
    assert manager.snapshots['proxio'] == old
    assert not manager._source_due('proxio', clock[0] + 299, force=True)
    assert manager._source_due('proxio', clock[0] + 300)
    assert response.closed


def test_public_json_uses_exact_local_quality_filter_dedup_and_cap(scanner, proxio, monkeypatch):
    manager, _clock = proxio
    rows = [proxy_row('8.8.%d.%d' % (i // 250, i % 250 + 1)) for i in range(750)]
    rows += [proxy_row('8.8.0.1', port=3128), proxy_row('1.1.1.1', anonymity='Transparent'),
             proxy_row('1.1.1.2', protocols=['SOCKS5']), proxy_row('1.1.1.3', reliability=20)]
    response = ProviderResponse(rows=rows)
    monkeypatch.setattr(scanner.requests, 'get', Mock(return_value=response))
    expected = manager._filter_proxio_rows(rows)
    assert len(expected) == 500 and len({scanner._proxy_host(p) for p in expected}) == 500
    assert manager._fetch_proxio_download() == expected
    assert response.closed


def test_unrated_but_strong_rows_are_preserved_by_both_paths(scanner, proxio, monkeypatch):
    manager, _clock = proxio
    rows = [proxy_row(reliability=None, latency_s=.8, uptime=.9)]
    assert manager._filter_proxio_rows(rows) == ['http://8.8.8.8:8080']
    get = Mock(return_value=ProviderResponse(rows=rows))
    monkeypatch.setattr(scanner.requests, 'get', get)
    assert manager._fetch_proxio_download() == ['http://8.8.8.8:8080']
    assert 'min_reliability' not in get.call_args.kwargs['params']
