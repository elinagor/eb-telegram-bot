"""Offline Webshare pagination bounds; never use provider credentials or network."""

from unittest.mock import Mock

import pytest

from test_coordinator_uk import scanner, forbid_unmocked_network


LIST_URL = 'https://proxy.webshare.io/api/v2/proxy/list/'


def proxy_row(host='11.0.0.1', **extra):
    row = dict(valid=True, proxy_address=host, port=8080,
               username='offline-user', password='offline-password')
    row.update(extra)
    return row


def proxy_uri(host='11.0.0.1'):
    return f'http://offline-user:offline-password@{host}:8080'


class ProviderResponse:
    def __init__(self, rows=(), next_url=None, status=200, json_error=None):
        self.status_code = status
        self.data = {'results': list(rows), 'next': next_url}
        self.json_error = json_error
        self.closed = False

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.data

    def close(self):
        self.closed = True


@pytest.fixture
def webshare(scanner, monkeypatch):
    monkeypatch.setattr(scanner, 'WEBSHARE_API_KEYS', ('offline-only-key',))
    manager = scanner.ProviderManager()
    state = manager.webshare_accounts[1]
    cached = proxy_uri('12.0.0.1')
    state['proxies'] = {cached}
    state['last_proxy_refresh'] = 123.0
    manager._rebuild_webshare_snapshot_locked()
    return manager, state, cached


def test_two_pages_return_deduplicated_rows_and_close_responses(scanner, webshare, monkeypatch):
    manager, state, _cached = webshare
    second_url = LIST_URL + '?page=2'
    first = ProviderResponse([proxy_row()], next_url=second_url)
    second = ProviderResponse([proxy_row(), proxy_row('11.0.0.2')])
    get = Mock(side_effect=[first, second])
    monkeypatch.setattr(scanner.requests, 'get', get)

    assert manager._fetch_webshare_proxy_list(1, state) == [proxy_uri(), proxy_uri('11.0.0.2')]
    assert [call.args[0] for call in get.call_args_list] == [LIST_URL, second_url]
    assert get.call_args_list[0].kwargs['params']['page_size'] == 100
    assert get.call_args_list[1].kwargs.get('params') is None
    for call in get.call_args_list:
        assert call.kwargs['allow_redirects'] is False
        assert call.kwargs['headers'] == {'Authorization': 'Token offline-only-key'}
    assert first.closed and second.closed


@pytest.mark.parametrize('empty_rows', [True, False])
def test_repeated_next_url_is_rejected_without_another_request(
    scanner, webshare, monkeypatch, empty_rows
):
    manager, state, _cached = webshare
    response = ProviderResponse([] if empty_rows else [proxy_row()], next_url=LIST_URL)
    get = Mock(return_value=response)
    monkeypatch.setattr(scanner.requests, 'get', get)

    assert manager._fetch_webshare_proxy_list(1, state) is None
    assert get.call_count == 1
    assert response.closed


def test_cycle_after_second_page_keeps_old_snapshot(scanner, webshare, monkeypatch):
    manager, state, cached = webshare
    second_url = LIST_URL + '?page=2'
    first = ProviderResponse([proxy_row()], next_url=second_url)
    second = ProviderResponse([proxy_row('11.0.0.2')], next_url=second_url)
    get = Mock(side_effect=[first, second])
    monkeypatch.setattr(scanner.requests, 'get', get)

    manager._refresh_webshare_all(force=True, include_stats=False)
    assert get.call_count == 2
    assert state['proxies'] == {cached}
    assert manager.proxy_sets['webshare'] == {cached}
    assert state['last_proxy_refresh'] == 123.0
    assert first.closed and second.closed


def test_five_terminal_pages_and_500_rows_are_accepted(scanner, webshare, monkeypatch):
    manager, state, _cached = webshare
    pages = [
        ProviderResponse(
            [proxy_row(f'11.0.{page}.{index}') for index in range(1, 101)],
            next_url=LIST_URL + f'?page={page + 1}' if page < 5 else None,
        )
        for page in range(1, 6)
    ]
    get = Mock(side_effect=pages)
    monkeypatch.setattr(scanner.requests, 'get', get)

    result = manager._fetch_webshare_proxy_list(1, state)
    assert result is not None and len(result) == 500
    assert len(set(result)) == 500
    assert get.call_count == 5
    assert all(response.closed for response in pages)


def test_next_after_fifth_empty_page_is_rejected_without_sixth_request(
    scanner, webshare, monkeypatch
):
    manager, state, _cached = webshare
    pages = [ProviderResponse([], next_url=LIST_URL + f'?page={page + 1}')
             for page in range(1, 6)]
    get = Mock(side_effect=pages)
    monkeypatch.setattr(scanner.requests, 'get', get)

    assert manager._fetch_webshare_proxy_list(1, state) is None
    assert get.call_count == 5
    assert all(response.closed for response in pages)


@pytest.mark.parametrize('next_url', [
    'https://external.example/api/v2/proxy/list/?page=2',
    'https://proxy.webshare.io.external.example/api/v2/proxy/list/?page=2',
    'http://proxy.webshare.io/api/v2/proxy/list/?page=2',
    'ftp://proxy.webshare.io/api/v2/proxy/list/?page=2',
    'https://proxy.webshare.io:444/api/v2/proxy/list/?page=2',
    'https://offline-user@proxy.webshare.io/api/v2/proxy/list/?page=2',
    'https://offline-user:offline-password@proxy.webshare.io/api/v2/proxy/list/?page=2',
    'https://proxy.webshare.io/api/v2/subscription/?page=2',
    'https://proxy.webshare.io/api/v2/proxy/list/?page=2#fragment',
    'https://proxy.webshare.io/api/v2/proxy/list/?page=2\r\nX-Injected: offline',
    'https://proxy.webshare.io/api/v2/proxy/list/?page=2\x00',
    '/api/v2/proxy/list/?page=2',
    2,
    {'page': 2},
])
def test_unsafe_or_malformed_next_is_not_requested(scanner, webshare, monkeypatch, next_url):
    manager, state, cached = webshare
    response = ProviderResponse([proxy_row()], next_url=next_url)
    get = Mock(return_value=response)
    monkeypatch.setattr(scanner.requests, 'get', get)

    manager._refresh_webshare_all(force=True, include_stats=False)
    assert get.call_count == 1
    assert state['proxies'] == {cached}
    assert manager.proxy_sets['webshare'] == {cached}
    assert state['last_proxy_refresh'] == 123.0
    assert response.closed


def test_explicit_https_default_port_is_accepted(scanner, webshare, monkeypatch):
    manager, state, _cached = webshare
    second_url = 'https://proxy.webshare.io:443/api/v2/proxy/list/?page=2'
    responses = [ProviderResponse([proxy_row()], next_url=second_url),
                 ProviderResponse([proxy_row('11.0.0.2')])]
    get = Mock(side_effect=responses)
    monkeypatch.setattr(scanner.requests, 'get', get)

    assert manager._fetch_webshare_proxy_list(1, state) == [proxy_uri(), proxy_uri('11.0.0.2')]
    assert get.call_count == 2 and get.call_args.args[0] == second_url
    assert all(response.closed for response in responses)


@pytest.mark.parametrize('status', [302, 429, 500])
def test_error_responses_are_closed_and_cached_snapshot_is_retained(
    scanner, webshare, monkeypatch, status
):
    manager, state, cached = webshare
    response = ProviderResponse(status=status)
    get = Mock(return_value=response)
    monkeypatch.setattr(scanner.requests, 'get', get)

    manager._refresh_webshare_all(force=True, include_stats=False)
    assert get.call_count == 1
    assert get.call_args.kwargs['allow_redirects'] is False
    assert state['proxies'] == {cached}
    assert manager.proxy_sets['webshare'] == {cached}
    assert state['last_proxy_refresh'] == 123.0
    assert response.closed


@pytest.mark.parametrize('status', [401, 403])
def test_authorization_rejection_still_disables_only_its_account(
    scanner, webshare, monkeypatch, status
):
    manager, state, _cached = webshare
    response = ProviderResponse(status=status)
    get = Mock(return_value=response)
    monkeypatch.setattr(scanner.requests, 'get', get)
    before = scanner.time.time()

    assert manager._fetch_webshare_proxy_list(1, state) is None
    assert get.call_count == 1
    assert state['api_disabled_until'] >= before + 1800
    assert state['proxies'] == set()
    assert manager.proxy_sets['webshare'] == set()
    assert response.closed


def test_invalid_json_response_is_closed_and_old_snapshot_survives(scanner, webshare, monkeypatch):
    manager, state, cached = webshare
    response = ProviderResponse(json_error=ValueError('offline malformed JSON'))
    monkeypatch.setattr(scanner.requests, 'get', Mock(return_value=response))

    manager._refresh_webshare_all(force=True, include_stats=False)
    assert state['proxies'] == {cached}
    assert manager.proxy_sets['webshare'] == {cached}
    assert state['last_proxy_refresh'] == 123.0
    assert response.closed


def test_more_than_500_valid_rows_keeps_only_bounded_prefix(scanner, webshare, monkeypatch):
    manager, state, _cached = webshare
    hosts = [f'11.1.{index // 250}.{index % 250 + 1}' for index in range(501)]
    response = ProviderResponse([proxy_row(host) for host in hosts])
    get = Mock(return_value=response)
    monkeypatch.setattr(scanner.requests, 'get', get)

    result = manager._fetch_webshare_proxy_list(1, state)
    assert get.call_count == 1
    assert result == [proxy_uri(host) for host in hosts[:500]]
    assert proxy_uri(hosts[500]) not in result
    assert response.closed
