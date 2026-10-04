"""UK integration regressions; all outside connections forbidden."""
import ast
import json
import hashlib
import threading
import time
from pathlib import Path
from unittest.mock import Mock
import pytest
from test_coordinator_uk import scanner, feed, Response, payload, forbid_unmocked_network


def test_legacy_failure_retains_independent_candidates(scanner, feed, monkeypatch):
    client, clock, _transport = feed
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager('https://legacy.example')
    old = ['http://1.1.1.1:80', 'socks5://9.9.9.9:1080']
    manager.proxies = list(old)
    manager.all_proxies = list(old)
    manager.legacy_current = frozenset(old)
    manager.refresh_coordinator()
    monkeypatch.setattr(manager, 'fetch_proxies_from_api', lambda **kw: [])
    monkeypatch.setattr(threading.current_thread(), 'name', 'proxy-metadata-worker')
    manager.refresh_proxies(force=True)
    assert set(old) <= set(manager.proxies)
    clock[0] += 181
    manager.refresh_coordinator()
    assert set(old) <= set(manager.proxies)


def test_scanner_threads_only_queue_source_updates(scanner, monkeypatch):
    manager = scanner.ProxyManager('https://legacy.example')
    with scanner.proxy_metadata_lock:
        scanner.proxy_metadata_jobs.clear()
    fetch = Mock(side_effect=AssertionError('API on scanner thread'))
    monkeypatch.setattr(manager, 'fetch_proxies_from_api', fetch)
    manager.refresh_proxies(force=True)
    manager.refresh_standard_merge(force=True)
    manager.refresh_deep_emergency(force=True)
    scanner.provider_manager.refresh_all(force=True)
    scanner.external_free_manager.refresh_all(force=True)
    scanner.external_free_manager.refresh_all_async(force=True)
    fetch.assert_not_called()
    assert {'standard', 'merge', 'deep'} <= scanner.proxy_metadata_jobs


def test_cached_managed_success_cannot_bypass_account_guards(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    managed = 'http://user:secret@8.8.8.8:80'
    free = 'http://1.1.1.1:80'
    manager.proxies = [free]
    manager.last_success_at[managed] = time.time()
    monkeypatch.setattr(scanner.provider_manager, 'source_fast', lambda p: 'webshare' if p == managed else 'free')
    monkeypatch.setattr(scanner.provider_manager, 'managed_proxy_available', lambda p: True)
    assert manager.get_candidate_batch(4, allow_webshare=False) == [free]


def test_due_tls_recheck_renews_before_expiry(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    proxy = 'http://8.8.8.8:80'
    manager.quality_ok_until[proxy] = time.time() + 20
    monkeypatch.setattr(scanner, 'proxy_manager', manager)
    monkeypatch.setattr(scanner, '_tcp_preflight_proxy', lambda *a, **kw: (True, None))
    tcp, tls = Mock(), Mock()
    connect = Mock(return_value=tcp)
    monkeypatch.setattr(scanner.socket, 'create_connection', connect)
    monkeypatch.setattr(scanner, '_quality_recv_headers', lambda *a: b'HTTP/1.1 200 Connection established\r\n\r\n')
    context = Mock()
    context.wrap_socket.return_value = tls
    monkeypatch.setattr(scanner.ssl, 'create_default_context', lambda: context)
    monkeypatch.setattr(scanner, '_quality_tls_context', None)
    assert scanner._quality_https_preflight_proxy(proxy) == (True, None)
    connect.assert_called_once()
    tls.do_handshake.assert_called_once()
    tls.close.assert_called_once()
    assert manager.quality_ok_until[proxy] - time.time() > 100
    assert scanner._quality_https_preflight_proxy(proxy) == (True, None)
    connect.assert_called_once()


def card(i, cls='s-item', price='£12.50', extras=''):
    return f'<li class="{cls}"><a class="s-item__link" href="https://www.ebay.co.uk/itm/{123456789000+i}"><h3 class="s-item__title">New listing Camera {i}</h3></a><span class="s-item__price">{price}</span><span class="s-item__shipping">£4.00 postage</span>{extras}</li>'


@pytest.mark.parametrize('root', ['id="srp-river-results"', 'class="srp-river-results extra"', 'class="extra srp-results"'])
@pytest.mark.parametrize('cls', ['s-item', 's-card', 'su-card-container'])
def test_root_limited_parser_preserves_uk_output_and_rewrite_boundary(scanner, monkeypatch, root, cls):
    html = '<html><body><aside>' + card(99, cls) + '</aside><ul ' + root + '>'
    html += card(1, cls, extras='<span class="s-item__bid-count">3 bids</span><span>Best Offer</span>')
    html += card(2, cls, '£20.00', '<span>Buy It Now</span><span class="s-item__price">£30.00</span>')
    html += '<div class="srp-river-answer--REWRITE_START">Expanded results</div>' + card(3, cls) + '</ul></body></html>'
    new = scanner.parse_ebay_listings(html)
    monkeypatch.setattr(scanner, '_main_search_soup_bounded', lambda html: scanner.BeautifulSoup(html, 'html.parser'))
    old = scanner.parse_ebay_listings(html)
    assert new == old and len(new) == 2
    assert new['123456789001']['auction']
    assert new['123456789001']['price'] == '£12.50'
    assert '123456789099' not in new and '123456789003' not in new


def test_global_jsonld_survives_root_only_dom(scanner, monkeypatch):
    structured = {'@type': 'Product', 'url': 'https://www.ebay.co.uk/itm/123456789001',
                  'offers': {'@type': 'Offer', 'priceCurrency': 'GBP', 'price': '27.99'}}
    html = '<html><head><script type="application/ld+json">' + json.dumps(structured) + '</script></head><body><ul id="srp-river-results">' + card(1, price='') + '</ul></body></html>'
    soup = scanner._main_search_soup_bounded(html)
    assert len(soup.find_all('script', type='application/ld+json')) == 1
    soup.decompose()
    new = scanner.parse_ebay_listings(html)
    monkeypatch.setattr(scanner, '_main_search_soup_bounded', lambda html: scanner.BeautifulSoup(html, 'html.parser'))
    assert new == scanner.parse_ebay_listings(html)


def test_missing_srp_never_notifies_recommendations(scanner):
    assert scanner.parse_ebay_listings('<aside>' + card(1) + '</aside>') is None


def test_ready_reserve_counts_local_tls_and_independent_hosts(scanner):
    manager = scanner.ProxyManager()
    primary = 'http://8.8.8.8:80'
    independent = 'http://1.1.1.1:80'
    twin = 'socks5://1.1.1.1:1080'
    manager.proxies = [primary, independent, twin]
    manager.coordinator_current = frozenset([primary])
    manager.coordinator_valid_until = time.time() + 100
    assert manager.reserve_diagnostics()['tls_ips'] == 0
    manager.quality_ok_until = {p: time.time() + 100 for p in manager.proxies}
    assert manager.reserve_diagnostics()['tls_ips'] == 2
    manager.host_bad_until['8.8.8.8'] = time.time() + 100
    assert manager.reserve_diagnostics()['tls_ips'] == 1


def test_existing_durable_uk_features_are_preserved():
    base = Path(__file__).resolve().parent
    before = json.loads((base / 'uk_feature_baseline.json').read_text(encoding='utf-8'))
    current = ast.parse((base / 'app.py').read_text(encoding='utf-8').replace('v6.52', 'v6.50').replace('v6.51', 'v6.50'))
    after = {n.name: hashlib.sha256(ast.dump(n, include_attributes=False).encode()).hexdigest()
             for n in current.body if isinstance(n, ast.FunctionDef)}
    # Hashes from UK v6.50: persisted jobs, duplicate prevention and Telegram content.
    for name in before:
        assert before[name] == after[name], name


def test_hot_failover_finds_ready_replacement_without_api_wait(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    old, good = 'http://8.8.8.8:80', 'socks5://1.1.1.1:1080'
    manager.proxies = manager.all_proxies = [good]
    manager.last_refresh = time.time()
    manager.last_success_at[good] = time.time()
    monkeypatch.setattr(scanner, 'proxy_manager', manager)
    monkeypatch.setattr(scanner, 'fixed_proxy', old)
    monkeypatch.setattr(scanner, 'fixed_profile', {'name': 'test'})
    monkeypatch.setattr(scanner, 'fixed_session', Mock())
    monkeypatch.setattr(scanner, '_adopt_webshare_handoff_if_ready', lambda: None)
    monkeypatch.setattr(scanner, '_memory_rss_mb', lambda: 100)
    monkeypatch.setattr(scanner, '_memory_maintenance', lambda *a, **kw: None)
    monkeypatch.setattr(scanner, 'get_preferred_profile', lambda: {'name': 'test'})
    monkeypatch.setattr(scanner, '_tcp_preflight_proxy', lambda *a, **kw: (True, None))
    monkeypatch.setattr(scanner, '_consume_restart_sticky_candidate', lambda: None)
    monkeypatch.setattr(scanner, '_queue_restart_sticky_persist', lambda *a, **kw: None)
    monkeypatch.setattr(scanner, 'wake_queued_auctions_for_new_fixed', lambda: None)
    monkeypatch.setattr(scanner, 'record_ebay_success', lambda: None)
    monkeypatch.setattr(manager, 'fetch_proxies_from_api', Mock(side_effect=AssertionError('API on hot path')))
    request = Mock(side_effect=lambda p, *a, **kw: ('blocked', None, kw.get('session')) if p == old
                   else ('success', 'fresh UK search response', Mock()))
    monkeypatch.setattr(scanner, '_make_request', request)
    assert scanner.fetch_ebay_html_with_fixed_pair() == 'fresh UK search response'
    assert scanner.fixed_proxy == good
    assert request.call_count == 2
    manager.fetch_proxies_from_api.assert_not_called()
    assert scanner.probe_limiter.count() == 0
