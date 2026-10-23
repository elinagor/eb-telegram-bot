"""Offline private-reserve regressions; no production credentials or network."""
import os
import stat
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from private_reserve import PrivateReserve, RESERVE_ID, normal_check_delay_bounds
from proxy_runtime import ProbeLimiter
from test_coordinator_uk import scanner, forbid_unmocked_network


SEARCH = 'https://www.ebay.co.uk/sch/i.html?_nkw=test&_sop=10'
ENDPOINT = 'https://offline-user:' + ('test-password-' * 4) + '@reserve.example:8443'


@pytest.fixture
def reserve_factory(monkeypatch):
    # The positive policy tests substitute certificate parsing only. The separate
    # malformed-material test below uses the real TLS parser and must fail closed.
    monkeypatch.setattr('private_reserve.ssl.SSLContext', lambda *args: Mock())
    instances = []

    def make(**kwargs):
        env = dict(UK_RESERVE_URL=ENDPOINT, UK_RESERVE_CA_PEM='offline CA',
                   UK_RESERVE_CLIENT_CERT_PEM='offline certificate',
                   UK_RESERVE_CLIENT_KEY_PEM='offline key')
        env.update(kwargs.pop('environ', {}))
        instance = PrivateReserve(env, SEARCH, **kwargs)
        instances.append(instance)
        return instance

    yield make
    for instance in instances:
        instance.close()


@pytest.mark.parametrize('raw,expected', [('15', (30, 45)), ('30', (30, 45)),
                                        ('40', (40, 45)), ('90', (45, 45)),
                                        ('invalid', (30, 45)), (None, (30, 45))])
def test_requested_normal_cadence(raw, expected):
    assert normal_check_delay_bounds(raw) == expected


def test_normal_healthy_sleep_does_not_unlock_reserve(reserve_factory):
    clock = [100.0]
    reserve = reserve_factory(clock=lambda: clock[0])
    assert reserve.enabled
    for _ in range(20):
        clock[0] += 45
        assert not reserve.eligible()
        reserve.note_success()
    reserve.note_failure()
    clock[0] += 24.99
    assert not reserve.eligible()
    clock[0] += .01
    assert reserve.eligible()
    reserve.note_success()
    clock[0] += 900
    assert not reserve.eligible()


def test_failure_attempt_start_and_short_failed_cycles_preserved(reserve_factory):
    clock = [100.0]
    reserve = reserve_factory(clock=lambda: clock[0])
    reserve.note_failure(started=90)
    clock[0] = 110
    reserve.note_failure(started=105)
    assert reserve.failure_since == 90
    assert not reserve.eligible()
    clock[0] = 115
    assert reserve.eligible()


class PendingExecutor:
    def __init__(self):
        self.jobs = []

    def submit(self, function, *args):
        future = Future()
        self.jobs.append((future, function, args))
        return future

    def run(self):
        future, function, args = self.jobs.pop(0)
        future.set_result(function(*args))
        return future


def test_existing_global_limit_serialization_and_cancellation(reserve_factory, monkeypatch):
    clock = [100.0]
    reserve = reserve_factory(clock=lambda: clock[0])
    reserve.note_failure(started=0)
    monkeypatch.setattr(reserve, '_request_once', lambda *args: ('success', 'valid'))
    executor, limiter = PendingExecutor(), ProbeLimiter(maximum=1)
    foreign = limiter.submit(executor, lambda: None, identity='ordinary')
    assert reserve.submit(executor, limiter, {}, None) is None
    assert reserve.last_attempt is None and not reserve.in_flight
    foreign.cancel()
    executor.jobs.clear()
    future = reserve.submit(executor, limiter, {}, None)
    assert future is not None and limiter.count() == 1
    assert reserve.submit(executor, limiter, {}, None) is None
    future.cancel()
    assert not reserve.in_flight and limiter.count() == 0


def test_simultaneous_callers_share_one_private_admission(reserve_factory, monkeypatch):
    reserve = reserve_factory(clock=lambda: 100)
    reserve.note_failure(started=0)
    monkeypatch.setattr(reserve, '_request_once', lambda *args: ('success', 'valid'))
    executor, limiter = PendingExecutor(), ProbeLimiter(maximum=12)
    with ThreadPoolExecutor(max_workers=8) as callers:
        futures = list(callers.map(lambda _n: reserve.submit(executor, limiter, {}, None), range(32)))
    assert sum(future is not None for future in futures) == 1
    assert limiter.count() == 1 and len(executor.jobs) == 1
    executor.run()
    assert limiter.count() == 0 and not reserve.in_flight


def test_spacing_quota_and_five_minute_rest(reserve_factory, monkeypatch):
    clock = [100.0]
    reserve = reserve_factory(clock=lambda: clock[0])
    reserve.note_failure(started=0)
    monkeypatch.setattr(reserve, '_request_once', lambda *args: ('success', 'valid'))
    executor, limiter = PendingExecutor(), ProbeLimiter(maximum=1)
    for attempt in range(3):
        clock[0] = 100 + 45 * attempt
        future = reserve.submit(executor, limiter, {}, None)
        assert future is not None
        assert executor.run().result() == ('success', 'valid', None)
        assert not reserve.in_flight and limiter.count() == 0
        if attempt < 2:
            clock[0] += 44.99
            assert reserve.submit(executor, limiter, {}, None) is None
    assert reserve.cooldown_until == 490
    clock[0] = 489.99
    assert not reserve.eligible()
    clock[0] = 490
    assert reserve.eligible()


@pytest.mark.parametrize('result,seconds', [('blocked', 900), ('rate_limited', 1800),
                                           ('proxy_ssl', 3600), ('proxy_auth', 3600),
                                           ('proxy_error', 90), ('proxy_timeout', 90)])
def test_failure_cooldowns(reserve_factory, monkeypatch, result, seconds):
    clock = [100.0]
    reserve = reserve_factory(clock=lambda: clock[0])
    reserve.note_failure(started=0)
    monkeypatch.setattr(reserve, '_request_once', lambda *args: (result, None))
    executor = PendingExecutor()
    reserve.submit(executor, ProbeLimiter(maximum=1), {}, None)
    executor.run()
    assert reserve.cooldown_until == 100 + seconds
    clock[0] = 100 + seconds - .01
    assert not reserve.eligible()
    clock[0] += .01
    assert reserve.eligible()


def test_verified_mtls_options_exact_target_no_redirects_and_close(reserve_factory):
    import certifi
    from curl_cffi.const import CurlOpt
    response = Mock(status_code=200, text='valid', url=SEARCH)
    session = Mock()
    session.get.return_value = response
    factory = Mock(return_value=session)
    reserve = reserve_factory(session_factory=factory)
    assert reserve._request_once({'impersonate': 'chrome'}, lambda *args: 'success') == ('success', 'valid')
    kwargs = factory.call_args.kwargs
    assert kwargs['verify'] is True and kwargs['trust_env'] is False
    assert kwargs['allow_redirects'] is False
    assert kwargs['proxy'] == ENDPOINT
    options = kwargs['curl_options']
    assert options[CurlOpt.CAINFO] == certifi.where()
    assert options[CurlOpt.CAINFO] != options[CurlOpt.PROXY_CAINFO]
    assert options[CurlOpt.PROXY_SSL_VERIFYPEER] == 1
    assert options[CurlOpt.PROXY_SSL_VERIFYHOST] == 2
    assert set((CurlOpt.PROXY_CAINFO, CurlOpt.PROXY_SSLCERT, CurlOpt.PROXY_SSLKEY)) <= options.keys()
    for option in (CurlOpt.PROXY_CAINFO, CurlOpt.PROXY_SSLCERT, CurlOpt.PROXY_SSLKEY):
        path = options[option]
        assert os.path.isfile(path)
        if os.name != 'nt':
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    if os.name != 'nt':
        assert stat.S_IMODE(os.stat(reserve._directory.name).st_mode) == 0o700
    session.get.assert_called_once_with(SEARCH, timeout=(5.0, 12.0), allow_redirects=False)
    response.close.assert_called_once()
    session.close.assert_called_once()
    directory = reserve._directory.name
    reserve.close()
    assert not os.path.exists(directory)


def test_curl_default_ca_initialization_preserves_separate_proxy_trust(reserve_factory, monkeypatch):
    import certifi
    from curl_cffi import Curl, CurlOpt

    reserve = reserve_factory()
    # Exercise the real curl_cffi pre-perform CA initialization without making
    # any connection. It must keep system origin trust and private proxy trust
    # separate, including when curl initializes its own default CA bundle.
    curl = Curl()
    try:
        actual_setopt = curl.setopt
        configured = {}
        calls = []

        def observed_setopt(option, value):
            configured[option] = value
            calls.append(option)
            return actual_setopt(option, value)

        monkeypatch.setattr(curl, 'setopt', observed_setopt)
        for option, value in reserve._curl_options.items():
            curl.setopt(option, value)
        curl._ensure_cacert()
        assert configured[CurlOpt.PROXY_CAINFO] == reserve._curl_options[CurlOpt.PROXY_CAINFO]
        assert configured[CurlOpt.CAINFO] == certifi.where()
        assert configured[CurlOpt.CAINFO] != configured[CurlOpt.PROXY_CAINFO]
        assert calls.count(CurlOpt.PROXY_CAINFO) == 1
        assert configured[CurlOpt.PROXY_SSL_VERIFYPEER] == 1
        assert configured[CurlOpt.PROXY_SSL_VERIFYHOST] == 2
    finally:
        curl.close()


@pytest.mark.parametrize('message,result', [('certificate rejected secret URL', 'proxy_ssl'),
                                           ('CONNECT 407 secret URL', 'proxy_auth'),
                                           ('curl: (28) timed out secret URL', 'proxy_timeout'),
                                           ('other secret URL', 'proxy_error')])
def test_transport_failures_return_only_safe_classifications(reserve_factory, caplog, message, result):
    session = Mock()
    session.get.side_effect = RuntimeError(message + ENDPOINT)
    reserve = reserve_factory(session_factory=Mock(return_value=session))
    assert reserve._request_once({'impersonate': 'chrome'}, lambda *args: 'success') == (result, None)
    assert ENDPOINT not in caplog.text and 'secret URL' not in caplog.text
    session.close.assert_called_once()


@pytest.mark.parametrize('target', ['http://www.ebay.co.uk/sch/i.html',
                                    'https://www.ebay.com/sch/i.html',
                                    'https://www.ebay.co.uk:444/sch/i.html',
                                    'https://user@www.ebay.co.uk/sch/i.html',
                                    'https://www.ebay.co.uk/itm/123456789012'])
def test_ineligible_search_configuration_fails_closed(target):
    env = dict(UK_RESERVE_URL=ENDPOINT, UK_RESERVE_CA_PEM='CA',
               UK_RESERVE_CLIENT_CERT_PEM='certificate', UK_RESERVE_CLIENT_KEY_PEM='key')
    reserve = PrivateReserve(env, target)
    assert not reserve.enabled and reserve._directory is None


def test_missing_and_invalid_material_fail_closed_without_files(caplog):
    assert not PrivateReserve({}, SEARCH).enabled
    assert not PrivateReserve({'UK_RESERVE_URL': ENDPOINT}, SEARCH).enabled
    env = dict(UK_RESERVE_URL=ENDPOINT, UK_RESERVE_CA_PEM='invalid',
               UK_RESERVE_CLIENT_CERT_PEM='invalid', UK_RESERVE_CLIENT_KEY_PEM='invalid')
    reserve = PrivateReserve(env, SEARCH)
    assert not reserve.enabled and reserve._directory is None
    assert ENDPOINT not in caplog.text


def test_existing_scanner_classification_rejects_blocks(scanner, monkeypatch):
    assert scanner._classify_private_reserve_response(Mock(status_code=403), '') == 'blocked'
    assert scanner._classify_private_reserve_response(Mock(status_code=429), '') == 'rate_limited'
    assert scanner._classify_private_reserve_response(Mock(status_code=407), '') == 'proxy_auth'
    monkeypatch.setattr(scanner, '_is_ebay_block_page', lambda *args: (True, 'challenge'))
    assert scanner._classify_private_reserve_response(Mock(status_code=200), 'page') == 'blocked'
    monkeypatch.setattr(scanner, '_is_ebay_block_page', lambda *args: (False, None))
    monkeypatch.setattr(scanner, '_looks_like_search_results', lambda body: body == 'search')
    assert scanner._classify_private_reserve_response(Mock(status_code=200), 'page') == 'http_error'
    assert scanner._classify_private_reserve_response(Mock(status_code=200), 'search') == 'success'


def test_late_private_success_never_mutates_shared_reputation(scanner, monkeypatch):
    success, failure = Mock(), Mock()
    monkeypatch.setattr(scanner.proxy_manager, 'mark_success', success)
    monkeypatch.setattr(scanner.proxy_manager, 'mark_failure', failure)
    live_session = Mock()
    monkeypatch.setattr(scanner, 'fixed_proxy', 'http://working.example:8080')
    monkeypatch.setattr(scanner, 'fixed_session', live_session)
    main_success, feedback = Mock(), Mock()
    monkeypatch.setattr(scanner, 'record_ebay_success', main_success)
    monkeypatch.setattr(scanner.coordinator_feed, 'record_result', feedback)
    future = Future()
    future.set_result(('success', 'valid private search', None))
    scanner._cleanup_late_probe_future(future, RESERVE_ID)
    success.assert_not_called()
    failure.assert_not_called()
    main_success.assert_not_called()
    feedback.assert_not_called()
    assert scanner.fixed_proxy == 'http://working.example:8080'
    assert scanner.fixed_session is live_session
    live_session.close.assert_not_called()


def test_private_one_shot_uses_global_budget_and_never_becomes_fixed(scanner, monkeypatch):
    # Keep normal candidates pending so the private request wins the same executor
    # without ever handing its identity to normal provider/pool/sticky accounting.
    reserve = Mock(configured=True, enabled=True)
    submitted = [False]
    executor = PendingExecutor()
    executor.shutdown = Mock()
    limiter = ProbeLimiter(maximum=12)

    def submit_reserve(actual_executor, actual_limiter, profile, classify):
        if submitted[0]:
            return None
        submitted[0] = True
        assert actual_executor is executor and actual_limiter is limiter
        return actual_limiter.submit(executor, lambda: ('success', 'private search', None),
                                     identity=RESERVE_ID)

    reserve.submit.side_effect = submit_reserve
    monkeypatch.setattr(scanner, 'private_reserve', reserve)
    manager = scanner.ProxyManager()
    ordinary = ['http://ordinary-%s.example:8080' % i for i in range(8)]
    manager.proxies = manager.all_proxies = ordinary
    manager.last_refresh = scanner.time.time()
    monkeypatch.setattr(scanner, 'proxy_manager', manager)
    monkeypatch.setattr(scanner, 'probe_limiter', limiter)
    monkeypatch.setattr(scanner, 'ThreadPoolExecutor', lambda **kwargs: executor)
    monkeypatch.setattr(scanner, 'fixed_proxy', None)
    monkeypatch.setattr(scanner, 'fixed_profile', None)
    monkeypatch.setattr(scanner, 'fixed_session', None)
    monkeypatch.setattr(scanner, '_adopt_webshare_handoff_if_ready', lambda: None)
    monkeypatch.setattr(scanner, '_consume_restart_sticky_candidate', lambda: None)
    monkeypatch.setattr(scanner, '_memory_maintenance', lambda *args, **kwargs: None)
    monkeypatch.setattr(scanner, '_memory_rss_mb', lambda: 100)
    monkeypatch.setattr(scanner, 'get_preferred_profile', lambda: {'name': 'offline'})
    monkeypatch.setattr(scanner.provider_manager, 'has_usable_webshare', lambda: False)
    monkeypatch.setattr(scanner, 'WEBSHARE_FIRST_BATCH', False)
    monkeypatch.setattr(scanner.external_free_manager, 'refresh_all_async', lambda **kwargs: None)
    sticky, account, success = Mock(), Mock(), Mock()
    monkeypatch.setattr(scanner, '_queue_restart_sticky_persist', sticky)
    monkeypatch.setattr(scanner.provider_manager, 'record_winner', account)
    monkeypatch.setattr(scanner, 'record_ebay_success', success)

    def finish_private(active, **kwargs):
        assert len(active) <= scanner.PROBE_CONCURRENCY
        assert limiter.count() <= scanner.PROBE_CONCURRENCY
        future, function, args = executor.jobs.pop(0)
        future.set_result(function(*args))
        return {future}, set(active) - {future}

    monkeypatch.setattr(scanner, 'wait', finish_private)
    try:
        assert scanner.fetch_ebay_html_with_fixed_pair() == 'private search'
        assert scanner.fixed_proxy is None and scanner.fixed_session is None
        assert RESERVE_ID not in manager.proxies
        assert not manager.last_success_at
        sticky.assert_not_called()
        account.assert_not_called()
        success.assert_called_once()
        reserve.note_failure.assert_called_once()
        assert limiter.count() == 0
    finally:
        for future, _function, _args in executor.jobs:
            future.cancel()
        scanner.main_discovery_active_event.clear()


def test_healthy_fixed_route_never_considers_emergency_despite_long_idle(scanner, monkeypatch):
    reserve = Mock()
    monkeypatch.setattr(scanner, 'private_reserve', reserve)
    monkeypatch.setattr(scanner, 'fixed_proxy', 'http://ordinary.example:8080')
    monkeypatch.setattr(scanner, 'fixed_profile', {'name': 'offline'})
    session = Mock()
    monkeypatch.setattr(scanner, 'fixed_session', session)
    monkeypatch.setattr(scanner, '_adopt_webshare_handoff_if_ready', lambda: None)
    monkeypatch.setattr(scanner, '_make_request', lambda *args, **kwargs: ('success', 'ordinary search', session))
    monkeypatch.setattr(scanner.proxy_manager, 'mark_success', Mock())
    monkeypatch.setattr(scanner, '_queue_restart_sticky_persist', Mock())
    success = Mock()
    monkeypatch.setattr(scanner, 'record_ebay_success', success)
    # Even a very old global success timestamp is deliberately irrelevant here.
    monkeypatch.setattr(scanner, 'last_ebay_success_at', 0)
    assert scanner.fetch_ebay_html_with_fixed_pair() == 'ordinary search'
    reserve.note_failure.assert_not_called()
    reserve.submit.assert_not_called()
    success.assert_called_once()
