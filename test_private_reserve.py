"""Offline private-reserve regressions; no production credentials or network."""
import os
import runpy
import stat
from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from private_reserve import PrivateReserve, RESERVE_ID, MAX_FETCH_BYTES, normal_check_delay_bounds
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


@pytest.mark.parametrize('raw_url', [
    'https://www.ebay.co.uk/sch/34017/i.html?_nkw=test+kit&_sop=10&LH_PrefLoc=1',
    'https://www.ebay.co.uk/sch/i.html?_nkw=test&_ipg=240&_sop=1&LH_PrefLoc=2&LH_PrefLoc=3',
    'https://www.ebay.co.uk/sch/i.html?_nkw=test&LH_PrefLoc=1&_ipg=60&_sop=10',
])
def test_scanner_reserve_uses_normalized_search_url(scanner, monkeypatch, raw_url):
    # Exercise real scanner initialization with a captured transport constructor;
    # malformed/missing pagination must not give the reserve a different search.
    constructor = Mock()
    monkeypatch.setattr('private_reserve.PrivateReserve', constructor)
    monkeypatch.setenv('EBAY_SEARCH_URL', raw_url)
    initialized = runpy.run_path(
        scanner.__file__, run_name='uk_reserve_normalization_test',
    )
    expected = scanner.normalize_ebay_search_url(raw_url)
    constructor.assert_called_once_with(search_url=expected)
    assert initialized['EBAY_SEARCH_URL'] == expected


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
    from curl_cffi.const import CurlInfo, CurlOpt
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
    assert kwargs['curl_infos'] == [CurlInfo.HTTP_CONNECTCODE]
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


@pytest.mark.parametrize('peer,expected', [
    ('192.0.2.19', 'true'), ('192.0.2.20', 'false'), ('private-invalid-peer', 'unknown'),
])
def test_vps_route_diagnostic_is_sanitized(reserve_factory, caplog, peer, expected):
    from curl_cffi.const import CurlInfo
    endpoint = ENDPOINT.replace('reserve.example', '192.0.2.19')
    private_marker = 'DO-NOT-LOG-private-value'
    body = '<title>Access Denied ' + private_marker + '</title><p>' + endpoint + '</p>'

    class Response:
        status_code = 403
        url = SEARCH
        text = body
        content = body.encode()
        primary_ip = peer
        infos = {CurlInfo.HTTP_CONNECTCODE: 200}
        close = Mock()

        @property
        def headers(self):
            raise AssertionError('Diagnostic must not inspect private headers ' + private_marker)

        @property
        def cookies(self):
            raise AssertionError('Diagnostic must not inspect private cookies ' + private_marker)

    response = Response()
    session = Mock()
    session.get.return_value = response
    reserve = reserve_factory(environ={'UK_RESERVE_URL': endpoint},
                              session_factory=Mock(return_value=session))
    with caplog.at_level('INFO'):
        assert reserve._request_once({'impersonate': 'chrome'}, lambda *args: 'blocked') == ('blocked', None)
    message = caplog.records[-1].getMessage()
    assert 'source=VPS browser_fetch=false connect_http=200 origin_http=403' in message
    assert 'peer_matches_config=' + expected in message
    assert 'bytes=' + str(len(body.encode())) in message
    assert 'title_class=access_denied body_class=challenge result=blocked duration_ms=' in message
    assert all(secret not in caplog.text for secret in (endpoint, peer, private_marker, body, '192.0.2.19'))
    response.close.assert_called_once()
    session.close.assert_called_once()


@pytest.mark.parametrize('body,result,title_class,body_class', [
    ('<title>Items | eBay</title><div class="s-card">/itm/123</div>', 'success', 'ebay', 'search'),
    ('<title>Pardon Our Interruption</title>', 'blocked', 'challenge', 'challenge'),
    ('<title>private arbitrary title</title>', 'http_error', 'other', 'other'),
    ('', 'http_error', 'empty', 'empty'),
])
def test_diagnostic_page_classes_are_fixed_enums(reserve_factory, caplog, body, result, title_class, body_class):
    reserve = reserve_factory()
    response = SimpleNamespace(status_code=200, infos={}, content=body.encode(), primary_ip='')
    with caplog.at_level('INFO'):
        reserve._log_diagnostic(response, body, result, 0)
    assert 'title_class=' + title_class in caplog.text
    assert 'body_class=' + body_class in caplog.text
    assert 'private arbitrary title' not in caplog.text
    duration_ms = int(caplog.records[-1].getMessage().rsplit('duration_ms=', 1)[1])
    assert 0 <= duration_ms <= 3_600_000


def test_diagnostic_reads_connect_code_from_exception_without_logging_exception(reserve_factory, caplog):
    from curl_cffi.const import CurlInfo
    error = RuntimeError('CONNECT failed private error ' + ENDPOINT)
    error.response = SimpleNamespace(status_code=0, infos={CurlInfo.HTTP_CONNECTCODE: 407},
                                     content=b'', primary_ip='private-invalid-peer')
    session = Mock()
    session.get.side_effect = error
    reserve = reserve_factory(session_factory=Mock(return_value=session))
    with caplog.at_level('INFO'):
        assert reserve._request_once({'impersonate': 'chrome'}, lambda *args: 'success') == ('proxy_error', None)
    assert 'connect_http=407 origin_http=0' in caplog.text
    assert 'title_class=empty body_class=empty' in caplog.text
    assert 'private error' not in caplog.text and ENDPOINT not in caplog.text
    session.close.assert_called_once()


def test_hostile_diagnostic_values_cannot_be_stringified_or_logged(reserve_factory, caplog):
    from curl_cffi.const import CurlInfo

    class Hostile:
        def __str__(self):
            raise AssertionError('private stringification')

        def __eq__(self, other):
            raise AssertionError('private comparison')

    reserve = reserve_factory()
    response = SimpleNamespace(status_code=Hostile(), infos={CurlInfo.HTTP_CONNECTCODE: Hostile()},
                               content=Hostile(), primary_ip=Hostile())
    with caplog.at_level('INFO'):
        reserve._log_diagnostic(response, Hostile(), Hostile(), 0)
    assert 'connect_http=0 origin_http=0 peer_matches_config=unknown bytes=0' in caplog.text
    assert 'title_class=empty body_class=empty result=proxy_error' in caplog.text
    assert 'private' not in caplog.records[-1].getMessage()


@pytest.mark.parametrize('failure', ['getter', 'logger'])
def test_diagnostic_failure_never_changes_success_or_cleanup(reserve_factory, monkeypatch, failure):
    response = Mock(status_code=200, text='valid', url=SEARCH)
    if failure == 'getter':
        type(response).infos = property(lambda self: (_ for _ in ()).throw(RuntimeError('private getter')))
    else:
        monkeypatch.setattr('private_reserve.logging.info', Mock(side_effect=RuntimeError('private logger')))
    session = Mock()
    session.get.return_value = response
    reserve = reserve_factory(session_factory=Mock(return_value=session))
    assert reserve._request_once({'impersonate': 'chrome'}, lambda *args: 'success') == ('success', 'valid')
    response.close.assert_called_once()
    session.close.assert_called_once()


FETCH_CONNECT = ENDPOINT.replace('reserve.example', '192.0.2.19')
FETCH_CONFIG = FETCH_CONNECT.replace(':8443', ':28444') + '/v1/search'
FETCH_TARGET = 'https://192.0.2.19:28444/v1/search'
SEARCH_HTML = b'<title>Items | eBay</title><div class="s-card"><a href="/itm/123456789012">Kit</a></div>'


class FetchResponse:
    def __init__(self, status=200, url=FETCH_TARGET):
        self.status_code = status
        self.url = url
        self.content = b''
        self.primary_ip = '192.0.2.19'
        self.infos = {}
        self.closed = False

    @property
    def text(self):
        return self.content.decode('utf-8', errors='replace')

    def close(self):
        self.closed = True


@pytest.fixture
def fetch_factory(reserve_factory):
    def make(payload=SEARCH_HTML, status=200, response_url=FETCH_TARGET,
             chunks=None, transport_error=None, **kwargs):
        from curl_cffi.curl import CURL_WRITEFUNC_ERROR
        response = FetchResponse(status, response_url)
        session = Mock()

        def post(*args, **options):
            if transport_error is not None:
                raise transport_error
            for chunk in ([payload] if chunks is None else chunks):
                if options['content_callback'](chunk) == CURL_WRITEFUNC_ERROR:
                    error = RuntimeError('private transfer aborted ' + FETCH_CONFIG)
                    error.response = response
                    raise error
            return response

        session.post.side_effect = post
        factory = Mock(return_value=session)
        config = {'UK_RESERVE_URL': FETCH_CONNECT, 'UK_RESERVE_FETCH_URL': FETCH_CONFIG}
        config.update(kwargs.pop('environ', {}))
        reserve = reserve_factory(environ=config, session_factory=factory, **kwargs)
        return reserve, factory, session, response
    return make


def test_browser_fetch_uses_direct_private_mtls_and_explicit_auth(fetch_factory, scanner, caplog):
    from curl_cffi.const import CurlInfo, CurlOpt
    reserve, factory, session, response = fetch_factory()
    assert reserve.enabled and reserve.browser_fetch
    with caplog.at_level('INFO'):
        assert reserve._request_once({}, scanner._classify_private_reserve_response) == ('success', SEARCH_HTML.decode())
    options = factory.call_args.kwargs
    assert options['verify'] == reserve._ca_path
    assert options['cert'] == reserve._client_cert
    assert options['trust_env'] is False and options['allow_redirects'] is False
    assert options['timeout'] == (5.0, 30.0)
    assert 'proxy' not in options and 'proxies' not in options
    assert options['curl_options'] == {CurlOpt.PROXY: ''}
    assert options['curl_infos'] == [CurlInfo.HTTP_CONNECTCODE]
    post = session.post.call_args
    assert post.args == (FETCH_TARGET,)
    assert post.kwargs['json'] == {'url': SEARCH}
    assert post.kwargs['auth'] == ('offline-user', 'test-password-' * 4)
    assert post.kwargs['allow_redirects'] is False and post.kwargs['timeout'] == (5.0, 30.0)
    session.get.assert_not_called()
    session.close.assert_called_once()
    assert response.closed
    assert 'source=VPS browser_fetch=true' in caplog.text
    assert 'peer_matches_config=true' in caplog.text
    assert all(secret not in caplog.text for secret in (FETCH_CONFIG, FETCH_TARGET, SEARCH, '192.0.2.19', 'test-password-'))


@pytest.mark.parametrize('payload,status,expected', [
    (b'<title>Home | eBay</title>', 200, 'http_error'),
    (b'<title>Pardon Our Interruption</title>', 200, 'blocked'),
    (b'', 403, 'blocked'), (b'', 429, 'rate_limited'),
    (b'', 413, 'http_error'), (b'', 503, 'http_error'),
    (b'', 401, 'proxy_auth'), (b'', 407, 'proxy_auth'), (b'', 302, 'http_error'),
])
def test_browser_fetch_rejects_failures_without_connect_fallback(fetch_factory, scanner, payload, status, expected):
    reserve, _factory, session, response = fetch_factory(payload, status)
    assert reserve._request_once({}, scanner._classify_private_reserve_response) == (expected, None)
    session.post.assert_called_once()
    session.get.assert_not_called()
    session.close.assert_called_once()
    assert response.closed


@pytest.mark.parametrize('status,seconds', [(403, 900), (429, 1800), (401, 3600), (413, 120), (503, 120)])
def test_browser_fetch_failure_retains_existing_cooldown(fetch_factory, scanner, status, seconds):
    reserve, _factory, _session, _response = fetch_factory(b'', status, clock=lambda: 100.0)
    assert reserve._run_reserved({}, scanner._classify_private_reserve_response)[1:] == (None, None)
    assert reserve.cooldown_until == 100.0 + seconds
    assert not reserve.in_flight


@pytest.mark.parametrize('error,expected', [
    (RuntimeError('certificate failed ' + FETCH_CONFIG), 'proxy_ssl'),
    (RuntimeError('CONNECT 407 ' + FETCH_CONFIG), 'proxy_auth'),
    (RuntimeError('curl: (28) timed out ' + FETCH_CONFIG), 'proxy_timeout'),
])
def test_browser_fetch_transport_errors_stay_private(fetch_factory, scanner, caplog, error, expected):
    reserve, _factory, session, _response = fetch_factory(transport_error=error)
    with caplog.at_level('INFO'):
        assert reserve._request_once({}, scanner._classify_private_reserve_response) == (expected, None)
    session.get.assert_not_called()
    session.close.assert_called_once()
    assert FETCH_CONFIG not in caplog.text and 'test-password-' not in caplog.text


@pytest.mark.parametrize('url', [
    FETCH_CONFIG.replace('https:', 'http:'),
    FETCH_CONFIG.replace('192.0.2.19', '192.0.2.20'),
    FETCH_CONFIG.replace('192.0.2.19', 'reserve.example'),
    FETCH_CONFIG.replace(':28444', ''),
    FETCH_CONFIG.replace(':28444', ':70000'),
    FETCH_CONFIG.replace('/v1/search', '/v1/other'),
    FETCH_CONFIG + '?url=private', FETCH_CONFIG + '#fragment',
    FETCH_CONFIG.replace('offline-user', 'different-user'),
    FETCH_CONFIG.replace('test-password-', 'other-password-'),
    FETCH_TARGET, FETCH_CONFIG + '\r\nX-Injected: private',
    FETCH_CONFIG + '\x00', ' ' + FETCH_CONFIG,
])
def test_browser_fetch_unsafe_config_fails_closed(reserve_factory, url, caplog):
    factory = Mock()
    reserve = reserve_factory(environ={'UK_RESERVE_URL': FETCH_CONNECT, 'UK_RESERVE_FETCH_URL': url},
                              session_factory=factory)
    assert reserve.configured and not reserve.enabled and reserve._directory is None
    assert reserve._request_once({}, Mock()) == ('http_error', None)
    factory.assert_not_called()
    assert url not in caplog.text


def test_browser_fetch_invalid_certificate_material_disables_optional_transport():
    config = {'UK_RESERVE_URL': FETCH_CONNECT, 'UK_RESERVE_FETCH_URL': FETCH_CONFIG,
              'UK_RESERVE_CA_PEM': 'invalid', 'UK_RESERVE_CLIENT_CERT_PEM': 'invalid',
              'UK_RESERVE_CLIENT_KEY_PEM': 'invalid'}
    reserve = PrivateReserve(config, SEARCH)
    assert not reserve.enabled and reserve._directory is None


def test_browser_fetch_checks_search_url_again_before_post(fetch_factory):
    reserve, factory, _session, _response = fetch_factory()
    reserve._search_url = 'https://external.example/private'
    assert reserve._request_once({}, Mock()) == ('http_error', None)
    factory.assert_not_called()


def test_browser_fetch_rejects_unexpected_response_url(fetch_factory):
    reserve, _factory, session, response = fetch_factory(response_url='https://external.example/private')
    classify = Mock(return_value='success')
    assert reserve._request_once({}, classify) == ('http_error', None)
    classify.assert_not_called()
    session.close.assert_called_once()
    assert response.closed


def test_browser_fetch_accepts_exactly_four_mib(fetch_factory, scanner):
    payload = SEARCH_HTML + b' ' * (MAX_FETCH_BYTES - len(SEARCH_HTML))
    reserve, _factory, session, response = fetch_factory(payload)
    result, body = reserve._request_once({}, scanner._classify_private_reserve_response)
    assert result == 'success' and len(body.encode()) == MAX_FETCH_BYTES
    assert response.closed
    session.close.assert_called_once()


@pytest.mark.parametrize('chunks', [
    [b'x' * (MAX_FETCH_BYTES + 1)],
    [SEARCH_HTML, b' ' * (MAX_FETCH_BYTES - len(SEARCH_HTML)), b'!'],
])
def test_browser_fetch_aborts_oversize_before_classification(fetch_factory, chunks, caplog):
    reserve, _factory, session, response = fetch_factory(chunks=chunks)
    classify = Mock(return_value='success')
    with caplog.at_level('INFO'):
        assert reserve._request_once({}, classify) == ('http_error', None)
    classify.assert_not_called()
    session.get.assert_not_called()
    session.close.assert_called_once()
    assert response.closed
    assert FETCH_CONFIG not in caplog.text and 'private transfer aborted' not in caplog.text


def test_browser_fetch_reuses_existing_admission_budget(fetch_factory, scanner):
    clock = [100.0]
    reserve, _factory, session, _response = fetch_factory(clock=lambda: clock[0])
    executor, limiter = PendingExecutor(), ProbeLimiter(maximum=1)
    reserve.note_failure()
    clock[0] = 124.99
    assert reserve.submit(executor, limiter, {}, scanner._classify_private_reserve_response) is None
    for now in (125.0, 170.0, 215.0):
        clock[0] = now
        future = reserve.submit(executor, limiter, {}, scanner._classify_private_reserve_response)
        assert future is not None
        executor.run()
        assert future.result()[0] == 'success'
        assert not reserve.in_flight and limiter.count() == 0
        clock[0] += 44.99
        assert reserve.submit(executor, limiter, {}, scanner._classify_private_reserve_response) is None
    assert session.post.call_count == 3 and reserve.cooldown_until == 515.0
    clock[0] = 514.99
    assert not reserve.eligible()
    clock[0] = 515.0
    assert reserve.eligible()


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
