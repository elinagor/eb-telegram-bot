"""UK-only, one-shot emergency transport, isolated from shared proxy discovery.

Configuration is private environment state. Neither exception text nor connection
details leave this module. PEM files are private, temporary, and never enter Git.
"""
import atexit
import os
import ssl
import tempfile
import threading
import time
from collections import deque
from urllib.parse import unquote, urlsplit


RESERVE_ID = 'uk-private-reserve'


def normal_check_delay_bounds(raw_interval='30'):
    """Keep successful/parse-only checks inside the requested 30--45 second band."""
    try:
        lower = min(45, max(30, int(raw_interval)))
    except (TypeError, ValueError, OverflowError):
        lower = 30
    return lower, 45


def _pem(value):
    # Render accepts real multiline values; literal escaped newlines are supported
    # without accepting arbitrary Python/string escapes.
    return str(value or '').replace('\\n', '\n').strip() + '\n'


def _safe_search_url(value):
    try:
        parts = urlsplit(value)
        return (parts.scheme == 'https' and parts.hostname == 'www.ebay.co.uk'
                and parts.port in (None, 443) and not parts.username
                and not parts.password and not parts.fragment
                and parts.path.startswith('/sch/')
                and not any(c in value for c in '\r\n\x00'))
    except (TypeError, ValueError):
        return False


def _safe_endpoint(value):
    try:
        parts = urlsplit(value)
        username, password = unquote(parts.username or ''), unquote(parts.password or '')
        return (parts.scheme == 'https' and bool(parts.hostname)
                and parts.port is not None and 1 <= parts.port <= 65535
                and parts.path in ('', '/') and not parts.query and not parts.fragment
                and 1 <= len(username) <= 64 and 32 <= len(password) <= 256
                and not any(c in username + password + value for c in '\r\n\x00'))
    except (TypeError, ValueError):
        return False


def _transport_result(exc):
    # The message is inspected locally only. Never log, return, or chain it.
    low = str(exc).lower()
    if 'curl: (60)' in low or 'certificate' in low or 'ssl' in low or 'tls' in low:
        return 'proxy_ssl'
    if '407' in low or 'proxy authentication' in low:
        return 'proxy_auth'
    if 'curl: (28)' in low or 'timed out' in low or 'timeout' in low:
        return 'proxy_timeout'
    return 'proxy_error'


class PrivateReserve:
    """A bounded emergency request, never a persistent or pool-managed session."""
    failure_after = 25.0
    minimum_spacing = 45.0
    window_seconds = 300.0
    window_attempts = 3
    mandatory_rest = 300.0

    def __init__(self, environ=None, search_url='', clock=time.monotonic,
                 session_factory=None):
        self.clock = clock
        self.lock = threading.Lock()
        self.failure_since = None
        self.last_attempt = None
        self.attempts = deque()
        self.cooldown_until = 0.0
        self.in_flight = False
        self.enabled = False
        self.configured = False
        self._directory = None
        self._endpoint = ''
        self._search_url = ''
        self._session_factory = session_factory
        self._curl_options = {}
        env = os.environ if environ is None else environ
        values = [env.get(key, '') for key in (
            'UK_RESERVE_URL', 'UK_RESERVE_CA_PEM',
            'UK_RESERVE_CLIENT_CERT_PEM', 'UK_RESERVE_CLIENT_KEY_PEM')]
        self.configured = any(values)
        if not all(values) or not _safe_endpoint(values[0]) or not _safe_search_url(search_url):
            return
        try:
            from curl_cffi.const import CurlOpt
            directory = tempfile.TemporaryDirectory(prefix='uk-reserve-')
            self._directory = directory
            os.chmod(directory.name, 0o700)
            paths = []
            for name, content in zip(('ca.pem', 'client.pem', 'client-key.pem'), values[1:]):
                path = os.path.join(directory.name, name)
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'w', encoding='ascii', newline='\n') as target:
                    target.write(_pem(content))
                os.chmod(path, 0o600)
                paths.append(path)
            # Validate CA syntax and the client certificate/key pairing before
            # enabling any request. Handshake checks certificate validity as well.
            validation = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            validation.load_verify_locations(cafile=paths[0])
            validation.load_cert_chain(certfile=paths[1], keyfile=paths[2])
            self._curl_options = {
                CurlOpt.PROXY_CAINFO: paths[0],
                CurlOpt.PROXY_SSLCERT: paths[1],
                CurlOpt.PROXY_SSLKEY: paths[2],
                CurlOpt.PROXY_SSL_VERIFYPEER: 1,
                CurlOpt.PROXY_SSL_VERIFYHOST: 2,
                CurlOpt.PROXY_SSLCERTTYPE: 'PEM',
                CurlOpt.PROXY_SSLKEYTYPE: 'PEM',
            }
            self._endpoint, self._search_url = values[0], search_url
            self.enabled = True
            atexit.register(self.close)
        except Exception:
            # Invalid/incomplete material disables only this optional reserve.
            self.close()

    def close(self):
        self.enabled = False
        directory, self._directory = self._directory, None
        if directory is not None:
            try:
                directory.cleanup()
            except Exception:
                # Optional transport initialization must never stop the scanner.
                pass

    def note_failure(self, started=None):
        now = self.clock()
        with self.lock:
            if self.failure_since is None:
                self.failure_since = now if started is None else min(now, started)

    def note_success(self):
        with self.lock:
            self.failure_since = None

    def _eligible_locked(self, now):
        while self.attempts and self.attempts[0] <= now - self.window_seconds:
            self.attempts.popleft()
        return (self.enabled and self.failure_since is not None
                and now - self.failure_since >= self.failure_after
                and not self.in_flight and now >= self.cooldown_until
                and (self.last_attempt is None or now - self.last_attempt >= self.minimum_spacing)
                and len(self.attempts) < self.window_attempts)

    def eligible(self):
        with self.lock:
            return self._eligible_locked(self.clock())

    def submit(self, executor, limiter, profile, classify):
        """Reserve one existing global probe slot; failed submission costs nothing."""
        with self.lock:
            now = self.clock()
            if not self._eligible_locked(now):
                return None
            self.in_flight = True
            try:
                future = limiter.submit(executor, self._run_reserved, profile, classify,
                                        identity=RESERVE_ID)
            except Exception:
                self.in_flight = False
                return None
            if future is None:
                self.in_flight = False
                return None
            self.last_attempt = now
            self.attempts.append(now)
            if len(self.attempts) >= self.window_attempts:
                self.cooldown_until = max(self.cooldown_until, now + self.mandatory_rest)
        # Register outside our lock: already-completed Futures invoke callbacks inline.
        future.add_done_callback(self._cancelled)
        return future

    def _cancelled(self, future):
        if future.cancelled():
            with self.lock:
                self.in_flight = False

    def _run_reserved(self, profile, classify):
        result, body = 'proxy_error', None
        try:
            result, body = self._request_once(profile, classify)
            return result, body, None
        finally:
            duration = {'blocked': 900, 'rate_limited': 1800,
                        'proxy_ssl': 3600, 'proxy_auth': 3600,
                        'proxy_error': 90, 'proxy_timeout': 90,
                        'http_error': 120}.get(result, 0)
            with self.lock:
                if duration:
                    self.cooldown_until = max(self.cooldown_until, self.clock() + duration)
                self.in_flight = False

    def _request_once(self, profile, classify):
        session = response = None
        try:
            factory = self._session_factory
            if factory is None:
                from curl_cffi import requests as cffi_requests
                factory = cffi_requests.Session
            session = factory(
                proxy=self._endpoint, headers={'Accept-Language': 'en-GB,en;q=0.9'},
                impersonate=profile['impersonate'], verify=True, trust_env=False,
                allow_redirects=False, timeout=(5.0, 12.0),
                curl_options=dict(self._curl_options),
            )
            response = session.get(self._search_url, timeout=(5.0, 12.0),
                                   allow_redirects=False)
            if getattr(response, 'url', '') and not _safe_search_url(str(response.url)):
                return 'http_error', None
            body = response.text or ''
            result = classify(response, body)
            if result not in ('success', 'blocked', 'rate_limited', 'proxy_auth', 'http_error'):
                result = 'http_error'
            return result, body if result == 'success' else None
        except Exception as exc:
            return _transport_result(exc), None
        finally:
            for obj in (response, session):
                if obj is not None:
                    try:
                        obj.close()
                    except Exception:
                        pass
