"""Optional, bounded UK proxy feed. No background threads or target-site requests."""

import ipaddress
import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime
from urllib.parse import urlsplit
from proxy_runtime import finite_number

import requests


class CoordinatorFeed:
    RESULT_MAP = {
        'success': 'success', 'blocked': 'blocked', 'rate_limited': 'blocked',
        'proxy_timeout': 'timeout', 'proxy_rejected': 'rejected',
        'proxy_ssl': 'ssl', 'proxy_error': 'error',
    }

    def __init__(self, base_url='', token='', limit=500, refresh_seconds=60,
                 stale_seconds=180, clock=time.time, transport=requests):
        self.base_url = base_url.strip().rstrip('/')
        self.token = token.strip()
        self.limit = max(1, min(int(limit), 500))
        self.refresh_seconds = max(30, int(refresh_seconds))
        self.stale_seconds = max(60, min(int(stale_seconds), 240))
        self.clock = clock
        self.transport = transport
        self.state_lock = threading.Lock()
        self.refresh_lock = threading.Lock()
        self.rows = ()
        self.valid_until = 0.0
        self.next_refresh = 0.0
        self.failures = 0
        self.ids = OrderedDict()
        self.metadata = {}
        self.pending = OrderedDict()
        self.last_feedback = 0.0
        self.accepted_feedback = 0
        try:
            parts = urlsplit(self.base_url)
            self.enabled = bool(
                self.token and parts.scheme == 'https' and parts.hostname
                and not parts.username and not parts.password
                and not parts.query and not parts.fragment and parts.path in ('', '/')
            )
        except ValueError:
            self.enabled = False
        if self.token and self.base_url and not self.enabled:
            logging.warning('Coordinator disabled: expected HTTPS origin without credentials/path')

    @staticmethod
    def public_uri(raw):
        try:
            if not isinstance(raw, str) or len(raw) > 200:
                return None
            p = urlsplit(raw.strip())
            if (p.scheme not in ('http', 'https', 'socks5') or not p.hostname or not p.port
                    or p.username is not None or p.password is not None
                    or p.path not in ('', '/') or p.query or p.fragment):
                return None
            address = ipaddress.ip_address(p.hostname)
            if not address.is_global:
                return None
            host = '[%s]' % address if address.version == 6 else str(address)
            return '%s://%s:%d' % (p.scheme, host, p.port)
        except (ValueError, TypeError):
            return None

    def snapshot(self):
        with self.state_lock:
            if self.clock() >= self.valid_until:
                return (), 0.0
            return self.rows, self.valid_until

    def metadata_snapshot(self):
        with self.state_lock:
            if self.clock() >= self.valid_until:
                return {}
            return {uri: dict(value) for uri, value in self.metadata.items()}

    def was_seen(self, proxy):
        with self.state_lock:
            return proxy in self.ids

    @staticmethod
    def _read_json(response, max_bytes, deadline):
        body = bytearray()
        for chunk in response.iter_content(chunk_size=8192):
            if time.monotonic() > deadline:
                raise TimeoutError('feed deadline')
            if chunk:
                body.extend(chunk)
                if len(body) > max_bytes:
                    raise ValueError('feed too large')
        return json.loads(body.decode('utf-8'))

    def refresh(self):
        if not self.enabled:
            return (), 0.0
        now = self.clock()
        with self.state_lock:
            due = now >= self.next_refresh
        # Never make a main discovery thread wait for a background metadata request.
        if not due or not self.refresh_lock.acquire(blocking=False):
            return self.snapshot()
        response = None
        try:
            deadline = time.monotonic() + 4.0
            response = self.transport.get(
                self.base_url + '/api/v1/proxies',
                params={'market': 'uk', 'limit': self.limit},
                headers={'Authorization': 'Bearer ' + self.token},
                timeout=(1.5, 2.5), stream=True, allow_redirects=False,
            )
            if response.status_code != 200:
                raise RuntimeError('HTTP %d' % response.status_code)
            data = self._read_json(response, 1024 * 1024, deadline)
            if not isinstance(data, dict) or data.get('market') != 'uk':
                raise ValueError('wrong market')
            generated = datetime.fromisoformat(str(data['generated_at']).replace('Z', '+00:00'))
            if generated.tzinfo is None:
                raise ValueError('missing timezone')
            age = max(0.0, self.clock() - generated.timestamp())
            if age > 180 or generated.timestamp() > self.clock() + 60:
                raise ValueError('stale snapshot')
            source_rows = data.get('proxies')
            if not isinstance(source_rows, list) or len(source_rows) > 500:
                raise ValueError('invalid rows')
            rows = []
            metadata = {}
            seen = set()
            for row in source_rows:
                if not isinstance(row, dict):
                    continue
                uri = self.public_uri(row.get('proxy'))
                key = row.get('id')
                if (not uri or uri in seen or row.get('managed') is not False
                        or row.get('health_state') != 'healthy'
                        or not isinstance(key, str) or len(key) != 64
                        or any(c not in '0123456789abcdef' for c in key)):
                    continue
                seen.add(uri)
                rows.append((uri, key, row.get('strong') is True))
                metadata[uri] = {
                    'score': finite_number(row.get('score'), -1000, 1000, 0),
                    'latency_ms': finite_number(row.get('neutral_latency_ms'), 0, 60000),
                    'market_quality': finite_number(row.get('market_quality'), 0, 1),
                    'failure_streak': finite_number(row.get('market_failure_streak'), 0, 100, 0),
                    'strong': row.get('strong') is True,
                    'rank': len(rows) - 1,
                }
                if len(rows) >= self.limit:
                    break
            # Empty genuine snapshots clear priority; malformed nonempty lists are failures.
            if source_rows and not rows:
                raise ValueError('no valid public rows')
            rows.sort(key=lambda row: not row[2])
            now = self.clock()
            with self.state_lock:
                self.rows = tuple(row[0] for row in rows)
                self.metadata = metadata
                self.valid_until = now + max(0.0, self.stale_seconds - age)
                self.next_refresh = now + self.refresh_seconds
                self.failures = 0
                for uri, key, _strong in rows:
                    self.ids[uri] = key
                    self.ids.move_to_end(uri)
                while len(self.ids) > 1000:
                    self.ids.popitem(last=False)
            logging.info('Coordinator UK snapshot: public=%d strong=%d; legacy fallback stays active',
                         len(rows), sum(row[2] for row in rows))
            self.flush_feedback()
        except Exception as error:
            now = self.clock()
            with self.state_lock:
                self.failures += 1
                pause = min(300, 30 * (2 ** min(self.failures - 1, 4)))
                self.next_refresh = now + pause
            # Exception messages can contain a secret URL/header: log only the class.
            status = getattr(response, 'status_code', None)
            logging.warning('Coordinator UK unavailable (%s, HTTP=%s); retry in %ds; using legacy reserve',
                            type(error).__name__, status, pause)
        finally:
            try:
                if response is not None:
                    response.close()
            except Exception:
                pass
            finally:
                self.refresh_lock.release()
        return self.snapshot()

    def record_result(self, proxy, result, latency_ms=None):
        """Queue compact outcomes from EXISTING real requests; absolutely no I/O here."""
        mapped = self.RESULT_MAP.get(result)
        if not self.enabled or not mapped:
            return
        with self.state_lock:
            key = self.ids.get(proxy)
            if key is None:
                return
            previous = self.pending.get(key, ())
            latest = {'proxy_key': key, 'result': mapped}
            latency = finite_number(latency_ms, 0, 120000)
            if latency is not None:
                latest['latency_ms'] = round(latency, 1)
            # Preserve a real success preceding the latest failure in this interval.
            # Repeated successes are coalesced; the final negative result still wins
            # server-side and cannot make a blocked endpoint eligible again.
            if mapped != 'success' and previous and previous[0]['result'] == 'success':
                self.pending[key] = (previous[0], latest)
            else:
                self.pending[key] = (latest,)
            self.pending.move_to_end(key)
            while len(self.pending) > 128:
                self.pending.popitem(last=False)

    def flush_feedback(self):
        now = self.clock()
        with self.state_lock:
            if not self.pending or now - self.last_feedback < 30:
                return
            self.last_feedback = now
            reports = []
            # Keep each endpoint's success/failure pair together and in order.
            while self.pending:
                events = next(iter(self.pending.values()))
                if len(reports) + len(events) > 64:
                    break
                self.pending.popitem(last=False)
                reports.extend(events)
        response = None
        try:
            deadline = time.monotonic() + 3.0
            response = self.transport.post(
                self.base_url + '/api/v1/report',
                headers={'Authorization': 'Bearer ' + self.token},
                json={'market': 'uk', 'reports': reports},
                timeout=(1.0, 2.0), stream=True, allow_redirects=False,
            )
            if response.status_code != 200:
                raise RuntimeError('feedback HTTP')
            data = self._read_json(response, 16384, deadline)
            if not isinstance(data, dict) or data.get('ok') is not True:
                raise ValueError('invalid feedback response')
            accepted = max(0, min(len(reports), int(data.get('accepted', 0))))
            with self.state_lock:
                self.accepted_feedback += accepted
            logging.info('Coordinator UK feedback: sent=%d accepted=%d (existing requests only)',
                         len(reports), accepted)
        except Exception as error:
            # At most once: don't replay a possibly committed report after a lost response.
            logging.warning('Coordinator UK feedback skipped (%s); scanner continues',
                            type(error).__name__)
        finally:
            try:
                if response is not None:
                    response.close()
            except Exception:
                pass
