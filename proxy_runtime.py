"""Bounded in-memory scheduling and measurements. No network, credentials or workers."""
import math
import threading
import time
from collections import Counter, OrderedDict, deque


def finite_number(value, low, high, default=None):
    try:
        value = float(value)
        return max(low, min(value, high)) if math.isfinite(value) else default
    except (TypeError, ValueError, OverflowError):
        return default


def adaptive_timeout(baseline, p95, ready, recovery=False):
    """Shorten only a measured fast endpoint when a replacement reserve exists."""
    if p95 is None or ready < 4:
        return baseline
    connect, read = baseline
    floor = 6.0 if recovery else 10.0
    return (min(connect, max(3.5, p95 + 1.5)),
            min(read, max(floor, p95 * 2.5 + 2.0)))


class ProbeLimiter:
    """One limit across executors, including unfinished probes from previous waves."""
    def __init__(self, maximum=8):
        self.maximum = maximum
        self.lock = threading.Lock()
        self.active = 0
        self.identities = set()

    def submit(self, executor, fn, *args, identity=None):
        with self.lock:
            if self.active >= self.maximum or (identity is not None and identity in self.identities):
                return None
            self.active += 1
            if identity is not None:
                self.identities.add(identity)
        try:
            future = executor.submit(fn, *args)
        except BaseException:
            self._release(identity)
            raise
        future.add_done_callback(lambda _future: self._release(identity))
        return future

    def _release(self, identity):
        with self.lock:
            self.active -= 1
            self.identities.discard(identity)

    def count(self):
        with self.lock:
            return self.active


class SpareSession:
    """Single-owner transfer; never keep response bodies or use one session concurrently."""
    def __init__(self, close, ttl=35, clock=time.monotonic):
        self.close, self.ttl, self.clock = close, ttl, clock
        self.lock = threading.Lock()
        self.slot = None

    def offer(self, proxy, profile, session):
        if session is None:
            return False
        old = None
        with self.lock:
            if self.slot and self.slot[3] > self.clock():
                return False
            old, self.slot = self.slot, (proxy, profile, session, self.clock() + self.ttl)
        if old:
            self.close(old[2])
        return True

    def take(self, eligible):
        with self.lock:
            slot, self.slot = self.slot, None
        if slot is None:
            return None
        if slot[3] <= self.clock() or not eligible(slot[0]):
            self.close(slot[2])
            return None
        return slot[:3]

    def expire(self, force=False):
        with self.lock:
            if self.slot is None or (not force and self.slot[3] > self.clock()):
                return
            old, self.slot = self.slot, None
        self.close(old[2])

    def count(self):
        with self.lock:
            return int(self.slot is not None and self.slot[3] > self.clock())


class PipelineMetrics:
    """Short rolling windows and bounded IP history; no response, URL or credential logs."""
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.lock = threading.Lock()
        self.requests = deque(maxlen=512)
        self.latencies = OrderedDict()
        self.stages = {}
        self.last_success = None
        self.success_gaps = deque(maxlen=128)
        self.outages = deque(maxlen=128)
        self.last_log = 0.0

    def record_request(self, identity, origin, result, seconds, kind):
        seconds = finite_number(seconds, 0, 120, 0)
        with self.lock:
            self.requests.append((self.clock(), origin, result, seconds, kind))
            if result == 'success':
                samples = self.latencies.setdefault(identity, deque(maxlen=12))
                samples.append(seconds)
                self.latencies.move_to_end(identity)
                while len(self.latencies) > 1000:
                    self.latencies.popitem(last=False)

    def successful_latency(self, identity):
        with self.lock:
            samples = list(self.latencies.get(identity, ()))
        if len(samples) < 3:
            return None
        return self.percentile(samples, .95)

    def stage(self, name, seconds):
        with self.lock:
            self.stages.setdefault(name, deque(maxlen=128)).append(max(0, seconds))

    def success(self, outage=None):
        with self.lock:
            now = self.clock()
            if self.last_success is not None:
                self.success_gaps.append(now - self.last_success)
            self.last_success = now
            if outage is not None:
                self.outages.append(max(0, outage))

    @staticmethod
    def percentile(samples, q=.95):
        if not samples:
            return 0.0
        ordered = sorted(samples)
        return ordered[min(len(ordered) - 1, max(0, math.ceil(len(ordered) * q) - 1))]

    def coordinator_fraction(self):
        """Mostly coordinator by default; retain exploration and independent fallback."""
        with self.lock:
            rows = [r for r in self.requests if self.clock() - r[0] < 900
                    and r[1] in ('coordinator', 'legacy') and r[4] == 'discovery']
        groups = {name: [r for r in rows if r[1] == name] for name in ('coordinator', 'legacy')}
        if min(map(len, groups.values())) < 12:
            return .75
        # Fast 403s are not useful throughput. A pseudocount must never promote a
        # repeatedly rejected feed above a group with actual successful responses.
        if not any(r[2] == 'success' for r in groups['coordinator']):
            return .5
        def utility(rows):
            success = sum(r[2] == 'success' for r in rows)
            probability = (success + 2) / (len(rows) + 8)
            cost = max(.4, sum(r[3] for r in rows) / len(rows))
            return probability / cost
        return .5 if utility(groups['coordinator']) < .7 * utility(groups['legacy']) else .75

    def public_acceptance(self):
        with self.lock:
            rows = [r for r in self.requests if self.clock() - r[0] < 900
                    and r[1] in ('coordinator', 'legacy') and r[4] == 'discovery']
        if len(rows) < 12:
            return None
        return sum(r[2] == 'success' for r in rows) / len(rows)

    def snapshot_if_due(self, interval=60):
        with self.lock:
            now = self.clock()
            if now - self.last_log < interval:
                return None
            self.last_log = now
            rows = [r for r in self.requests if now - r[0] < 900]
            counts = Counter((r[1], r[2] == 'success') for r in rows)
            return dict(
                sources={name: dict(attempts=sum(v for (n, _ok), v in counts.items() if n == name),
                                    success=counts[(name, True)])
                         for name in sorted({r[1] for r in rows})},
                gap_p95=round(self.percentile(self.success_gaps), 2),
                outage_p95=round(self.percentile(self.outages), 2),
                stages={name: round(self.percentile(v), 2) for name, v in self.stages.items()},
            )
