"""Offline discovery scheduling regressions; no network or real worker threads."""
from concurrent.futures import Future
import time
from unittest.mock import Mock

import pytest

from proxy_runtime import ProbeLimiter
from test_coordinator_uk import scanner, forbid_unmocked_network


@pytest.mark.parametrize('escalated, previous_wave, normal_exhausted',
                         [(False, False, False), (True, False, False),
                          (False, True, False), (False, False, True)])
def test_rolling_external_probes_do_not_starve_coordinator_and_legacy(
    scanner, monkeypatch, escalated, previous_wave, normal_exhausted
):
    """One fast normal completion must not create another full external allowance.

    External futures stay pending while ordinary futures finish one by one. This
    reproduces the actual rolling schedule: per-batch limits silently turn into an
    all-external wave unless already-running external futures count toward the cap.
    An unfinished earlier wave must still count against the shared global limiter.
    """
    manager = scanner.ProxyManager()
    normal = [f'http://{11 if i % 2 else 12}.0.0.{i}:80' for i in range(1, 60)]
    primary = frozenset(p for p in normal if p.startswith('http://11.'))
    manager.coordinator_current = primary
    manager.coordinator_valid_until = time.time() + 100
    monkeypatch.setattr(manager, 'standard_pool_loaded', lambda: True)
    monkeypatch.setattr(manager, 'pool_stats', lambda: (500, 500, 0, 0))
    monkeypatch.setattr(manager, 'emergency_needed', lambda: False)
    monkeypatch.setattr(manager, 'get_due_reprobe_candidate', lambda **kw: None)
    monkeypatch.setattr(manager, 'get_webshare_half_open_candidate', lambda **kw: None)
    monkeypatch.setattr(manager, 'get_premium_half_open_candidate', lambda **kw: None)

    normal_selections = []

    def normal_batch(count, tried_hosts=None, **kwargs):
        if normal_exhausted:
            return []
        chosen = [p for p in normal if scanner._proxy_host(p) not in (tried_hosts or ())][:count]
        normal_selections.extend(chosen)
        return chosen

    external_index = [0]
    credits = {}

    def external_batch(max_total=1, **kwargs):
        chosen = []
        for _ in range(min(2, max_total)):
            external_index[0] += 1
            chosen.append((f'http://13.0.0.{external_index[0]}:80', 'proxio'))
        return chosen

    monkeypatch.setattr(manager, 'get_candidate_batch', normal_batch)
    monkeypatch.setattr(manager, 'get_external_free_candidates', external_batch)
    monkeypatch.setattr(scanner, 'proxy_manager', manager)
    monkeypatch.setattr(scanner.external_free_manager, 'credit_source_fast', credits.get)
    monkeypatch.setattr(scanner.external_free_manager, 'claim_credit', lambda p, src: credits.__setitem__(p, src))
    monkeypatch.setattr(scanner.external_free_manager, 'record_winner', lambda p: None)
    monkeypatch.setattr(scanner.provider_manager, 'record_winner', lambda *a: None)
    monkeypatch.setattr(scanner.provider_manager, 'has_usable_webshare', lambda: False)
    monkeypatch.setattr(scanner, 'WEBSHARE_FIRST_BATCH', False)
    monkeypatch.setattr(scanner, 'PROBE_ESCALATE_AFTER', 0 if escalated else 999)
    monkeypatch.setattr(scanner, 'PROBE_CONCURRENCY', 4)
    monkeypatch.setattr(scanner, 'PROBE_ESCALATED_CONCURRENCY', 4)
    monkeypatch.setattr(scanner, '_connection_outage_snapshot', lambda: (0, False, True))
    monkeypatch.setattr(scanner, '_memory_rss_mb', lambda: 100)
    monkeypatch.setattr(scanner, '_memory_maintenance', lambda *a, **kw: None)
    monkeypatch.setattr(scanner, '_adopt_webshare_handoff_if_ready', lambda: None)
    monkeypatch.setattr(scanner, '_consume_restart_sticky_candidate', lambda: None)
    monkeypatch.setattr(scanner, '_queue_restart_sticky_persist', lambda *a, **kw: None)
    monkeypatch.setattr(scanner, 'get_preferred_profile', lambda: {'name': 'offline'})
    monkeypatch.setattr(scanner, 'record_ebay_success', lambda: None)
    monkeypatch.setattr(scanner, 'wake_queued_auctions_for_new_fixed', lambda: None)
    monkeypatch.setattr(scanner, 'fixed_proxy', None)
    monkeypatch.setattr(scanner, 'fixed_profile', None)
    monkeypatch.setattr(scanner, 'fixed_session', None)
    monkeypatch.setattr(scanner, '_discovery_replacement_pause', lambda results: 0)

    futures = {}

    class PendingExecutor:
        def __init__(self, **kwargs):
            pass

        def submit(self, fn, proxy, *args):
            future = Future()
            futures[future] = proxy
            return future

        def shutdown(self, **kwargs):
            pass

    monkeypatch.setattr(scanner, 'ThreadPoolExecutor', PendingExecutor)
    limiter = ProbeLimiter(maximum=12)
    monkeypatch.setattr(scanner, 'probe_limiter', limiter)
    earlier = None
    if previous_wave:
        earlier = limiter.submit(PendingExecutor(), None, 'http://14.0.0.1:80', identity='14.0.0.1')

    snapshots = []

    def finish_one(active, **kwargs):
        current = [futures[f] for f in active]
        snapshots.append(current)
        assert limiter.count() <= 4
        # Keep external work outstanding to expose unbounded replacement growth.
        regular = [f for f in active if not futures[f].startswith('http://13.')]
        target = regular[0] if regular else active[0]
        success = len(snapshots) >= 8 or not regular
        target.set_result(('success', 'offline successful UK response', Mock())
                          if success else ('proxy_timeout', None, None))
        return {target}, set(active) - {target}

    monkeypatch.setattr(scanner, 'wait', finish_one)
    try:
        assert scanner.fetch_ebay_html_with_fixed_pair() == 'offline successful UK response'
        if normal_exhausted:
            # Borrow otherwise idle slots; a source-share correction must not reduce
            # throughput when only independent external feeds have candidates left.
            assert len(snapshots[0]) == 4
            assert all(p.startswith('http://13.') for p in snapshots[0])
            assert normal_selections == []
        else:
            external_cap = 2 if escalated else 1
            assert max(sum(p.startswith('http://13.') for p in rows) for rows in snapshots) <= external_cap
            assert len(normal_selections) >= 8
            assert any(p in primary for p in normal_selections)
            assert any(p not in primary for p in normal_selections)
        assert limiter.count() == int(previous_wave)
    finally:
        if earlier is not None:
            earlier.cancel()
        scanner.main_discovery_active_event.clear()

