"""Offline regressions for the public share across one-slot rolling refills."""
import time

import pytest

from test_coordinator_uk import scanner, forbid_unmocked_network


def public_pools(scanner, primary_count=32, independent_count=32):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:80' % i for i in range(1, primary_count + 1)]
    independent = ['http://1.1.1.%d:80' % i for i in range(1, independent_count + 1)]
    manager.proxies = independent + primary
    manager.all_proxies = list(manager.proxies)
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 120
    return manager, primary, independent


@pytest.mark.parametrize('fraction, expected', [(0.5, 10), (0.75, 15)])
def test_one_slot_refills_honor_public_share(scanner, monkeypatch, fraction, expected):
    manager, primary, independent = public_pools(scanner)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: fraction)
    selected, tried = [], set()
    for _ in range(20):
        batch = manager.get_candidate_batch(1, tried_hosts=tried, allow_premium=False)
        assert len(batch) == 1
        selected.extend(batch)
        tried.update(scanner._proxy_host(p) for p in batch)
        assert 0 <= manager.coordinator_rolling_credit < 1
    assert selected[0] in primary
    assert len(set(selected)) == 20
    assert sum(p in primary for p in selected) == expected
    assert sum(p in independent for p in selected) == 20 - expected


@pytest.mark.parametrize('missing', ['primary', 'independent'])
def test_one_source_exhaustion_never_delays_fallback_or_builds_debt(scanner, monkeypatch, missing):
    manager, primary, independent = public_pools(scanner)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: .5)
    unavailable = primary if missing == 'primary' else independent
    available = independent if missing == 'primary' else primary
    tried = {scanner._proxy_host(p) for p in unavailable}
    for _ in range(8):
        batch = manager.get_candidate_batch(1, tried_hosts=tried, allow_premium=False)
        assert len(batch) == 1 and batch[0] in available
        tried.add(scanner._proxy_host(batch[0]))
        assert manager.coordinator_rolling_credit == .5
    # Once both sources are available again, the normal bounded phase resumes.
    first = manager.get_candidate_batch(1, allow_premium=False)
    second = manager.get_candidate_batch(1, tried_hosts={scanner._proxy_host(first[0])}, allow_premium=False)
    assert first[0] in primary and second[0] in independent


def test_one_slot_respects_quarantine_tried_hosts_and_duplicate_ports(scanner, monkeypatch):
    manager, primary, independent = public_pools(scanner, 4, 4)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: .5)
    twin = independent[0].replace(':80', ':8080')
    manager.proxies.append(twin)
    manager.all_proxies.append(twin)
    manager.host_bad_until[scanner._proxy_host(primary[0])] = time.time() + 300
    manager.bad_until[independent[1]] = time.time() + 300
    tried = {scanner._proxy_host(primary[1])}
    selected = []
    for _ in range(5):
        batch = manager.get_candidate_batch(1, tried_hosts=tried, allow_premium=False)
        assert batch
        selected.extend(batch)
        tried.update(scanner._proxy_host(p) for p in batch)
    hosts = [scanner._proxy_host(p) for p in selected]
    assert len(hosts) == len(set(hosts))
    assert primary[0] not in selected and primary[1] not in selected
    assert independent[1] not in selected


def test_recent_ebay_success_keeps_priority_without_consuming_public_credit(scanner, monkeypatch):
    manager, primary, independent = public_pools(scanner)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: .75)
    winner = independent[-1]
    manager.last_success_at[winner] = time.time()
    assert manager.get_candidate_batch(1, allow_premium=False) == [winner]
    assert manager.coordinator_rolling_credit == .5


def test_local_negative_signals_do_not_become_fairness_candidates(scanner, monkeypatch):
    manager, primary, independent = public_pools(scanner, 4, 4)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: .5)
    for p in independent:
        manager.quality_bad_until[p] = time.time() + 30
    for _ in range(4):
        assert manager.get_candidate_batch(1, allow_premium=False)[0] in primary
        assert manager.coordinator_rolling_credit == .5


def test_changed_share_stays_bounded_and_does_not_starve_either_source(scanner, monkeypatch):
    manager, primary, independent = public_pools(scanner)
    selected, tried = [], set()
    for fraction in ([.75] * 8 + [.5] * 8):
        monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda f=fraction: f)
        batch = manager.get_candidate_batch(1, tried_hosts=tried, allow_premium=False)
        selected.extend(batch)
        tried.update(scanner._proxy_host(p) for p in batch)
        assert 0 <= manager.coordinator_rolling_credit < 1
    assert sum(p in primary for p in selected[:8]) == 6
    assert sum(p in primary for p in selected[8:]) == 4


def test_expired_coordinator_has_no_stale_priority(scanner, monkeypatch):
    manager, primary, independent = public_pools(scanner)
    monkeypatch.setattr(scanner.pipeline_metrics, 'coordinator_fraction', lambda: .75)
    manager.coordinator_valid_until = time.time() - 1
    assert manager.get_candidate_batch(1, allow_premium=False)
    assert manager.coordinator_rolling_credit == .5
