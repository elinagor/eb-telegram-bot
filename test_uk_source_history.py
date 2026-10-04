"""Regression for low-volume source evidence during a timeout-heavy outage."""
from proxy_runtime import PipelineMetrics


def test_busy_legacy_cannot_erase_recent_coordinator_failures():
    now = [1000.0]
    metrics = PipelineMetrics(clock=lambda: now[0])
    for _ in range(12):
        metrics.record_request('primary', 'coordinator', 'blocked', 1, 'discovery')
    for _ in range(520):
        metrics.record_request('fallback', 'legacy', 'proxy_timeout', 3.5, 'discovery')
    assert metrics.coordinator_fraction() == .5
    now[0] += 901
    assert metrics.coordinator_fraction() == .75


def test_fixed_requests_do_not_replace_discovery_evidence():
    metrics = PipelineMetrics(clock=lambda: 1000)
    for _ in range(12):
        metrics.record_request('primary', 'coordinator', 'blocked', 1, 'discovery')
        metrics.record_request('fallback', 'legacy', 'success', 2, 'discovery')
    for _ in range(520):
        metrics.record_request('fixed', 'legacy', 'success', 2, 'fixed')
    assert metrics.coordinator_fraction() == .5


def test_history_stays_bounded_and_success_can_restore_priority():
    metrics = PipelineMetrics(clock=lambda: 1000)
    for i in range(600):
        for source in ('coordinator', 'legacy'):
            metrics.record_request(str(i), source, 'success', 2, 'discovery')
    assert metrics.coordinator_fraction() == .75
    assert all(len(rows) <= 128 for rows in metrics.discovery_by_source.values())
    assert len(metrics.requests) == 512
