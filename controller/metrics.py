"""Prometheus metrics (#11). Every series that matters after a restart is rebuilt from the cluster:
last success/failure from Backup records, owned snapshots from VolumeSnapshot events."""
from __future__ import annotations

import threading

from prometheus_client import Counter, Gauge, Histogram

RUNS = Counter("backup_operator_runs_total", "Finished backup runs by final result.",
               ["namespace", "source", "result"])
ATTEMPT_FAILURES = Counter("backup_operator_attempt_failures_total",
                           "Failed attempts (including ones that were retried) by reason.",
                           ["namespace", "source", "reason"])
DURATION = Histogram("backup_operator_run_duration_seconds", "Run duration from admission to recorded result.",
                     ["namespace", "source"], buckets=(60, 300, 900, 1800, 3600, 7200, 14400, 28800))
LAST_SUCCESS = Gauge("backup_operator_last_success_timestamp_seconds",
                     "Completion time of the newest successful backup per source.", ["namespace", "source"])
LAST_FAILURE = Gauge("backup_operator_last_failure_timestamp_seconds",
                     "Completion time of the newest failed run per source.", ["namespace", "source"])
ACTIVE_RUNS = Gauge("backup_operator_active_runs", "Admitted runs that hold a concurrency slot.")
OWNED_SNAPSHOTS = Gauge("backup_operator_owned_snapshots", "VolumeSnapshots created by the operator that still exist.",
                        ["namespace"])
CLEANUP_STUCK = Gauge("backup_operator_cleanup_stuck_requests", "Requests whose cleanup exceeded its timeout.")
SCHEDULE_INTERVAL = Gauge("backup_operator_schedule_interval_seconds",
                          "Nominal interval of each BackupSchedule (for staleness alerts).", ["namespace", "source"])

SCHEDULE_READY = Gauge("backup_operator_schedule_ready",
                       "1 when the schedule's source volume, credentials and repository settings are valid.",
                       ["namespace", "source"])

_lock = threading.Lock()
_latest: dict[tuple[str, str, str], float] = {}
_snapshots: dict[str, set[str]] = {}
_stuck: set[str] = set()


def observe_result(namespace: str, source: str, phase: str, completed_ts: float) -> None:
    """Track the newest Succeeded/Failed completion per source (idempotent, order-independent)."""
    gauge = LAST_SUCCESS if phase == "Succeeded" else LAST_FAILURE if phase == "Failed" else None
    if gauge is None:
        return
    key = (namespace, source, phase)
    with _lock:
        if completed_ts > _latest.get(key, 0):
            _latest[key] = completed_ts
            gauge.labels(namespace=namespace, source=source).set(completed_ts)


def snapshot_event(namespace: str, name: str, exists: bool) -> None:
    with _lock:
        names = _snapshots.setdefault(namespace, set())
        (names.add if exists else names.discard)(name)
        OWNED_SNAPSHOTS.labels(namespace=namespace).set(len(names))


def cleanup_stuck(key: str, stuck: bool) -> None:
    with _lock:
        (_stuck.add if stuck else _stuck.discard)(key)
        CLEANUP_STUCK.set(len(_stuck))
