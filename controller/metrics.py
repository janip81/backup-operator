"""Prometheus metrics (#11). Every series that matters after a restart is rebuilt from the cluster:
last success/failure from Backup records, owned snapshots from VolumeSnapshot events."""
from __future__ import annotations

import heapq
import threading
import time

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
OWNED_SNAPSHOTS = Gauge("backup_operator_owned_snapshots",
                        "VolumeSnapshots created by the operator that still exist, per source PVC.", ["namespace", "pvc"])
OWNED_SNAPSHOT_OLDEST = Gauge("backup_operator_owned_snapshot_oldest_created_timestamp_seconds",
                              "Creation time of the oldest operator-owned VolumeSnapshot per source PVC.",
                              ["namespace", "pvc"])
SNAPSHOT_DELETING_SINCE = Gauge("backup_operator_snapshot_deleting_since_timestamp_seconds",
                                "Oldest deletion request of a VolumeSnapshot (any owner) that still exists, per PVC.",
                                ["namespace", "pvc"])
ALL_SNAPSHOTS = Gauge("backup_operator_cluster_volume_snapshots",
                      "All VolumeSnapshots per namespace, whoever created them (independent of operator labels).",
                      ["namespace"])
CLEANUP_STUCK = Gauge("backup_operator_cleanup_stuck_requests", "Requests whose cleanup exceeded its timeout.")
SCHEDULE_INTERVAL = Gauge("backup_operator_schedule_interval_seconds",
                          "Nominal interval of each BackupSchedule (for staleness alerts).", ["namespace", "source"])

SCHEDULE_READY = Gauge("backup_operator_schedule_ready",
                       "1 when the schedule's source volume, credentials and repository settings are valid.",
                       ["namespace", "source"])

# Early-warning series for the failure that once overloaded a cluster (runaway snapshots/requests).
SNAPSHOTS_CREATED = Counter("backup_operator_snapshots_created_total",
                            "VolumeSnapshots the operator asked the API to create.", ["namespace", "pvc"])
REQUESTS = Gauge("backup_operator_requests", "BackupRequests that exist, by phase (v0.3 objects count as Legacy).",
                 ["phase"])
OLDEST_UNFINISHED = Gauge("backup_operator_oldest_unfinished_request_created_timestamp_seconds",
                          "Creation time of the oldest v0.4 BackupRequest that hasn't finished (0 when none).")
ORPHAN_CONTENTS = Gauge("backup_operator_orphan_snapshot_contents",
                        "VolumeSnapshotContents older than 15 minutes whose VolumeSnapshot doesn't exist.")
API_CALLS = Counter("backup_operator_api_calls_total", "Kubernetes API calls made by the reconciler.")
API_ERRORS = Counter("backup_operator_api_errors_total", "Failed Kubernetes API calls by HTTP status (or 'network').",
                     ["code"])

INVENTORY_SYNCED = Gauge("backup_operator_inventory_synced_timestamp_seconds",
                         "Last full relist of the snapshot, content and request inventories.")
INVENTORY_FIRST_SYNC = Gauge("backup_operator_inventory_first_sync_timestamp_seconds",
                             "First complete inventory since this process started (growth alerts wait for it).")

ORPHAN_GRACE_SECONDS = 900
TERMINAL_PHASES = {"Succeeded", "Failed", "Legacy"}

_lock = threading.RLock()  # re-entrant: resync applies many events while holding it
_latest: dict[tuple[str, str, str], float] = {}
_records: dict[str, tuple[str, str, str, float]] = {}  # "ns/name" -> (ns, source, phase, completed)
_stuck: set[str] = set()
# Incremental inventories: O(1) work per watch event, so replaying thousands of objects stays cheap.
_snaps: dict[tuple[str, str], dict] = {}            # (ns, name) -> {pvc, owned, created, deleting}
_snap_groups: dict[tuple[str, str], dict] = {}     # (ns, pvc) -> counts and min-heaps (lazy deletion)
_ns_counts: dict[str, int] = {}
_requests: dict[str, tuple[str, float]] = {}       # key -> (phase, created)
_phase_counts: dict[str, int] = {}
_contents: dict[str, tuple[str, float]] = {}       # content name -> ("ns/name" of its snapshot, created)
# Ordering between watch events and a relist: every watch event stamps its object with a sequence
# number (deletes too, as tombstones). A relist skips objects stamped after it started listing,
# because the watch already delivered something newer than the list result.
_seq = 0
_touched: dict[tuple[str, object], int] = {}


def _touch(kind: str, key) -> None:
    global _seq
    _seq += 1
    _touched[(kind, key)] = _seq


def record_event(record: str, namespace: str, source: str, phase: str, completed_ts: float | None) -> None:
    """Track each Backup record (completed_ts None = deleted); the only writer of last success/failure.
    Deleting records, or a record moving to an older time or another phase, lowers or removes the source.
    Inventory and gauges change under one lock, so a concurrent delete can't be undone."""
    new = (namespace, source, phase, completed_ts) \
        if completed_ts is not None and phase in ("Succeeded", "Failed") else None
    with _lock:
        old = _records.pop(record, None)
        if new:
            _records[record] = new
        for key in {k[:3] for k in (old, new) if k}:
            if new and key == new[:3] and (old is None or old[:3] != key or new[3] >= old[3]):
                if new[3] > _latest.get(key, 0):  # only raises: no scan needed
                    _latest[key] = new[3]
                    (LAST_SUCCESS if key[2] == "Succeeded" else LAST_FAILURE).labels(
                        namespace=key[0], source=key[1]).set(new[3])
            else:
                _recompute_latest(*key)


def _recompute_latest(namespace: str, source: str, phase: str) -> None:
    gauge = LAST_SUCCESS if phase == "Succeeded" else LAST_FAILURE
    newest = max((r[3] for r in _records.values() if r[:3] == (namespace, source, phase)), default=None)
    if newest is None:
        _latest.pop((namespace, source, phase), None)
        _set_or_remove(gauge, {"namespace": namespace, "source": source}, None)
    else:
        _latest[(namespace, source, phase)] = newest
        gauge.labels(namespace=namespace, source=source).set(newest)


# Fixed failure reasons (engine and jobspec). Counters start at 0 for every source the operator sees, so
# the first failure is a visible increase instead of a series that appears already at 1.
FAILURE_REASONS = ("ApiErrors", "ApiRejected", "CloneError", "CloneTimeout", "CredentialsNotFound",
                   "InsecureRepository", "InvalidPluginConfig", "InvalidRepository", "JobFailed",
                   "JobTimeout", "NameConflict", "PluginNotFound", "SnapshotError", "SnapshotLimit",
                   "SnapshotTimeout", "SourceNotBound", "SourceNotFound")
_initialised: set[tuple[str, str, str | None]] = set()


def init_source(namespace: str, source: str, pvc: str | None = None) -> None:
    with _lock:
        if (namespace, source, pvc) in _initialised:
            return
        _initialised.add((namespace, source, pvc))
    for result in ("succeeded", "failed"):
        RUNS.labels(namespace=namespace, source=source, result=result)
    for reason in FAILURE_REASONS:
        ATTEMPT_FAILURES.labels(namespace=namespace, source=source, reason=reason)
    if pvc:
        SNAPSHOTS_CREATED.labels(namespace=namespace, pvc=pvc)


def _set_or_remove(gauge, labels: dict, value: float | None) -> None:
    if value is None:
        try:
            gauge.remove(*labels.values())
        except KeyError:
            pass
    else:
        gauge.labels(**labels).set(value)


def _group(ns: str, pvc: str) -> dict:
    return _snap_groups.setdefault((ns, pvc), {"names": set(), "owned": 0, "created": [], "deleting": []})


def _compact(ns: str, pvc: str, g: dict) -> None:
    """Rebuild a group's heaps once stale entries outnumber live ones (bounded memory, amortised O(1))."""
    if len(g["created"]) + len(g["deleting"]) <= 2 * len(g["names"]) + 32:
        return
    live = [(n, _snaps[(ns, n)]) for n in g["names"]]
    g["created"] = [(i["created"], n) for n, i in live if i["owned"]]
    g["deleting"] = [(i["deleting"], n) for n, i in live if i["deleting"]]
    heapq.heapify(g["created"])
    heapq.heapify(g["deleting"])


def _heap_min(ns: str, pvc: str, heap: list, field: str) -> float | None:
    """Smallest value still current: entries of removed or changed snapshots are dropped lazily."""
    while heap:
        value, name = heap[0]
        cur = _snaps.get((ns, name))
        if cur and cur["pvc"] == pvc and cur[field] == value and (field != "created" or cur["owned"]):
            return value
        heapq.heappop(heap)
    return None


def _refresh_snapshot_group(ns: str, pvc: str) -> None:
    g = _group(ns, pvc)
    labels = {"namespace": ns, "pvc": pvc}
    OWNED_SNAPSHOTS.labels(**labels).set(g["owned"])
    _set_or_remove(OWNED_SNAPSHOT_OLDEST, labels, _heap_min(ns, pvc, g["created"], "created"))
    _set_or_remove(SNAPSHOT_DELETING_SINCE, labels, _heap_min(ns, pvc, g["deleting"], "deleting"))
    if not g["names"]:
        _snap_groups.pop((ns, pvc), None)


def snapshot_event(namespace: str, name: str, pvc: str, owned: bool, created: float,
                   deleting: float | None, exists: bool, watch: bool = True) -> None:
    """Track every VolumeSnapshot (any owner). O(log n) per event, so replaying a pile-up stays cheap."""
    key = (namespace, name)
    new = {"pvc": pvc, "owned": owned, "created": created, "deleting": deleting} if exists else None
    with _lock:
        if watch:
            _touch("snapshot", key)
        old = _snaps.pop(key, None)
        if old:
            g = _group(namespace, old["pvc"])
            g["names"].discard(name)
            g["owned"] -= old["owned"]
            _ns_counts[namespace] -= 1
        if new:
            _snaps[key] = new
            g = _group(namespace, pvc)
            g["names"].add(name)
            g["owned"] += owned
            _ns_counts[namespace] = _ns_counts.get(namespace, 0) + 1
            if owned and (old is None or old != new):
                heapq.heappush(g["created"], (created, name))
            if deleting and (old is None or old.get("deleting") != deleting or old["pvc"] != pvc):
                heapq.heappush(g["deleting"], (deleting, name))
            _compact(namespace, pvc, g)
        ALL_SNAPSHOTS.labels(namespace=namespace).set(_ns_counts.get(namespace, 0))
        if old and (new is None or old["pvc"] != pvc):
            _refresh_snapshot_group(namespace, old["pvc"])
        if new:
            _refresh_snapshot_group(namespace, pvc)


def snapshot_names() -> set[str]:
    with _lock:
        return {f"{ns}/{n}" for ns, n in _snaps}


def content_event(name: str, snapshot_ref: str, created: float, exists: bool, watch: bool = True) -> None:
    with _lock:
        if watch:
            _touch("content", name)
        if exists:
            _contents[name] = (snapshot_ref, created)
        else:
            _contents.pop(name, None)


def orphan_contents(now: float) -> int:
    """Contents past the grace period whose snapshot isn't known (checked at scrape time)."""
    with _lock:
        known = {f"{ns}/{n}" for ns, n in _snaps}
        return sum(1 for ref, created in _contents.values()
                   if now - created > ORPHAN_GRACE_SECONDS and ref not in known)


def cleanup_stuck(key: str, stuck: bool) -> None:
    with _lock:
        (_stuck.add if stuck else _stuck.discard)(key)
        CLEANUP_STUCK.set(len(_stuck))



def request_event(key: str, phase: str | None, created: float = 0.0, watch: bool = True) -> None:
    """Track each request's phase (None = deleted); counts are updated incrementally."""
    with _lock:
        if watch:
            _touch("request", key)
        old = _requests.pop(key, None)
        if old:
            _phase_counts[old[0]] -= 1
            REQUESTS.labels(phase=old[0]).set(_phase_counts[old[0]])
        if phase is not None:
            _requests[key] = (phase, created)
            _phase_counts[phase] = _phase_counts.get(phase, 0) + 1
            REQUESTS.labels(phase=phase).set(_phase_counts[phase])


def oldest_unfinished() -> float:
    with _lock:
        return min((c for ph, c in _requests.values() if ph not in TERMINAL_PHASES), default=0.0)


def install_scrape_time_gauges(clock) -> None:
    """Values derived from the inventories are computed when Prometheus scrapes."""
    ORPHAN_CONTENTS.set_function(lambda: orphan_contents(clock()))
    OLDEST_UNFINISHED.set_function(oldest_unfinished)


def resync_begin() -> int:
    """Call before listing; pass the result to resync()."""
    with _lock:
        return _seq


def resync(snapshots: list[dict], contents: list[dict], requests: list[dict], now: float, since: int) -> None:
    """Replace the inventories with a full list, which removes entries whose DELETED event was missed.
    Objects the watch touched after `since` keep the watch's (newer) state. Each item holds the keyword
    arguments of the matching *_event function, without `exists`."""
    with _lock:
        def stale(kind, key):
            return _touched.get((kind, key), 0) <= since

        listed = {(i["namespace"], i["name"]) for i in snapshots}
        for key in [k for k in _snaps if k not in listed and stale("snapshot", k)]:
            snapshot_event(*key, pvc="", owned=False, created=0.0, deleting=None, exists=False, watch=False)
        for i in snapshots:
            if stale("snapshot", (i["namespace"], i["name"])):
                snapshot_event(**i, exists=True, watch=False)

        listed = {i["name"] for i in contents}
        for name in [n for n in _contents if n not in listed and stale("content", n)]:
            content_event(name, "", 0.0, exists=False, watch=False)
        for i in contents:
            if stale("content", i["name"]):
                content_event(**i, exists=True, watch=False)

        listed = {i["key"] for i in requests}
        for key in [k for k in _requests if k not in listed and stale("request", k)]:
            request_event(key, None, watch=False)
        for i in requests:
            if stale("request", i["key"]):
                request_event(**i, watch=False)

        # Stamps up to `since` can't affect a later relist (it starts after this one did).
        for k in [k for k, v in _touched.items() if v <= since]:
            del _touched[k]
        INVENTORY_SYNCED.set(now)
        if not INVENTORY_FIRST_SYNC._value.get():
            INVENTORY_FIRST_SYNC.set(now)


install_scrape_time_gauges(time.time)
