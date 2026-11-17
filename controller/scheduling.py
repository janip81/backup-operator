"""BackupSchedule reconciler (#9): at most one BackupRequest per cron tick, explicit catch-up and
overlap policies, bounded history.

The request name is derived from the schedule name and the tick, so repeated timer runs, failed
status writes and operator restarts can never create a second run for the same tick. Between ticks
the reconciler makes no API calls and writes no status.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from croniter import croniter

from . import config as cfg
from . import metrics
from .engine import LABEL_PREFIX, MANAGED_BY, TERMINAL, parse_ts, ts
from .kube import TransientApiError

log = logging.getLogger("backup-operator.schedule")

SCHEDULE_LABEL = f"{LABEL_PREFIX}/schedule"
SCHEDULE_UID_LABEL = f"{LABEL_PREFIX}/schedule-uid"
SCHEDULED_TIME = f"{LABEL_PREFIX}/scheduled-time"


def _tz(spec: dict):
    return ZoneInfo(spec.get("timeZone") or "UTC")


def latest_tick(spec: dict, now: datetime) -> datetime:
    """The most recent tick at or before now (UTC); never a tick in the future."""
    local = now.astimezone(_tz(spec))
    prev = croniter(spec["schedule"], local).get_prev(datetime)
    following = croniter(spec["schedule"], prev).get_next(datetime)
    tick = following if following <= local else prev  # include a tick exactly at now
    return tick.astimezone(UTC)


def next_tick(spec: dict, after: datetime) -> datetime:
    it = croniter(spec["schedule"], after.astimezone(_tz(spec)))
    return it.get_next(datetime).astimezone(UTC)


def interval_seconds(spec: dict, now: datetime) -> float:
    a = next_tick(spec, now)
    return (next_tick(spec, a) - a).total_seconds()


def request_name(schedule: str, uid: str, tick: datetime) -> str:
    """Identity = schedule UID + tick. A recreated schedule with the same name gets different names."""
    stamp = tick.strftime("%Y%m%d%H%M")
    tag = hashlib.sha256(uid.encode()).hexdigest()[:6]
    prefix = schedule[:63 - len(stamp) - len(tag) - 2].rstrip("-.")
    return f"{prefix}-{tag}-{stamp}"


def validate(spec: dict, ns: str, kube) -> tuple[bool, str, str]:
    """Check what a run needs before creating it (#15): valid repository URL, an existing Bound source PVC,
    a StorageClass for the temporary PVC that exists (when one is named), the credentials Secret (with RESTIC_PASSWORD) and, for custom plugins, the script ConfigMap."""
    from .jobspec import repository_problem
    t = spec.get("template") or {}
    src_ns = t.get("sourceNamespace") or ns
    repo = t.get("repository") or {}
    problem = repository_problem(repo, None)
    if problem:
        return False, problem[0], problem[1]
    pvc = kube.get_pvc(src_ns, t.get("sourcePVC") or "")
    if not pvc:
        return False, "SourcePVCNotFound", f"Source PVC {src_ns}/{t.get('sourcePVC')} not found"
    if (pvc.get("status") or {}).get("phase") != "Bound":
        return False, "SourcePVCNotBound", f"Source PVC {src_ns}/{t.get('sourcePVC')} is not Bound"
    from .jobspec import clone_storage_class
    sc_name = clone_storage_class(t, pvc)
    if sc_name and not kube.get_storage_class(sc_name):
        return False, "StorageClassNotFound", f"StorageClass {sc_name} for the temporary PVC not found"
    secret = kube.get_secret(src_ns, repo.get("credentialsSecret") or "")
    if not secret:
        return False, "CredentialsNotFound", f"Secret {src_ns}/{repo.get('credentialsSecret')} not found"
    problem = repository_problem(repo, list((secret.get("data") or {}).keys()))
    if problem:
        return False, problem[0], f"{problem[1]} ({src_ns}/{repo.get('credentialsSecret')})"
    plugin = t.get("plugin") or {}
    if plugin.get("name") == "custom":
        ref = plugin.get("configMapRef") or {}
        cm = kube.get_configmap(src_ns, ref.get("name") or "")
        if not cm or ref.get("key", "plugin.sh") not in (cm.get("data") or {}):
            return False, "PluginNotFound", f"ConfigMap {src_ns}/{ref.get('name')} with the plugin script not found"
    return True, "Valid", "Source volume, credentials and repository settings are valid"


def _ready_patch(status: dict, ok: bool, reason: str, message: str, now: datetime, generation) -> dict:
    current = next((c for c in status.get("conditions") or [] if c.get("type") == "Ready"), {})
    value = "True" if ok else "False"
    cond = {"type": "Ready", "status": value, "reason": reason, "message": message,
            "lastTransitionTime": current.get("lastTransitionTime") if current.get("status") == value else ts(now)}
    patch = {}
    if {k: current.get(k) for k in cond} != cond:
        patch["conditions"] = [cond]
    if status.get("observedGeneration") != generation:
        patch["observedGeneration"] = generation
    return patch


def reconcile_schedule(body: dict, kube, now: datetime) -> dict | None:
    md, spec = body["metadata"], body.get("spec") or {}
    status = dict(body.get("status") or {})
    name, ns, uid = md["name"], md["namespace"], md["uid"]
    src_ns = _src_ns(spec, ns)
    metrics.SCHEDULE_INTERVAL.labels(namespace=src_ns, source=name).set(interval_seconds(spec, now))
    metrics.init_source(src_ns, name, (spec.get("template") or {}).get("sourcePVC"))
    generation = md.get("generation", 1)
    ready_cond = next((c for c in status.get("conditions") or [] if c.get("type") == "Ready"), None)

    patch: dict = {}
    if status.get("observedGeneration") != generation or ready_cond is None:
        # New or changed schedule: validate once now (between ticks nothing else touches the API).
        ok, reason, message = validate(spec, ns, kube)
        patch.update(_ready_patch(status, ok, reason, message, now, generation))
        metrics.SCHEDULE_READY.labels(namespace=src_ns, source=name).set(1 if ok else 0)
    elif ready_cond:
        metrics.SCHEDULE_READY.labels(namespace=src_ns, source=name).set(1 if ready_cond["status"] == "True" else 0)

    # Paused (whenSourceUnused: Skip) holds between ticks; only a tick decision changes it. Leaving Skip
    # clears it at once, so a schedule is never reported paused (and its staleness alert suppressed)
    # under a policy that can't pause.
    skip_policy = (spec.get("whenSourceUnused") or "Run") == "Skip"
    paused = skip_policy and status.get("phase") == "Paused"
    phase = "Suspended" if spec.get("suspend") else "Paused" if paused else "Active"
    if status.get("phase") != phase:
        patch["phase"] = phase
    if not skip_policy and status.get("sourceUnusedSince"):
        patch["sourceUnusedSince"] = None
    metrics.SCHEDULE_PAUSED.labels(namespace=src_ns, source=name).set(1 if phase == "Paused" else 0)
    upcoming = ts(next_tick(spec, now))
    created = parse_ts(md.get("creationTimestamp")) or now
    last = parse_ts(status.get("lastScheduleTime")) or created  # no run for ticks before creation

    tick = latest_tick(spec, now)
    if spec.get("suspend") or tick <= last:
        if status.get("nextScheduleTime") != upcoming:
            patch["nextScheduleTime"] = upcoming
        return patch or None

    # A tick is due: decide once, then record lastScheduleTime so it is never re-evaluated.
    patch.update({"lastScheduleTime": ts(tick), "nextScheduleTime": upcoming})
    deadline = int(spec.get("startingDeadlineSeconds") or cfg.SCHEDULE_STARTING_DEADLINE)
    if (now - tick).total_seconds() > deadline:
        return {**patch, "lastSkipped": {"time": ts(tick), "reason": "MissedDeadline"},
                "message": f"Skipped {ts(tick)}: more than {deadline}s late (operator down?)"}

    req_name = request_name(name, uid, tick)
    owned = kube.list_crs_ns("backuprequests", ns, f"{SCHEDULE_UID_LABEL}={uid}")
    # Pause state first, on every due tick and before any other early return: a retry after a lost
    # status write, a failing validation or an active previous run must not leave a stale Paused.
    pause = _pause_decision(spec, status, kube, src_ns, owned, tick)
    if pause is not None:
        patch.update(pause)
        metrics.SCHEDULE_PAUSED.labels(namespace=src_ns, source=name).set(1 if pause["phase"] == "Paused" else 0)
        if pause["phase"] == "Paused" and not kube.get_cr("backuprequests", ns, req_name):
            return {**patch, "lastSkipped": {"time": ts(tick), "reason": "SourceUnused"},
                    "message": f"Paused at {ts(tick)}: no pod uses the source PVC and a backup of it exists"}
    if (spec.get("concurrencyPolicy") or "Forbid") == "Forbid":
        active = [r["metadata"]["name"] for r in owned
                  if (r.get("status") or {}).get("phase") not in TERMINAL and r["metadata"]["name"] != req_name]
        if active:
            return {**patch, "lastSkipped": {"time": ts(tick), "reason": "PreviousRunActive"},
                    "message": f"Skipped {ts(tick)}: previous run still active ({', '.join(active)})"}

    existing = kube.get_cr("backuprequests", ns, req_name)
    if existing is None:
        # Re-check right before the run: a broken setup skips the tick visibly instead of creating a run
        # that can only fail. (An already created request for this tick is kept: idempotency first.)
        ok, reason, message = validate(spec, ns, kube)
        patch.update(_ready_patch({**status, **patch}, ok, reason, message, now, generation))
        metrics.SCHEDULE_READY.labels(namespace=src_ns, source=name).set(1 if ok else 0)
        if not ok:
            return {**patch, "lastSkipped": {"time": ts(tick), "reason": reason},
                    "message": f"Skipped {ts(tick)}: {message}"}
        kube.create_cr("backuprequests", ns, _request(body, req_name, tick))
        existing = kube.get_cr("backuprequests", ns, req_name)
    if ((existing or {}).get("metadata") or {}).get("labels", {}).get(SCHEDULE_UID_LABEL) != uid:
        raise TransientApiError(f"BackupRequest {req_name} exists but does not belong to this schedule")
    _prune_history(kube, ns, spec, owned, keep_name=req_name)
    return {**patch, "lastRequest": req_name, "message": f"Created {req_name} for {ts(tick)}", "lastSkipped": None}


def source_in_use(kube, ns: str, pvc: str) -> bool:
    """True if a pod that hasn't finished mounts the PVC (Pending counts: it is about to use it)."""
    for pod in kube.list_pods(ns):
        if (pod.get("status") or {}).get("phase") in ("Succeeded", "Failed"):
            continue
        for vol in (pod.get("spec") or {}).get("volumes") or []:
            if (vol.get("persistentVolumeClaim") or {}).get("claimName") == pvc:
                return True
    return False


def _pause_decision(spec: dict, status: dict, kube, src_ns: str, owned: list[dict],
                    tick: datetime) -> dict | None:
    """whenSourceUnused: Skip. While a pod uses the source: run and forget when it became unused. Once
    unused: remember the first such tick and keep running until a backup started at or after it has
    succeeded (the app's final state), then pause. Returns a status patch, or None for whenSourceUnused: Run."""
    if (spec.get("whenSourceUnused") or "Run") != "Skip":
        return None
    pvc = (spec.get("template") or {}).get("sourcePVC") or ""
    if source_in_use(kube, src_ns, pvc):
        return {"sourceUnusedSince": None, "phase": "Active"}
    since = parse_ts(status.get("sourceUnusedSince")) or tick
    starts = [parse_ts((r.get("status") or {}).get("startedAt")) for r in owned
              if (r.get("status") or {}).get("phase") == "Succeeded"]
    newest = max((t for t in starts if t), default=None)
    paused = newest is not None and newest >= since
    return {"sourceUnusedSince": ts(since), "phase": "Paused" if paused else "Active"}


def _src_ns(spec: dict, ns: str) -> str:
    return (spec.get("template") or {}).get("sourceNamespace") or ns


def _request(schedule: dict, name: str, tick: datetime) -> dict:
    md = schedule["metadata"]
    return {
        "apiVersion": f"{cfg.GROUP}/{cfg.VERSION}",
        "kind": "BackupRequest",
        "metadata": {
            "name": name,
            "namespace": md["namespace"],
            "labels": {MANAGED_BY[0]: MANAGED_BY[1], SCHEDULE_LABEL: md["name"][:63], SCHEDULE_UID_LABEL: md["uid"]},
            "annotations": {SCHEDULED_TIME: ts(tick)},
            "ownerReferences": [{"apiVersion": f"{cfg.GROUP}/{cfg.VERSION}", "kind": "BackupSchedule",
                                 "name": md["name"], "uid": md["uid"], "controller": True,
                                 "blockOwnerDeletion": False}],
        },
        "spec": dict(schedule["spec"].get("template") or {}),
    }


def _prune_history(kube, ns: str, spec: dict, owned: list[dict], keep_name: str) -> None:
    """Keep the newest N succeeded and M failed requests of this schedule; never touch active ones,
    and never the request of the tick being decided (it is this tick's idempotency marker until
    lastScheduleTime is persisted). Deleting a request runs its cleanup finalizer."""
    limits = {"Succeeded": int(spec.get("successfulHistoryLimit", 3)), "Failed": int(spec.get("failedHistoryLimit", 3))}
    unique = {r["metadata"]["name"]: r for r in owned if r["metadata"]["name"] != keep_name}
    for phase, keep in limits.items():
        done = [r for r in unique.values() if (r.get("status") or {}).get("phase") == phase]
        done.sort(key=lambda r: r["metadata"].get("annotations", {}).get(SCHEDULED_TIME, ""), reverse=True)
        for r in done[keep:]:
            kube.delete_cr("backuprequests", ns, r["metadata"]["name"], r["metadata"].get("uid"))


def now_utc() -> datetime:
    return datetime.now(UTC)
