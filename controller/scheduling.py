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


def reconcile_schedule(body: dict, kube, now: datetime) -> dict | None:
    md, spec = body["metadata"], body.get("spec") or {}
    status = dict(body.get("status") or {})
    name, ns, uid = md["name"], md["namespace"], md["uid"]
    metrics.SCHEDULE_INTERVAL.labels(namespace=_src_ns(spec, ns), source=name).set(interval_seconds(spec, now))

    patch: dict = {}
    phase = "Suspended" if spec.get("suspend") else "Active"
    if status.get("phase") != phase:
        patch["phase"] = phase
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
    if (spec.get("concurrencyPolicy") or "Forbid") == "Forbid":
        active = [r["metadata"]["name"] for r in owned
                  if (r.get("status") or {}).get("phase") not in TERMINAL and r["metadata"]["name"] != req_name]
        if active:
            return {**patch, "lastSkipped": {"time": ts(tick), "reason": "PreviousRunActive"},
                    "message": f"Skipped {ts(tick)}: previous run still active ({', '.join(active)})"}

    kube.create_cr("backuprequests", ns, _request(body, req_name, tick))
    existing = kube.get_cr("backuprequests", ns, req_name)
    if ((existing or {}).get("metadata") or {}).get("labels", {}).get(SCHEDULE_UID_LABEL) != uid:
        raise TransientApiError(f"BackupRequest {req_name} exists but does not belong to this schedule")
    _prune_history(kube, ns, spec, owned, keep_name=req_name)
    return {**patch, "lastRequest": req_name, "message": f"Created {req_name} for {ts(tick)}", "lastSkipped": None}


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
