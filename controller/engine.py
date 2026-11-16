"""BackupRequest reconciler: a bounded, resumable state machine (#8).

Every reconcile cycle reads the request's persisted status, performs at most one short step
(no sleeping, no long polling), and returns a status patch. Retries reuse the same
deterministic resource names, so a restart or a transient error continues the current attempt
instead of creating new objects.

Phases:
  Pending      -> waiting for admission (concurrency/snapshot limits) or for the next attempt
  Snapshotting -> VolumeSnapshot created, waiting for readyToUse
  Cloning      -> PVC restored from the snapshot, waiting for Bound (or WaitForFirstConsumer)
  Running      -> worker Job running
  CleaningUp   -> deleting this attempt's temporary objects, then finalizing the outcome
  Succeeded / Failed -> terminal
"""
from __future__ import annotations

import json
import logging
import random
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kubernetes.client.exceptions import ApiException

from . import config as cfg
from . import jobspec, metrics
from .kube import TransientApiError

log = logging.getLogger("backup-operator.engine")

TERMINAL = ("Succeeded", "Failed")
ACTIVE = ("Snapshotting", "Cloning", "Running", "Recording", "CleaningUp")
NEVER_FAIL = ("Recording", "CleaningUp")  # after the transfer: errors back off, the outcome never changes
LABEL_PREFIX = "backup.techmonkeys.se"
MANAGED_BY = ("app.kubernetes.io/managed-by", "backup-operator")


class AttemptFailed(Exception):
    """The current attempt can't complete. terminal=True means retrying won't help."""

    def __init__(self, reason: str, message: str, terminal: bool = False):
        super().__init__(message)
        self.reason, self.message, self.terminal = reason, message, terminal


# --------------------------------------------------------------------------- time helpers
def ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def retry_delay(attempt: int, rnd=random.random) -> float:
    """Capped exponential backoff with +-20% jitter for attempt N (1-based) failing."""
    base = min(cfg.RETRY_BASE_SECONDS * 2 ** (attempt - 1), cfg.RETRY_MAX_SECONDS)
    return base * (0.8 + 0.4 * rnd())


def api_error_delay(errors: int, rnd=random.random) -> float:
    return min(5 * 2 ** errors, 300) * (0.8 + 0.4 * rnd())


# --------------------------------------------------------------------------- admission
class Admission:
    """In-process admission control. The operator runs as a single active replica (kopf peering).
    New runs are admitted only after a successful bootstrap has registered every run that was
    already active, so a restart can never exceed the limits."""

    def __init__(self, ready: bool = False):
        self.ready = ready
        self._lock = threading.Lock()
        self._active: dict[str, str] = {}  # request key -> source pvc key

    def note_active(self, key: str, pvc_key: str) -> None:
        with self._lock:
            self._active[key] = pvc_key

    def release(self, key: str) -> None:
        with self._lock:
            self._active.pop(key, None)

    def try_admit(self, key: str, pvc_key: str, now: datetime) -> tuple[bool, str]:
        with self._lock:
            if key in self._active:
                return True, ""
            if not self.ready:
                return False, "Operator starting; waiting for the list of in-flight runs"
            if pvc_key in self._active.values():
                return False, f"Another run is active for {pvc_key}"
            if len(self._active) >= cfg.MAX_CONCURRENT_RUNS:
                return False, f"Concurrency limit reached ({cfg.MAX_CONCURRENT_RUNS} active runs)"
            self._active[key] = pvc_key
            return True, ""

    def bootstrap(self, requests: list[dict]) -> None:
        """Register runs that were active before a restart, so limits hold from the first cycle."""
        for body in requests:
            phase = (body.get("status") or {}).get("phase")
            if phase in ACTIVE:
                req = Req.from_body(body)
                self.note_active(req.key, req.pvc_key)
        self.ready = True

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)


# --------------------------------------------------------------------------- request view
@dataclass
class Req:
    name: str
    namespace: str
    uid: str
    spec: dict
    status: dict
    labels_in: dict

    @classmethod
    def from_body(cls, body: dict) -> Req:
        md = body["metadata"]
        return cls(md["name"], md["namespace"], md["uid"], dict(body.get("spec") or {}), dict(body.get("status") or {}),
                   dict(md.get("labels") or {}))

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def src_ns(self) -> str:
        # Recorded at first admission; the spec is immutable too, but cleanup must never follow a changed spec.
        return self.status.get("sourceNamespace") or self.spec.get("sourceNamespace") or self.namespace

    @property
    def src_pvc(self) -> str:
        return self.spec["sourcePVC"]

    @property
    def source(self) -> str:
        """Stable identity for metrics and records: the schedule name, or the source PVC for ad-hoc runs."""
        return self.labels_in.get(f"{LABEL_PREFIX}/schedule") or self.src_pvc

    @property
    def pvc_key(self) -> str:
        return f"{self.src_ns}/{self.src_pvc}"

    @property
    def attempt(self) -> int:
        return int(self.status.get("attempt") or 1)

    @property
    def max_attempts(self) -> int:
        return int(self.spec.get("maxAttempts") or cfg.DEFAULT_MAX_ATTEMPTS)

    def names(self, attempt: int | None = None) -> dict:
        base = f"bo-{self.uid[:8]}-a{attempt or self.attempt}"
        return {"snapshot": f"{base}-snap", "pvc": f"{base}-pvc", "job": f"{base}-job"}

    def labels(self) -> dict:
        return {
            MANAGED_BY[0]: MANAGED_BY[1],
            f"{LABEL_PREFIX}/request": self.name[:63],
            f"{LABEL_PREFIX}/request-uid": self.uid,
            f"{LABEL_PREFIX}/source-pvc": self.src_pvc[:63],
            f"{LABEL_PREFIX}/attempt": str(self.attempt),
        }


    def selector(self) -> str:
        return f"{MANAGED_BY[0]}={MANAGED_BY[1]},{LABEL_PREFIX}/request-uid={self.uid}"

    def owns(self, obj: dict | None) -> bool:
        labels = ((obj or {}).get("metadata") or {}).get("labels") or {}
        return labels.get(MANAGED_BY[0]) == MANAGED_BY[1] and labels.get(f"{LABEL_PREFIX}/request-uid") == self.uid


def _require_owned(req: Req, obj: dict | None, kind: str, name: str) -> dict | None:
    """An object with our deterministic name that we didn't create is never used or deleted (#10)."""
    if obj is not None and not req.owns(obj):
        raise AttemptFailed("NameConflict", f"{kind} {req.src_ns}/{name} exists but is not owned by this request",
                            terminal=True)
    return obj


# --------------------------------------------------------------------------- reconcile
def reconcile(body: dict, kube, admission: Admission, now: datetime, rnd=random.random) -> dict | None:
    """Run one step for a BackupRequest. Returns a status patch, or None when nothing changed."""
    req = Req.from_body(body)
    if (body.get("metadata") or {}).get("deletionTimestamp"):
        return None  # being deleted: the delete handler owns cleanup (#10)
    phase = req.status.get("phase") or "Pending"
    if phase in TERMINAL:
        admission.release(req.key)
        return None

    if phase in ACTIVE:
        # Before the backoff check: an active run must always count against the limits,
        # including right after a restart while it is backing off.
        admission.note_active(req.key, req.pvc_key)

    nxt = parse_ts(req.status.get("nextAttemptAt"))
    if nxt and now < nxt:
        return None  # backing off: no API calls at all

    patch = _step(req, phase, kube, admission, now, rnd)
    if patch and patch.pop("_release", None):
        admission.release(req.key)
    return patch or None


def _step(req: Req, phase: str, kube, admission: Admission, now: datetime, rnd) -> dict | None:
    try:
        patch = STEPS[phase](req, kube, admission, now) or {}
        if req.status.get("apiErrors") and "apiErrors" not in patch:
            patch["apiErrors"] = 0
        if req.status.get("nextAttemptAt") and "nextAttemptAt" not in patch:
            patch["nextAttemptAt"] = None
        return patch
    except TransientApiError as e:
        errors = int(req.status.get("apiErrors") or 0) + 1
        log.warning("%s: transient API error %d/%d: %s", req.key, errors, cfg.API_ERROR_BUDGET, e)
        if phase in NEVER_FAIL or errors <= cfg.API_ERROR_BUDGET:
            # Cleanup never gives up and never changes the attempt's outcome: it backs off (capped) and retries.
            return {"apiErrors": errors, "lastError": str(e)[:500],
                    "nextAttemptAt": ts(now + timedelta(seconds=api_error_delay(errors, rnd)))}
        return _fail_attempt(req, phase, AttemptFailed("ApiErrors", f"Too many consecutive API errors: {e}"), now)
    except (AttemptFailed, ApiException) as e:
        if isinstance(e, ApiException):  # permanent API rejection (400/403/422): retrying won't help
            e = AttemptFailed("ApiRejected", f"HTTP {e.status}: {e.reason}", True)
        if phase in NEVER_FAIL:
            errors = int(req.status.get("apiErrors") or 0) + 1
            # Diagnostics of the run itself (reason/message) are kept; the blocker goes to lastError.
            return {"lastError": f"{phase} blocked: {e.message}"[:500], "apiErrors": errors,
                    "nextAttemptAt": ts(now + timedelta(seconds=api_error_delay(errors, rnd)))}
        return _fail_attempt(req, phase, e, now)


def _fail_attempt(req: Req, phase: str, err: AttemptFailed, now: datetime) -> dict:
    log.warning("%s: attempt %d failed (%s): %s", req.key, req.attempt, err.reason, err.message)
    metrics.ATTEMPT_FAILURES.labels(namespace=req.src_ns, source=req.source, reason=err.reason).inc()
    retry = not err.terminal and req.attempt < req.max_attempts
    history = list(req.status.get("attempts") or [])[-9:] + [
        {"attempt": req.attempt, "reason": err.reason, "message": err.message[:300], "finishedAt": ts(now)}]
    patch = {"outcome": "retry" if retry else "failed", "reason": err.reason, "message": err.message[:1000],
             "lastError": err.message[:500], "apiErrors": 0, "attempts": history, "attempt": req.attempt}
    if not retry:
        # Final failure: record it durably before anything is cleaned up (#11).
        return {**patch, "phase": "Recording", "phaseStartedAt": ts(now), "completedAt": ts(now)}
    if phase == "Pending":
        return {**patch, **_finalize(req, "retry", now)}  # nothing was created for this attempt
    return {**patch, "phase": "CleaningUp", "phaseStartedAt": ts(now)}


def _phase_timed_out(req: Req, now: datetime, limit: int) -> bool:
    started = parse_ts(req.status.get("phaseStartedAt"))
    return bool(started and now - started > timedelta(seconds=limit))


# --------------------------------------------------------------------------- steps
def step_pending(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    # Admission first (in memory): queued requests make no API calls at all.
    ok, why = admission.try_admit(req.key, req.pvc_key, now)
    if not ok:
        return _queued(req, "Queued", why)
    try:
        return _start_attempt(req, kube, admission, now)
    except BaseException:
        admission.release(req.key)
        raise


def _start_attempt(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    src = kube.get_pvc(req.src_ns, req.src_pvc)
    if not src:
        raise AttemptFailed("SourceNotFound", f"Source PVC {req.pvc_key} not found")
    if (src.get("status") or {}).get("phase") != "Bound":
        raise AttemptFailed("SourceNotBound", f"Source PVC {req.pvc_key} is not Bound")

    selector = (f"{MANAGED_BY[0]}={MANAGED_BY[1]},"
                f"{LABEL_PREFIX}/source-pvc={req.src_pvc[:63]}")
    existing = kube.list_snapshots(req.src_ns, selector)
    if len(existing) >= cfg.MAX_SNAPSHOTS_PER_PVC:
        admission.release(req.key)
        names = ", ".join(s["metadata"]["name"] for s in existing)
        return _queued(req, "SnapshotCeiling",
                       f"{len(existing)} operator-owned snapshot(s) already exist for {req.pvc_key}: {names}")

    patch = {"phase": "Snapshotting", "phaseStartedAt": ts(now), "attempt": req.attempt,
             "sourceNamespace": req.src_ns,
             "maxAttempts": req.max_attempts, "resources": req.names(), "reason": "Snapshotting",
             "message": f"Attempt {req.attempt}/{req.max_attempts}: creating snapshot", "outcome": None}
    if not req.status.get("startedAt"):
        patch["startedAt"] = ts(now)
    return patch


def _queued(req: Req, reason: str, message: str) -> dict | None:
    if req.status.get("reason") == reason and req.status.get("message") == message:
        return None  # unchanged: avoid status churn
    return {"phase": "Pending", "reason": reason, "message": message}


def step_snapshotting(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    n = req.names()
    snap = _require_owned(req, kube.get_snapshot(req.src_ns, n["snapshot"]), "VolumeSnapshot", n["snapshot"])
    if snap is None:
        kube.create_snapshot(req.src_ns, jobspec.snapshot(req, n))
        return None
    st = snap.get("status") or {}
    err = (st.get("error") or {}).get("message") or ""
    if err:
        if "FailedPrecondition" in err or "maximum" in err.lower():
            raise AttemptFailed("SnapshotLimit", f"Storage refused the snapshot (per-volume limit): {err}",
                                terminal=True)
        raise AttemptFailed("SnapshotError", err)
    if st.get("readyToUse"):
        return {"phase": "Cloning", "phaseStartedAt": ts(now), "reason": "Cloning",
                "message": "Snapshot ready; restoring a temporary PVC", "restoreSize": st.get("restoreSize")}
    if _phase_timed_out(req, now, cfg.SNAPSHOT_TIMEOUT):
        raise AttemptFailed("SnapshotTimeout", f"Snapshot not ready after {cfg.SNAPSHOT_TIMEOUT}s")
    return None


def step_cloning(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    n = req.names()
    pvc = _require_owned(req, kube.get_pvc(req.src_ns, n["pvc"]), "PersistentVolumeClaim", n["pvc"])
    if pvc is None:
        size = req.status.get("restoreSize")
        if not size:
            src = kube.get_pvc(req.src_ns, req.src_pvc) or {}
            size = (((src.get("spec") or {}).get("resources") or {}).get("requests") or {}).get("storage")
        if not size:
            raise AttemptFailed("CloneError", "Could not determine the size for the temporary PVC")
        kube.create_pvc(req.src_ns, jobspec.clone_pvc(req, n, size))
        return None
    if (pvc.get("status") or {}).get("phase") == "Bound" or _waits_for_consumer(kube, pvc):
        return {"phase": "Running", "phaseStartedAt": ts(now), "reason": "Running",
                "message": "Temporary PVC ready; starting worker job"}
    if _phase_timed_out(req, now, cfg.CLONE_TIMEOUT):
        raise AttemptFailed("CloneTimeout", f"Temporary PVC not bound after {cfg.CLONE_TIMEOUT}s")
    return None


def _waits_for_consumer(kube, pvc: dict) -> bool:
    sc_name = (pvc.get("spec") or {}).get("storageClassName")
    sc = kube.get_storage_class(sc_name) if sc_name else None
    return bool(sc and sc.get("volumeBindingMode") == "WaitForFirstConsumer")


def step_running(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    n = req.names()
    job = _require_owned(req, kube.get_job(req.src_ns, n["job"]), "Job", n["job"])
    if job is None:
        jobspec.ensure_dependencies(req, n, kube)
        kube.create_job(req.src_ns, jobspec.worker_job(req, n))
        return None
    for c in (job.get("status") or {}).get("conditions") or []:
        if c.get("status") != "True":
            continue
        if c.get("type") == "Complete":
            done = (job.get("status") or {}).get("completionTime") or ts(now)
            return {"phase": "Recording", "phaseStartedAt": ts(now), "outcome": "succeeded", "completedAt": done,
                    "reason": "Recording", "message": "Worker job completed; recording the result"}
        if c.get("type") == "Failed":
            raise AttemptFailed("JobFailed", f"Worker job failed: {c.get('reason')}: {c.get('message') or ''}")
    if _phase_timed_out(req, now, cfg.JOB_TIMEOUT + 300):
        raise AttemptFailed("JobTimeout", f"Worker job did not finish within {cfg.JOB_TIMEOUT}s")
    return None


def record_name(req: Req, existing: dict | None) -> str:
    """The record is named after the request. If an unrelated record (a deleted request that used the same
    name) already holds that name, this request's record gets a UID suffix instead of adopting it."""
    if existing is None or _record_uid(existing) == req.uid:
        return req.name
    return f"{req.name[:54]}-{req.uid[:8]}"


def _record_uid(rec: dict | None) -> str | None:
    return (((rec or {}).get("spec") or {}).get("requestRef") or {}).get("uid")


def step_recording(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    """Persist the outcome as a Backup record before anything is deleted (#11). Idempotent: repeating
    this step after an error or restart never duplicates the record and never re-runs the transfer.
    Cleanup only starts after the stored record has been read back with the expected result."""
    outcome = req.status.get("outcome") or "failed"
    phase = "Succeeded" if outcome == "succeeded" else "Failed"
    name = req.status.get("backupRef") or record_name(req, kube.get_cr("backups", req.namespace, req.name))
    if name != req.status.get("backupRef"):
        return {"backupRef": name}  # persist the chosen name first, then write the record next cycle
    rec = kube.get_cr("backups", req.namespace, name)
    if rec is None:
        kube.create_cr("backups", req.namespace, jobspec.backup_record(req, name))
        return None  # next cycle reads it back and checks it is ours before writing the result
    if _record_uid(rec) != req.uid:  # includes records without a UID: never adopted or overwritten
        raise TransientApiError(f"Backup {name} exists but was not created by this request")
    stored = (rec or {}).get("status") or {}
    if stored.get("phase") == phase:  # durable: verified by reading it back
        completed = parse_ts(stored.get("completedAt")) or now
        started = parse_ts(stored.get("startedAt"))
        metrics.RUNS.labels(namespace=req.src_ns, source=req.source, result=phase.lower()).inc()
        if started and phase == "Succeeded":
            metrics.DURATION.labels(namespace=req.src_ns, source=req.source).observe(
                (completed - started).total_seconds())
        metrics.observe_result(req.src_ns, req.source, phase, completed.timestamp())
        if phase == "Succeeded":
            _sync_records_with_retention(req, kube, name, stored)
        return {"phase": "CleaningUp", "phaseStartedAt": ts(now),
                "message": f"{phase} recorded as Backup {name}; removing temporary resources"}

    result = _job_result(req, kube)
    started = parse_ts(req.status.get("startedAt"))
    completed = parse_ts(req.status.get("completedAt")) or now
    status = {
        "phase": phase, "completedAt": ts(completed), "recordedAt": ts(now), "startedAt": req.status.get("startedAt"),
        "durationSeconds": int((completed - started).total_seconds()) if started else None,
        "attempts": req.attempt, "source": req.source,
        "reason": "Succeeded" if phase == "Succeeded" else req.status.get("reason"),
        "message": (req.status.get("message") or "")[:1000] if phase == "Failed" else "Backup completed",
        "result": result.get("result"), "artifact": result.get("artifact"),
    }
    # Conditional on the version we just verified as ours: a replaced record makes this fail (409), not overwrite.
    kube.patch_cr_status("backups", req.namespace, name, {k: v for k, v in status.items() if v is not None},
                         resource_version=rec["metadata"].get("resourceVersion"))
    return None  # next cycle reads the record back before cleanup starts


def _sync_records_with_retention(req: Req, kube, current: str, stored: dict) -> None:
    """Delete Succeeded Backup records whose restic snapshot was just pruned by retention (#12), so the
    records list exactly the recovery points that still exist. Only records of the same source and
    repository are considered, and nothing is deleted unless the worker reported a complete kept list."""
    retention = (stored.get("artifact") or {}).get("retention") or {}
    kept = retention.get("kept")
    if not kept or len(kept) >= 200 or not retention.get("removed"):
        return
    repo = (req.spec.get("repository") or {}).get("url")
    selector = f"{MANAGED_BY[0]}={MANAGED_BY[1]},{LABEL_PREFIX}/source={req.source[:63]}"
    for rec in kube.list_crs_ns("backups", req.namespace, selector):
        st, spec = rec.get("status") or {}, rec.get("spec") or {}
        short = (st.get("artifact") or {}).get("shortId")
        if (rec["metadata"]["name"] != current and st.get("phase") == "Succeeded" and short
                and short not in kept and spec.get("repository") == repo and spec.get("source") == req.source
                and spec.get("sourceNamespace") == req.src_ns):  # same scope as the worker: host = namespace
            log.info("%s: removing record %s (snapshot %s pruned by retention)", req.key, rec["metadata"]["name"], short)
            kube.delete_cr("backups", req.namespace, rec["metadata"]["name"], rec["metadata"].get("uid"))


def _job_result(req: Req, kube) -> dict:
    """Exit details and the worker's JSON summary (termination message, never logs) from the job pod."""
    pods = kube.list_job_pods(req.src_ns, req.names()["job"]) if req.status.get("resources") else []
    for pod in pods:
        for cs in ((pod.get("status") or {}).get("containerStatuses") or []):
            term = ((cs.get("state") or {}).get("terminated")) or {}
            if not term:
                continue
            out = {"result": {"exitCode": term.get("exitCode"), "reason": term.get("reason")}}
            msg = term.get("message") or ""
            try:
                parsed = json.loads(msg) if msg.strip().startswith("{") else None
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                out["artifact"] = parsed
            return out
    return {}


def step_cleaning(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    n = req.names()
    ns = req.src_ns
    remaining = []
    for kind, getter, deleter in (
        ("job", kube.get_job, kube.delete_job),
        ("pvc", kube.get_pvc, kube.delete_pvc),
        ("snapshot", kube.get_snapshot, kube.delete_snapshot),
    ):
        obj = getter(ns, n[kind])
        if obj is None or not req.owns(obj):
            continue  # gone, or not ours: never delete objects this request didn't create
        remaining.append(n[kind])
        if not (obj.get("metadata") or {}).get("deletionTimestamp"):
            deleter(ns, n[kind], obj["metadata"].get("uid"))
    if remaining:
        if _phase_timed_out(req, now, cfg.CLEANUP_TIMEOUT):
            metrics.cleanup_stuck(req.key, True)
            msg = f"Cleanup stuck, still waiting for: {', '.join(remaining)}"
            if req.status.get("lastError") != msg:
                return {"lastError": msg}
        return None
    metrics.cleanup_stuck(req.key, False)
    return _finalize(req, req.status.get("outcome") or "failed", now)


def cleanup_all(req: Req, kube) -> list[str]:
    """Delete every object any attempt of this request created (label-selected, ownership-checked).
    Used when a request is deleted mid-run. Returns the names still waiting to disappear."""
    remaining = []
    for kind in ("job", "pvc", "snapshot"):
        for obj in kube.list_owned(kind, req.src_ns, req.selector()):
            if not req.owns(obj):
                continue
            name = obj["metadata"]["name"]
            remaining.append(name)
            if not obj["metadata"].get("deletionTimestamp"):
                kube.delete(kind, req.src_ns, name, obj["metadata"].get("uid"))
    return remaining


def _finalize(req: Req, outcome: str, now: datetime) -> dict:
    """End of an attempt, after cleanup. The result was already recorded in Recording."""
    if outcome == "retry":
        delay = retry_delay(req.attempt)
        return {"phase": "Pending", "attempt": req.attempt + 1, "outcome": None, "resources": None,
                "nextAttemptAt": ts(now + timedelta(seconds=delay)), "phaseStartedAt": None,
                "message": f"Attempt {req.attempt} failed; retrying in {int(delay)}s", "_release": True}
    if outcome == "succeeded":
        return {"phase": "Succeeded", "finishedAt": ts(now), "reason": "Succeeded", "nextAttemptAt": None,
                "message": f"Backup completed (attempt {req.attempt})", "_release": True}
    return {"phase": "Failed", "finishedAt": ts(now), "nextAttemptAt": None, "_release": True}


STEPS = {
    "Pending": step_pending,
    "Snapshotting": step_snapshotting,
    "Cloning": step_cloning,
    "Running": step_running,
    "Recording": step_recording,
    "CleaningUp": step_cleaning,
}
