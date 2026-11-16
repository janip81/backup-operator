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

import logging
import random
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kubernetes.client.exceptions import ApiException

from . import config as cfg
from . import jobspec
from .kube import TransientApiError
from .metrics import backup_duration, backup_failures, backup_total

log = logging.getLogger("backup-operator.engine")

TERMINAL = ("Succeeded", "Failed")
ACTIVE = ("Snapshotting", "Cloning", "Running", "CleaningUp")
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

    @classmethod
    def from_body(cls, body: dict) -> Req:
        md = body["metadata"]
        return cls(md["name"], md["namespace"], md["uid"], dict(body.get("spec") or {}), dict(body.get("status") or {}))

    @property
    def key(self) -> str:
        return f"{self.namespace}/{self.name}"

    @property
    def src_ns(self) -> str:
        return self.spec.get("sourceNamespace") or self.namespace

    @property
    def src_pvc(self) -> str:
        return self.spec["sourcePVC"]

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
        return {"snapshot": f"{base}-snap", "pvc": f"{base}-pvc", "job": f"{base}-job",
                "secret": f"{base}-ssh", "configMap": f"{base}-script"}

    def labels(self) -> dict:
        return {
            MANAGED_BY[0]: MANAGED_BY[1],
            f"{LABEL_PREFIX}/request": self.name[:63],
            f"{LABEL_PREFIX}/request-uid": self.uid,
            f"{LABEL_PREFIX}/source-pvc": self.src_pvc[:63],
            f"{LABEL_PREFIX}/attempt": str(self.attempt),
        }


# --------------------------------------------------------------------------- reconcile
def reconcile(body: dict, kube, admission: Admission, now: datetime, rnd=random.random) -> dict | None:
    """Run one step for a BackupRequest. Returns a status patch, or None when nothing changed."""
    req = Req.from_body(body)
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
        if phase == "CleaningUp" or errors <= cfg.API_ERROR_BUDGET:
            # Cleanup never gives up and never changes the attempt's outcome: it backs off (capped) and retries.
            return {"apiErrors": errors, "lastError": str(e)[:500],
                    "nextAttemptAt": ts(now + timedelta(seconds=api_error_delay(errors, rnd)))}
        return _fail_attempt(req, phase, AttemptFailed("ApiErrors", f"Too many consecutive API errors: {e}"), now)
    except (AttemptFailed, ApiException) as e:
        if isinstance(e, ApiException):  # permanent API rejection (400/403/422): retrying won't help
            e = AttemptFailed("ApiRejected", f"HTTP {e.status}: {e.reason}", True)
        if phase == "CleaningUp":
            errors = int(req.status.get("apiErrors") or 0) + 1
            return {"reason": "CleanupStuck", "message": f"Cleanup blocked: {e.message}"[:1000], "apiErrors": errors,
                    "nextAttemptAt": ts(now + timedelta(seconds=api_error_delay(errors, rnd)))}
        return _fail_attempt(req, phase, e, now)


def _fail_attempt(req: Req, phase: str, err: AttemptFailed, now: datetime) -> dict:
    log.warning("%s: attempt %d failed (%s): %s", req.key, req.attempt, err.reason, err.message)
    retry = not err.terminal and req.attempt < req.max_attempts
    patch = {"outcome": "retry" if retry else "failed", "reason": err.reason, "message": err.message[:1000],
             "lastError": err.message[:500], "apiErrors": 0}
    if phase == "Pending":
        # Nothing was created for this attempt, so there's nothing to clean up.
        return {**patch, **_finalize(req, "retry" if retry else "failed", now)}
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

    backup_total.labels(namespace=req.src_ns, method=req.spec.get("transferMethod", "rsync")).inc()
    patch = {"phase": "Snapshotting", "phaseStartedAt": ts(now), "attempt": req.attempt,
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
    snap = kube.get_snapshot(req.src_ns, n["snapshot"])
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
    pvc = kube.get_pvc(req.src_ns, n["pvc"])
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
    job = kube.get_job(req.src_ns, n["job"])
    if job is None:
        jobspec.ensure_dependencies(req, n, kube)
        kube.create_job(req.src_ns, jobspec.worker_job(req, n))
        return None
    for c in (job.get("status") or {}).get("conditions") or []:
        if c.get("status") != "True":
            continue
        if c.get("type") == "Complete":
            return {"phase": "CleaningUp", "phaseStartedAt": ts(now), "outcome": "succeeded",
                    "reason": "CleaningUp", "message": "Worker job completed; removing temporary resources"}
        if c.get("type") == "Failed":
            raise AttemptFailed("JobFailed", f"Worker job failed: {c.get('reason')}: {c.get('message') or ''}")
    if _phase_timed_out(req, now, cfg.JOB_TIMEOUT + 300):
        raise AttemptFailed("JobTimeout", f"Worker job did not finish within {cfg.JOB_TIMEOUT}s")
    return None


def step_cleaning(req: Req, kube, admission: Admission, now: datetime) -> dict | None:
    n = req.names()
    ns = req.src_ns
    remaining = []
    for kind, getter, deleter in (
        ("job", kube.get_job, kube.delete_job),
        ("pvc", kube.get_pvc, kube.delete_pvc),
        ("snapshot", kube.get_snapshot, kube.delete_snapshot),
        ("secret", kube.get_secret, kube.delete_secret),
        ("configMap", kube.get_configmap, kube.delete_configmap),
    ):
        obj = getter(ns, n[kind])
        if obj is None:
            continue
        remaining.append(n[kind])
        if not (obj.get("metadata") or {}).get("deletionTimestamp"):
            deleter(ns, n[kind])
    if remaining:
        if _phase_timed_out(req, now, cfg.CLEANUP_TIMEOUT):
            msg = f"Cleanup still waiting for: {', '.join(remaining)}"
            if req.status.get("reason") != "CleanupStuck" or req.status.get("message") != msg:
                return {"reason": "CleanupStuck", "message": msg}
        return None
    return _finalize(req, req.status.get("outcome") or "failed", now, kube)


def _finalize(req: Req, outcome: str, now: datetime, kube=None) -> dict:
    admission_key = req.key
    if outcome == "retry":
        delay = retry_delay(req.attempt)
        return {"phase": "Pending", "attempt": req.attempt + 1, "outcome": None, "resources": None,
                "nextAttemptAt": ts(now + timedelta(seconds=delay)), "phaseStartedAt": None,
                "message": f"Attempt {req.attempt} failed; retrying in {int(delay)}s", "_release": admission_key}
    started = parse_ts(req.status.get("startedAt"))
    duration = (now - started).total_seconds() if started else None
    method = req.spec.get("transferMethod", "rsync")
    if outcome == "succeeded":
        if kube is not None:
            jobspec.record_backup(req, kube, now, duration)
        if duration is not None:
            backup_duration.labels(namespace=req.src_ns, method=method).observe(duration)
        return {"phase": "Succeeded", "finishedAt": ts(now), "reason": "Succeeded", "nextAttemptAt": None,
                "message": f"Backup completed (attempt {req.attempt})", "_release": admission_key}
    backup_failures.labels(namespace=req.src_ns, method=method, reason=req.status.get("reason") or "Failed").inc()
    return {"phase": "Failed", "finishedAt": ts(now), "nextAttemptAt": None, "_release": admission_key}


STEPS = {
    "Pending": step_pending,
    "Snapshotting": step_snapshotting,
    "Cloning": step_cloning,
    "Running": step_running,
    "CleaningUp": step_cleaning,
}
