"""kopf wiring for BackupRequest: a periodic, level-triggered reconcile (see controller/engine.py)."""
from datetime import UTC, datetime

import kopf

from .. import config as cfg
from .. import engine
from ..runtime import ADMISSION, ensure_bootstrapped, kube


@kopf.on.startup()
def bootstrap_admission(**_):
    ensure_bootstrapped()


@kopf.timer(cfg.GROUP, cfg.VERSION, "backuprequests", interval=cfg.RECONCILE_INTERVAL, sharp=False)
def reconcile_request(body, patch, logger, **_):
    ensure_bootstrapped()
    status_patch = engine.reconcile(dict(body), kube(), ADMISSION, datetime.now(UTC))
    if status_patch:
        for k, v in status_patch.items():
            patch.status[k] = v
        logger.debug(f"status -> {status_patch.get('phase', '(unchanged phase)')}: {status_patch.get('message', '')}")


CLEANUP_GIVE_UP = 3600  # seconds


@kopf.on.delete(cfg.GROUP, cfg.VERSION, "backuprequests", backoff=15)
def cleanup_on_delete(body, logger, runtime, **_):
    """Finalizer: a deleted request removes every temporary object any of its attempts created and
    waits until they are gone. After CLEANUP_GIVE_UP seconds it stops waiting explicitly (so the
    request can go away); leftovers keep their ownership labels and stay visible to the snapshot
    metrics and alerts."""
    req = engine.Req.from_body(dict(body))
    try:
        remaining = engine.cleanup_all(req, kube())
    except Exception as e:  # noqa: BLE001 - API errors count against the same bound
        if runtime.total_seconds() < CLEANUP_GIVE_UP:
            raise kopf.TemporaryError(f"Cleanup failed, retrying: {e}", delay=30) from e
        logger.error(f"Giving up cleanup after {CLEANUP_GIVE_UP}s of errors: {e}")
        return
    if remaining and runtime.total_seconds() < CLEANUP_GIVE_UP:
        raise kopf.TemporaryError(f"Waiting for cleanup of: {', '.join(remaining)}", delay=15)
    if remaining:
        logger.error(f"Giving up waiting for cleanup after {CLEANUP_GIVE_UP}s; left behind: {', '.join(remaining)}")
    ADMISSION.release(req.key)
    logger.info("Temporary objects removed")
