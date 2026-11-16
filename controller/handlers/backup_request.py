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


@kopf.on.delete(cfg.GROUP, cfg.VERSION, "backuprequests", backoff=15, timeout=3600)
def cleanup_on_delete(body, logger, **_):
    """Finalizer: a deleted request removes every temporary object any of its attempts created.
    Waits (bounded by the handler timeout) until they are gone, then releases admission."""
    req = engine.Req.from_body(dict(body))
    remaining = engine.cleanup_all(req, kube())
    if remaining:
        raise kopf.TemporaryError(f"Waiting for cleanup of: {', '.join(remaining)}", delay=15)
    ADMISSION.release(req.key)
    logger.info("Temporary objects removed")
