"""kopf wiring for BackupRequest: a periodic, level-triggered reconcile (see controller/engine.py)."""
from datetime import UTC, datetime

import kopf

from .. import config as cfg
from .. import engine
from ..runtime import ADMISSION, kube


@kopf.on.startup()
def bootstrap_admission(logger, **_):
    """Register runs that were active before the restart. If this fails, the startup grace period
    (ADMISSION_DELAY_SECONDS) still lets active runs re-register before anything new is admitted."""
    try:
        ADMISSION.bootstrap(kube().list_crs("backuprequests"))
        logger.info(f"Admission bootstrap: {ADMISSION.active_count()} active run(s)")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Admission bootstrap failed, relying on the startup grace period: {e}")


@kopf.timer(cfg.GROUP, cfg.VERSION, "backuprequests", interval=cfg.RECONCILE_INTERVAL, sharp=False)
def reconcile_request(body, patch, logger, **_):
    status_patch = engine.reconcile(dict(body), kube(), ADMISSION, datetime.now(UTC))
    if status_patch:
        for k, v in status_patch.items():
            patch.status[k] = v
        logger.debug(f"status -> {status_patch.get('phase', '(unchanged phase)')}: {status_patch.get('message', '')}")
