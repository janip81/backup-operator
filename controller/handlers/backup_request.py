"""kopf wiring for BackupRequest: a periodic, level-triggered reconcile (see controller/engine.py)."""
from datetime import UTC, datetime

import kopf

from .. import config as cfg
from .. import engine
from ..runtime import ADMISSION, kube


@kopf.timer(cfg.GROUP, cfg.VERSION, "backuprequests", interval=cfg.RECONCILE_INTERVAL, sharp=False)
def reconcile_request(body, patch, logger, **_):
    status_patch = engine.reconcile(dict(body), kube(), ADMISSION, datetime.now(UTC))
    if status_patch:
        for k, v in status_patch.items():
            patch.status[k] = v
        logger.debug(f"status -> {status_patch.get('phase', '(unchanged phase)')}: {status_patch.get('message', '')}")
