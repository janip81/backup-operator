"""kopf wiring for BackupSchedule (see controller/scheduling.py)."""
import kopf

from .. import config as cfg
from .. import scheduling
from ..runtime import kube


@kopf.timer(cfg.GROUP, cfg.VERSION, "backupschedules", interval=cfg.SCHEDULE_TIMER_INTERVAL, sharp=False)
def reconcile_schedule(body, patch, **_):
    status_patch = scheduling.reconcile_schedule(dict(body), kube(), scheduling.now_utc())
    for k, v in (status_patch or {}).items():
        patch.status[k] = v
