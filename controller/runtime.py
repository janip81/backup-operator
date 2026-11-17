"""Process-wide singletons: the API client and admission registry."""
import logging
import threading
import time

from . import config as cfg
from .engine import Admission
from .kube import Kube

log = logging.getLogger("backup-operator.runtime")
ADMISSION = Admission()
_KUBE = None
_boot_lock = threading.Lock()
_last_boot = 0.0


def kube() -> Kube:
    global _KUBE
    if _KUBE is None:
        _KUBE = Kube()
    return _KUBE


def ensure_bootstrapped() -> bool:
    """Try the admission bootstrap at most every BOOTSTRAP_RETRY seconds until it succeeds.
    Until then no new run is admitted; active runs keep reconciling."""
    global _last_boot
    if ADMISSION.ready:
        return True
    with _boot_lock:
        if ADMISSION.ready or time.monotonic() - _last_boot < cfg.BOOTSTRAP_RETRY:
            return ADMISSION.ready
        _last_boot = time.monotonic()
        try:
            ADMISSION.bootstrap(kube().list_crs("backuprequests"))
            log.info("Admission bootstrap: %d active run(s)", ADMISSION.active_count())
        except Exception as e:  # noqa: BLE001
            log.warning("Admission bootstrap failed; new runs stay queued: %s", e)
    return ADMISSION.ready
