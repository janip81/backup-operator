"""Process-wide singletons: the API client and admission registry."""
from datetime import UTC, datetime

from .engine import Admission
from .kube import Kube

ADMISSION = Admission(started_at=datetime.now(UTC))
_KUBE = None


def kube() -> Kube:
    global _KUBE
    if _KUBE is None:
        _KUBE = Kube()
    return _KUBE
