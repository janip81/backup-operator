"""Metrics rebuilt from cluster state (#11): kopf replays every existing object as an event at startup,
so last-success/failure times and owned-snapshot counts survive operator restarts."""
from datetime import datetime

import kopf

from .. import config as cfg
from .. import metrics


def _epoch(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


@kopf.on.event(cfg.GROUP, cfg.VERSION, "backups")
def backup_event(event, body, **_):
    if event.get("type") == "DELETED":
        return
    status = body.get("status") or {}
    completed = _epoch(status.get("completedAt"))
    if completed is None:
        return
    source = status.get("source") or (body.get("spec") or {}).get("source") \
        or (body.get("spec") or {}).get("targetNamespace") or body["metadata"]["name"]
    ns = (body.get("spec") or {}).get("sourceNamespace") or body["metadata"]["namespace"]
    metrics.observe_result(ns, source, status.get("phase") or "", completed)


@kopf.on.event("snapshot.storage.k8s.io", "v1", "volumesnapshots",
               labels={"app.kubernetes.io/managed-by": "backup-operator"})
def snapshot_event(event, body, **_):
    md = body["metadata"]
    metrics.snapshot_event(md["namespace"], md["name"], exists=event.get("type") != "DELETED")
