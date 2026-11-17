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


# All VolumeSnapshots, not only the operator's: leftovers without our labels (older versions, manual
# runs) must still show up in the inventory and alerts.
@kopf.on.event("snapshot.storage.k8s.io", "v1", "volumesnapshots")
def snapshot_event(event, body, **_):
    md = body["metadata"]
    pvc = ((body.get("spec") or {}).get("source") or {}).get("persistentVolumeClaimName") or "(pre-provisioned)"
    owned = (md.get("labels") or {}).get("app.kubernetes.io/managed-by") == "backup-operator"
    metrics.snapshot_event(md["namespace"], md["name"], pvc=pvc, owned=owned,
                           created=_epoch(md.get("creationTimestamp")) or 0.0,
                           deleting=_epoch(md.get("deletionTimestamp")),
                           exists=event.get("type") != "DELETED")


@kopf.on.event("snapshot.storage.k8s.io", "v1", "volumesnapshotcontents")
def snapshot_content_event(event, body, **_):
    md = body["metadata"]
    ref = (body.get("spec") or {}).get("volumeSnapshotRef") or {}
    metrics.content_event(md["name"], f"{ref.get('namespace')}/{ref.get('name')}",
                          created=_epoch(md.get("creationTimestamp")) or 0.0,
                          exists=event.get("type") != "DELETED")


@kopf.on.event(cfg.GROUP, cfg.VERSION, "backuprequests")
def request_event(event, body, **_):
    """Counts every request, including v0.3 ones the controller no longer handles, so a pile-up shows."""
    md = body["metadata"]
    key = f"{md['namespace']}/{md['name']}"
    if event.get("type") == "DELETED":
        metrics.request_event(key, None)
        return
    if "repository" not in (body.get("spec") or {}):
        phase = "Legacy"
    else:
        phase = (body.get("status") or {}).get("phase") or "Pending"
    metrics.request_event(key, phase, created=_epoch(md.get("creationTimestamp")) or 0.0)
