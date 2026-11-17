"""Metrics rebuilt from cluster state (#11): kopf replays every existing object as an event at startup,
so last-success/failure times and inventories survive operator restarts. A periodic full relist
corrects inventories for DELETED events missed while a watch reconnected. Read-only: no handler here
patches the objects it watches."""
import logging
import threading
import time
from datetime import datetime

import kopf

from .. import config as cfg
from .. import metrics
from ..runtime import kube

log = logging.getLogger("backup-operator.inventory")
_STOP = threading.Event()


def _epoch(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def record_fields(body: dict) -> dict | None:
    """v0.4 records only (they reference their request); v0.3 records are history from another storage
    format and are not counted."""
    spec = body.get("spec") or {}
    if not (spec.get("requestRef") or {}).get("uid"):
        return None
    status = body.get("status") or {}
    md = body["metadata"]
    return {"record": f"{md['namespace']}/{md['name']}",
            "namespace": spec.get("sourceNamespace") or md["namespace"],
            "source": status.get("source") or spec.get("source") or md["name"],
            "phase": status.get("phase") or "",
            "completed_ts": _epoch(status.get("completedAt"))}


@kopf.on.event(cfg.GROUP, cfg.VERSION, "backups")
def backup_event(event, body, **_):
    f = record_fields(body)
    if f is None:
        return
    if event.get("type") == "DELETED":
        f["completed_ts"] = None
    metrics.record_event(**f)


def snapshot_fields(body: dict) -> dict:
    md = body["metadata"]
    return {"namespace": md["namespace"], "name": md["name"],
            "pvc": ((body.get("spec") or {}).get("source") or {}).get("persistentVolumeClaimName")
            or "(pre-provisioned)",
            "owned": (md.get("labels") or {}).get("app.kubernetes.io/managed-by") == "backup-operator",
            "created": _epoch(md.get("creationTimestamp")) or 0.0,
            "deleting": _epoch(md.get("deletionTimestamp"))}


def content_fields(body: dict) -> dict:
    ref = (body.get("spec") or {}).get("volumeSnapshotRef") or {}
    return {"name": body["metadata"]["name"], "snapshot_ref": f"{ref.get('namespace')}/{ref.get('name')}",
            "created": _epoch(body["metadata"].get("creationTimestamp")) or 0.0}


def request_fields(body: dict) -> dict:
    """v0.3 requests (no spec.repository) are counted as Legacy so a pile-up of them still shows."""
    md = body["metadata"]
    if "repository" not in (body.get("spec") or {}):
        phase = "Legacy"
    else:
        phase = (body.get("status") or {}).get("phase") or "Pending"
    return {"key": f"{md['namespace']}/{md['name']}", "phase": phase,
            "created": _epoch(md.get("creationTimestamp")) or 0.0}


# All VolumeSnapshots, not only the operator's: leftovers without our labels (older versions, manual
# runs) must still show up in the inventory and alerts. Event handlers never patch the objects.
@kopf.on.event("snapshot.storage.k8s.io", "v1", "volumesnapshots")
def snapshot_event(event, body, **_):
    metrics.snapshot_event(**snapshot_fields(body), exists=event.get("type") != "DELETED")


@kopf.on.event("snapshot.storage.k8s.io", "v1", "volumesnapshotcontents")
def snapshot_content_event(event, body, **_):
    metrics.content_event(**content_fields(body), exists=event.get("type") != "DELETED")


@kopf.on.event(cfg.GROUP, cfg.VERSION, "backuprequests")
def request_event(event, body, **_):
    f = request_fields(body)
    metrics.request_event(f["key"], None if event.get("type") == "DELETED" else f["phase"], f["created"])
    spec = body.get("spec") or {}
    if f["phase"] != "Legacy" and event.get("type") != "DELETED":
        labels = body["metadata"].get("labels") or {}
        src_ns = spec.get("sourceNamespace") or body["metadata"]["namespace"]
        metrics.init_source(src_ns, labels.get(f"{cfg.GROUP}/schedule") or spec.get("sourcePVC", ""),
                            spec.get("sourcePVC"))


def resync_inventory(kube, now: float) -> None:
    since = metrics.resync_begin()
    snaps = kube.list_all("snapshot.storage.k8s.io", "v1", "volumesnapshots")
    contents = kube.list_all("snapshot.storage.k8s.io", "v1", "volumesnapshotcontents")
    requests = kube.list_all(cfg.GROUP, cfg.VERSION, "backuprequests")
    metrics.resync([snapshot_fields(b) for b in snaps], [content_fields(b) for b in contents],
                   [request_fields(b) for b in requests], now, since)


def _resync_loop(stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            resync_inventory(kube(), time.time())
        except Exception as e:  # noqa: BLE001 - metrics must never stop the operator
            log.warning("Inventory resync failed, retrying: %s", e)
            stop.wait(60)
            continue
        stop.wait(cfg.INVENTORY_RESYNC)


@kopf.on.startup()
def start_inventory_resync(**_):
    threading.Thread(target=_resync_loop, args=(_STOP,), name="inventory-resync", daemon=True).start()


@kopf.on.cleanup()
def stop_inventory_resync(**_):
    _STOP.set()
