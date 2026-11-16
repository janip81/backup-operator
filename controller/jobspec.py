"""Object builders for one backup attempt: snapshot, temporary PVC, worker Job and its dependencies.

All objects get the attempt's deterministic name and the request's ownership labels, so retries and
cleanup can find them without guessing (#8, #10).
"""
from __future__ import annotations

from . import config as cfg

SNAP_API = "snapshot.storage.k8s.io/v1"


def _meta(req, name: str) -> dict:
    return {"name": name, "namespace": req.src_ns, "labels": req.labels()}


def snapshot(req, n: dict) -> dict:
    return {
        "apiVersion": SNAP_API,
        "kind": "VolumeSnapshot",
        "metadata": _meta(req, n["snapshot"]),
        "spec": {"volumeSnapshotClassName": req.spec["snapshotClass"],
                 "source": {"persistentVolumeClaimName": req.src_pvc}},
    }


def clone_pvc(req, n: dict, size: str) -> dict:
    spec = {
        "dataSource": {"name": n["snapshot"], "kind": "VolumeSnapshot", "apiGroup": "snapshot.storage.k8s.io"},
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": size}},
    }
    storage_class = req.spec.get("storageClass") or cfg.DEFAULT_STORAGE
    if storage_class:
        spec["storageClassName"] = storage_class
    return {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": _meta(req, n["pvc"]), "spec": spec}


def _preset(req) -> tuple[str, str]:
    script = req.spec.get("customScript") or {}
    return script.get("preset") or "rsync-snapshot-default-backup-script", script.get("key") or "backup.sh"


def ensure_dependencies(req, n: dict, kube) -> None:
    """Copy the transfer script and SSH key into the source namespace under this attempt's own names.

    Copies are never shared between runs, so cleaning one run up can't break another (#10)."""
    preset, key = _preset(req)
    src_cm = kube.get_configmap(cfg.OPERATOR_NAMESPACE, preset)
    if not src_cm or key not in (src_cm.get("data") or {}):
        from .engine import AttemptFailed
        raise AttemptFailed("ScriptNotFound", f"Script preset {preset}:{key} not found", terminal=True)
    kube.create_configmap(req.src_ns, {"metadata": _meta(req, n["configMap"]),
                                       "data": {key: src_cm["data"][key]}})
    src_secret = kube.get_secret(cfg.OPERATOR_NAMESPACE, "backup-ssh-key")
    if not src_secret or "ssh_key" not in (src_secret.get("data") or {}):
        from .engine import AttemptFailed
        raise AttemptFailed("CredentialsNotFound", "Destination credentials not found", terminal=True)
    kube.create_secret(req.src_ns, {"metadata": _meta(req, n["secret"]), "type": "Opaque",
                                    "data": {"ssh_key": src_secret["data"]["ssh_key"]}})


def worker_job(req, n: dict) -> dict:
    _, key = _preset(req)
    labels = req.labels()
    env = {
        "NAMESPACE": req.src_ns,
        "PVC_NAME": req.src_pvc,
        "REMOTE_PATH": req.spec.get("remotePath", ""),
        "SOURCE_PATH": req.spec.get("sourcePath", ""),
        "MOUNT_PATH": cfg.MOUNT_PATH,
        "SSH_KEY_PATH": "/etc/backup/ssh/ssh_key",
    }
    pod_spec = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,  # workers never talk to the Kubernetes API
        "containers": [{
            "name": "worker",
            "image": cfg.WORKER_IMAGE,
            # The worker reports its JSON summary here; logs are never copied into records (#11, #13).
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
            "command": ["/bin/bash", f"/opt/custom/{key}"],
            "env": [{"name": k, "value": v} for k, v in env.items()],
            "volumeMounts": [
                {"name": "src", "mountPath": cfg.MOUNT_PATH, "readOnly": True},
                {"name": "custom", "mountPath": "/opt/custom", "readOnly": True},
                {"name": "ssh", "mountPath": "/etc/backup/ssh", "readOnly": True},
            ],
        }],
        "volumes": [
            {"name": "src", "persistentVolumeClaim": {"claimName": n["pvc"], "readOnly": True}},
            {"name": "custom", "configMap": {"name": n["configMap"]}},
            {"name": "ssh", "secret": {"secretName": n["secret"], "defaultMode": 0o400}},
        ],
    }
    if cfg.IMAGE_PULL_SECRET:
        pod_spec["imagePullSecrets"] = [{"name": cfg.IMAGE_PULL_SECRET}]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _meta(req, n["job"]),
        "spec": {
            "backoffLimit": 0,  # retries are owned by the operator, not the Job controller
            "activeDeadlineSeconds": cfg.JOB_TIMEOUT,
            "template": {"metadata": {"labels": labels}, "spec": pod_spec},
        },
    }


def backup_record(req) -> dict:
    """The Backup record (spec only; the result goes into status). Named after the request: idempotent."""
    labels = {k: v for k, v in req.labels().items() if not k.endswith("/attempt")}
    labels["backup.techmonkeys.se/source"] = req.source[:63]
    return {
        "apiVersion": f"{cfg.GROUP}/{cfg.VERSION}",
        "kind": "Backup",
        "metadata": {"name": req.name, "namespace": req.namespace, "labels": labels},
        "spec": {"requestRef": {"name": req.name, "namespace": req.namespace, "uid": req.uid},
                 "source": req.source, "sourceNamespace": req.src_ns, "sourcePVC": req.src_pvc,
                 "method": req.spec.get("transferMethod", "rsync")},
    }
