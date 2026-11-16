"""Object builders for one backup attempt: snapshot, temporary PVC, worker Job and its dependencies.

All objects get the attempt's deterministic name and the request's ownership labels, so retries and
cleanup can find them without guessing (#8, #10).
"""
from __future__ import annotations

import re

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


PLUGIN_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9]*$")
CREDS_DIR = "/etc/backup/creds"


def _plugin(req) -> dict:
    return req.spec.get("plugin") or {"name": "files"}


def plugin_env(req) -> dict:
    """plugin.config {pathsToKeep: [a, b]} -> PLUGIN_PATHS_TO_KEEP=a,b (values are data, never shell)."""
    env = {}
    for key, value in (_plugin(req).get("config") or {}).items():
        if not PLUGIN_KEY.match(key):
            from .engine import AttemptFailed
            raise AttemptFailed("InvalidPluginConfig", f"invalid plugin config key: {key!r}", terminal=True)
        name = "PLUGIN_" + re.sub(r"(?<!^)(?=[A-Z])", "_", key).upper()
        if isinstance(value, list):
            value = ",".join(str(v) for v in value)
        elif isinstance(value, bool):
            value = "true" if value else "false"
        env[name] = str(value)
    return env


def ensure_dependencies(req, n: dict, kube) -> None:
    """Validate what the worker will mount from the source namespace. Nothing is copied (#10, #13):
    the credentials Secret and a custom plugin's ConfigMap are owned by the application namespace."""
    from .engine import AttemptFailed
    repo = req.spec.get("repository") or {}
    secret = kube.get_secret(req.src_ns, repo.get("credentialsSecret", ""))
    if not secret or "RESTIC_PASSWORD" not in (secret.get("data") or {}):
        raise AttemptFailed("CredentialsNotFound",
                            f"Secret {req.src_ns}/{repo.get('credentialsSecret')} with key RESTIC_PASSWORD not found",
                            terminal=True)
    plugin = _plugin(req)
    if plugin.get("name") == "custom":
        ref = plugin.get("configMapRef") or {}
        cm = kube.get_configmap(req.src_ns, ref.get("name", ""))
        if not cm or ref.get("key", "plugin.sh") not in (cm.get("data") or {}):
            raise AttemptFailed("PluginNotFound", f"ConfigMap {req.src_ns}/{ref.get('name')} with the plugin script "
                                f"key {ref.get('key', 'plugin.sh')!r} not found", terminal=True)
    plugin_env(req)  # validates config keys


def worker_security_context(req) -> dict:
    """Default: root with every capability dropped except DAC_READ_SEARCH (read any file on the snapshot
    clone, write nothing outside its scratch volumes). With spec.worker.runAsUser the worker runs as that
    non-root user with no capabilities at all (Pod Security "restricted"), for volumes readable by it."""
    base = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "privileged": False}
    worker = req.spec.get("worker") or {}
    if worker.get("runAsUser") is not None:
        return {**base, "runAsNonRoot": True, "runAsUser": int(worker["runAsUser"]),
                "runAsGroup": int(worker.get("runAsGroup", worker["runAsUser"])),
                "capabilities": {"drop": ["ALL"]}}
    return {**base, "runAsUser": 0, "runAsGroup": 0, "runAsNonRoot": False,
            "capabilities": {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}}


def worker_job(req, n: dict) -> dict:
    labels = req.labels()
    plugin = _plugin(req)
    repo = req.spec.get("repository") or {}
    retention = req.spec.get("retention") or {}
    env = {
        "RESTIC_REPOSITORY": repo["url"],
        "INIT_REPOSITORY": "true" if repo.get("initialize", True) else "false",
        "ALLOW_INSECURE": "true" if repo.get("allowInsecure") else "false",
        "BACKUP_SOURCE": req.source,
        "BACKUP_HOST": req.src_ns,
        "BACKUP_REQUEST": req.name,
        "PLUGIN": plugin.get("name", "files"),
        "VERIFY": "true" if req.spec.get("verify") else "false",
        "SOURCE_DIR": "/source",
        "OUTPUT_DIR": "/output",
        "CREDS_DIR": CREDS_DIR,
        **{k: str(retention[f]) for f, k in (("keepLast", "KEEP_LAST"), ("keepDaily", "KEEP_DAILY"),
                                             ("keepWeekly", "KEEP_WEEKLY"), ("keepMonthly", "KEEP_MONTHLY"))
           if retention.get(f) is not None},
        **plugin_env(req),
    }
    mounts = [
        {"name": "source", "mountPath": "/source", "readOnly": True},
        {"name": "output", "mountPath": "/output"},
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "creds", "mountPath": CREDS_DIR, "readOnly": True},
    ]
    volumes = [
        {"name": "source", "persistentVolumeClaim": {"claimName": n["pvc"], "readOnly": True}},
        {"name": "output", "emptyDir": {"sizeLimit": cfg.WORKER_OUTPUT_LIMIT}},
        {"name": "tmp", "emptyDir": {"sizeLimit": cfg.WORKER_TMP_LIMIT}},
        {"name": "creds", "secret": {"secretName": repo["credentialsSecret"], "defaultMode": 0o400}},
    ]
    if plugin.get("name") == "custom":
        ref = plugin.get("configMapRef") or {}
        mounts.append({"name": "plugin", "mountPath": "/plugin-custom", "readOnly": True})
        volumes.append({"name": "plugin", "configMap": {"name": ref["name"], "defaultMode": 0o444,
                                                        "items": [{"key": ref.get("key", "plugin.sh"),
                                                                   "path": "plugin.sh"}]}})
    pod_spec = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,  # workers never talk to the Kubernetes API
        "enableServiceLinks": False,
        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{
            "name": "worker",
            "image": cfg.WORKER_IMAGE,
            "args": ["backup"],
            # The worker reports its JSON summary here; logs are never copied into records (#11, #13).
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
            "env": [{"name": k, "value": v} for k, v in env.items()],
            "volumeMounts": mounts,
            "resources": cfg.WORKER_RESOURCES,
            "securityContext": worker_security_context(req),
        }],
        "volumes": volumes,
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


def backup_record(req, name: str) -> dict:
    """The Backup record (spec only; the result goes into status)."""
    labels = {k: v for k, v in req.labels().items() if not k.endswith("/attempt")}
    labels["backup.techmonkeys.se/source"] = req.source[:63]
    return {
        "apiVersion": f"{cfg.GROUP}/{cfg.VERSION}",
        "kind": "Backup",
        "metadata": {"name": name, "namespace": req.namespace, "labels": labels},
        "spec": {"requestRef": {"name": req.name, "namespace": req.namespace, "uid": req.uid},
                 "source": req.source, "sourceNamespace": req.src_ns, "sourcePVC": req.src_pvc,
                 "plugin": _plugin(req).get("name", "files"),
                 "repository": (req.spec.get("repository") or {}).get("url")},
    }
