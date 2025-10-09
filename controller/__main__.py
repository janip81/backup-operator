#!/usr/bin/env python3
import kopf
import subprocess
import json
import time
import yaml
import os
import sys
import builtins
import random
import string
import textwrap
from datetime import datetime
from prometheus_client import Counter, Histogram, start_http_server

# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------
def log(*args, **kwargs):
    builtins.print(*args, **kwargs, flush=True)
print = log

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
WORKER_IMAGE       = os.getenv("WORKER_IMAGE", "ghcr.io/janip81/backup-operator-worker:latest")
SERVICE_ACCOUNT    = os.getenv("SERVICE_ACCOUNT", "backup-runner")
IMAGE_PULL_SECRET  = os.getenv("IMAGE_PULL_SECRET", "ghcr-creds")
DEFAULT_STORAGE    = os.getenv("DEFAULT_STORAGE_CLASS", "vsphere-csi")
MOUNT_PATH         = os.getenv("MOUNT_PATH", "/mnt/source")
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "backup-operator")

# -------------------------------------------------------------------
# PROMETHEUS METRICS
# -------------------------------------------------------------------
backup_total = Counter("backup_operator_backups_total", "Total backups started", ["namespace", "method"])
backup_failures = Counter("backup_operator_backups_failed_total", "Failed backups", ["namespace", "method", "reason"])
backup_duration = Histogram("backup_operator_backup_duration_seconds", "Duration of backup jobs", ["namespace", "method"])
backup_job_total = Counter("backup_operator_backups_job_total", "Total backups per job", ["namespace", "method", "job"])
backup_job_duration = Histogram("backup_operator_backups_job_duration_seconds", "Backup duration per job", ["namespace", "method", "job"])

# -------------------------------------------------------------------
# UTILS
# -------------------------------------------------------------------
def run(cmd, input=None, capture=True, check=True, ignore_not_found=False):
    """Run kubectl and return stdout; handle 'NotFound' gracefully when requested."""
    if cmd and cmd[0] == "kubectl":
        print("[CMD]", " ".join(cmd))
    try:
        result = subprocess.run(cmd, text=True, input=input,
                                capture_output=capture, check=check)
        if capture:
            if result.stdout.strip():
                print(result.stdout.strip())
            if result.stderr.strip():
                print(result.stderr.strip(), file=sys.stderr)
            return result.stdout.strip()
        return ""
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip() if e.stderr else ""
        if ignore_not_found and "NotFound" in stderr:
            print(f"[WARN] Resource not found for command: {' '.join(cmd)}")
            return None
        print(f"[ERROR] Command failed: {' '.join(cmd)}")
        if e.stdout:
            print("[STDOUT]", e.stdout.strip())
        if e.stderr:
            print("[STDERR]", e.stderr.strip(), file=sys.stderr)
        raise

def rand_suffix(n=5):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def common_labels(job, req_name, ts):
    return {
        "app.kubernetes.io/created-by": "backup-operator",
        "backup.techmonkeys.se/job": job,
        "backup.techmonkeys.se/request": req_name,
        "backup.techmonkeys.se/timestamp": ts,
    }

def labels_yaml(job, req_name, ts, indent=2):
    labels = common_labels(job, req_name, ts)
    pad = " " * indent
    return "\n".join([f"{pad}{k}: {v}" for k, v in labels.items()])

def wait_for_snapshot(ns, name, timeout=600):
    print(f"[INFO] Waiting for snapshot {ns}/{name}...")
    start = time.time()
    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "volumesnapshot", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"Snapshot {name} was deleted during wait")
        if json.loads(out).get("status", {}).get("readyToUse"):
            print(f"[INFO] Snapshot {name} ready.")
            return True
        time.sleep(5)
    raise TimeoutError(f"Snapshot {name} not ready after {timeout}s")

def wait_for_pvc_bound(ns, name, timeout=300):
    print(f"[INFO] Waiting for PVC {ns}/{name} to Bind...")
    start = time.time()

    pvc_json = run(["kubectl", "-n", ns, "get", "pvc", name, "-o", "json"], ignore_not_found=True)
    if not pvc_json:
        raise FileNotFoundError(f"PVC {name} was deleted before binding")
    pvc = json.loads(pvc_json)
    sc_name = pvc.get("spec", {}).get("storageClassName")

    if sc_name:
        sc_json = run(["kubectl", "get", "storageclass", sc_name, "-o", "json"], ignore_not_found=True)
        if sc_json:
            sc = json.loads(sc_json)
            mode = sc.get("volumeBindingMode", "")
            if mode == "WaitForFirstConsumer":
                print(f"[INFO] StorageClass {sc_name} uses WaitForFirstConsumer — creating temporary pod to trigger binding.")
                pod_name = f"pvc-binder-{rand_suffix()}"
                binder_manifest = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {ns}
  labels:
    app: pvc-binder
spec:
  restartPolicy: Never
  containers:
  - name: binder
    image: busybox
    command: ["sleep", "10"]
    volumeMounts:
    - name: vol
      mountPath: /mnt
  volumes:
  - name: vol
    persistentVolumeClaim:
      claimName: {name}
"""
                run(["kubectl", "apply", "-f", "-"], input=binder_manifest)
                try:
                    while time.time() - start < timeout:
                        out = run(["kubectl", "-n", ns, "get", "pvc", name, "-o", "json"], ignore_not_found=True)
                        if not out:
                            raise FileNotFoundError(f"PVC {name} deleted during binding wait")
                        phase = json.loads(out).get("status", {}).get("phase")
                        if phase == "Bound":
                            print(f"[INFO] PVC {name} bound after pod trigger.")
                            break
                        time.sleep(2)
                finally:
                    run(["kubectl", "-n", ns, "delete", "pod", pod_name, "--ignore-not-found=true"])
                    print(f"[INFO] Temporary binder pod {pod_name} deleted.")

    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "pvc", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"PVC {name} was deleted during wait")
        if json.loads(out).get("status", {}).get("phase") == "Bound":
            print(f"[INFO] PVC {name} bound.")
            return True
        time.sleep(3)
    raise TimeoutError(f"PVC {name} not bound after {timeout}s")

def wait_for_job(ns, name, timeout=7200):
    print(f"[INFO] Waiting for Job {ns}/{name}...")
    start = time.time()
    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "job", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"Job {name} was deleted during wait")
        job = json.loads(out)
        for c in job.get("status", {}).get("conditions", []):
            if c.get("type") == "Complete" and c.get("status") == "True":
                print(f"[INFO] Job {name} succeeded.")
                return "Succeeded"
            if c.get("type") == "Failed" and c.get("status") == "True":
                print(f"[ERROR] Job {name} failed.")
                return "Failed"
        time.sleep(5)
    raise TimeoutError(f"Job {name} timeout after {timeout}s")

def get_pvc_size(ns, pvc):
    out = run(["kubectl", "-n", ns, "get", "pvc", pvc, "-o", "json"])
    return json.loads(out)["spec"]["resources"]["requests"]["storage"]

def update_status(namespace, name, phase, message):
    patch = {
        "status": {
            "phase": phase,
            "message": message,
            "updatedAt": datetime.utcnow().isoformat() + "Z",
        }
    }
    try:
        run([
            "kubectl", "-n", namespace,
            "patch", "backuprequest", name,
            "--type=merge", "-p", json.dumps(patch)
        ])
        print(f"[INFO] Status updated: {phase} - {message}")
    except Exception as e:
        print(f"[WARN] Could not patch status: {e}")

def ensure_rbac(ns, job="unknown", req="unknown", ts=None):
    if not ts:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    manifest = f"""
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {SERVICE_ACCOUNT}
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {SERVICE_ACCOUNT}-role
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
rules:
  - apiGroups: [""]
    resources: ["pods","pods/log","persistentvolumeclaims","configmaps","secrets"]
    verbs: ["get","list","watch","create","patch","update","delete"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get","list","create","delete","watch"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshots"]
    verbs: ["get","list","create","delete","watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {SERVICE_ACCOUNT}-binding
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
subjects:
- kind: ServiceAccount
  name: {SERVICE_ACCOUNT}
  namespace: {ns}
roleRef:
  kind: Role
  name: {SERVICE_ACCOUNT}-role
  apiGroup: rbac.authorization.k8s.io
"""
    run(["kubectl", "apply", "-f", "-"], input=manifest)
    print(f"[INFO] RBAC ensured in {ns}")

def copy_secret(name, src_ns, dst_ns):
    try:
        src_yaml = run(["kubectl", "-n", src_ns, "get", "secret", name, "-o", "yaml"])
        data = yaml.safe_load(src_yaml)
        data["metadata"]["namespace"] = dst_ns
        for f in ["resourceVersion","uid","creationTimestamp","managedFields"]:
            data["metadata"].pop(f, None)
        run(["kubectl", "apply", "-f", "-"], input=yaml.safe_dump(data, sort_keys=False))
        print(f"[INFO] Secret {name} copied {src_ns} → {dst_ns}")
    except subprocess.CalledProcessError:
        print(f"[WARN] Secret {name} not found in {src_ns}")

def copy_configmap(cm, key, src_ns, dst_ns):
    src_yaml = run(["kubectl", "-n", src_ns, "get", "configmap", cm, "-o", "yaml"])
    data = yaml.safe_load(src_yaml)
    data["metadata"]["namespace"] = dst_ns
    data["metadata"]["name"] = cm
    for f in ["resourceVersion","uid","creationTimestamp","managedFields"]:
        data["metadata"].pop(f, None)
    run(["kubectl", "apply", "-f", "-"], input=yaml.safe_dump(data, sort_keys=False))
    print(f"[INFO] ConfigMap {cm}:{key} copied {src_ns} → {dst_ns}")
    return cm

def get_job_logs(ns, job, tail=100):
    try:
        pods_json = run(["kubectl", "-n", ns, "get", "pods", "-l", f"job-name={job}", "-o", "json"])
        pods = json.loads(pods_json).get("items", [])
        if not pods:
            return "[WARN] No pods found for job."
        pod = pods[0]["metadata"]["name"]
        print(f"[INFO] Fetching logs from {pod}...")
        logs = run(["kubectl", "-n", ns, "logs", pod, "--tail", str(tail)], capture=True, check=False)
        return logs.strip() or "[INFO] (no logs)"
    except Exception as e:
        return f"[WARN] Could not get logs: {e}"

def cleanup(ns, *resources):
    for r in resources:
        run(["kubectl", "-n", ns, "delete", r, "--ignore-not-found=true"])

# -------------------------------------------------------------------
# KOPF HANDLERS
# -------------------------------------------------------------------
@kopf.on.startup()
def startup(logger, **_):
    try:
        start_http_server(8080)
        logger.info("[DEBUG] Metrics endpoint started on :8080/metrics")
    except Exception as e:
        logger.warning(f"[WARN] Could not enable Kopf metrics, fallback to manual: {e}")
    logger.info("[DEBUG] Kopf startup complete; watching BackupRequest events.")

@kopf.on.create("backup.techmonkeys.se", "v1", "backuprequests")
@kopf.on.resume("backup.techmonkeys.se", "v1", "backuprequests")
def handle_backup(spec, name, namespace, **_):
    start = time.time()
    print(f"\n[INFO] === Handling BackupRequest {namespace}/{name} ===")

    src_ns = spec["sourceNamespace"]
    src_pvc = spec["sourcePVC"]
    snapshot_class = spec["snapshotClass"]
    storage_class = spec.get("storageClass", DEFAULT_STORAGE)
    remote_path = spec["remotePath"]
    method = spec.get("transferMethod", "rsync").lower()
    source_path = spec.get("sourcePath", "/mnt/source/backups")
    custom_script = spec.get("customScript", {"preset": "rsync-snapshot-default-backup-script", "key": "backup.sh"})

    app = name
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    suffix = rand_suffix()

    snapshot = f"{app}-snap-{ts}-{suffix}"
    pvc_backup = f"{app}-backup-{ts}-{suffix}"
    job = f"backup-{app}-{ts}-{suffix}"

    backup_total.labels(namespace=src_ns, method=method).inc()
    backup_job_total.labels(namespace=src_ns, method=method, job=job).inc()

    try:
        update_status(namespace, name, "Starting", "Initializing backup process")
        ensure_rbac(src_ns, job, name, ts)
        copy_secret(IMAGE_PULL_SECRET, OPERATOR_NAMESPACE, src_ns)
        copy_secret("backup-ssh-key", OPERATOR_NAMESPACE, src_ns)

        # Snapshot creation
        update_status(namespace, name, "Snapshotting", f"Creating snapshot {snapshot}")
        run(["kubectl", "-n", src_ns, "apply", "-f", "-"], input=f"""
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: {snapshot}
  labels:
{labels_yaml(job, name, ts, indent=4)}
spec:
  volumeSnapshotClassName: {snapshot_class}
  source:
    persistentVolumeClaimName: {src_pvc}
""")
        try:
            wait_for_snapshot(src_ns, snapshot)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"volumesnapshot/{snapshot}")
            return
        update_status(namespace, name, "SnapshotReady", f"Snapshot {snapshot} ready")

        # Clone PVC
        size = get_pvc_size(src_ns, src_pvc)
        update_status(namespace, name, "Cloning", f"Creating PVC {pvc_backup} from snapshot")
        run(["kubectl", "-n", src_ns, "apply", "-f", "-"], input=f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {pvc_backup}
  labels:
{labels_yaml(job, name, ts, indent=4)}
spec:
  storageClassName: {storage_class}
  dataSource:
    name: {snapshot}
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: {size}
""")
        try:
            wait_for_pvc_bound(src_ns, pvc_backup)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"volumesnapshot/{snapshot}", f"pvc/{pvc_backup}")
            return

        # ConfigMap
        cm = custom_script.get("preset") or "rsync-snapshot-default-backup-script"
        key = custom_script.get("key", "backup.sh")
        copy_configmap(cm, key, OPERATOR_NAMESPACE, src_ns)

        # Job
        update_status(namespace, name, "Running", f"Starting job {job}")
        run(["kubectl", "-n", src_ns, "apply", "-f", "-"], input=f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: {job}
  labels:
{labels_yaml(job, name, ts, indent=4)}
spec:
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
{labels_yaml(job, name, ts, indent=8)}
    spec:
      serviceAccountName: {SERVICE_ACCOUNT}
      restartPolicy: Never
      imagePullSecrets:
      - name: {IMAGE_PULL_SECRET}
      containers:
      - name: worker
        image: {WORKER_IMAGE}
        imagePullPolicy: Always
        command: ["/bin/bash", "-lc"]
        args: ["cp /opt/custom/{key} /tmp/{key} && chmod +x /tmp/{key} && /tmp/{key}"]
        env:
        - name: NAMESPACE
          value: "{src_ns}"
        - name: PVC_NAME
          value: "{src_pvc}"
        - name: SNAPSHOT_CLASS
          value: "{snapshot_class}"
        - name: STORAGE_CLASS
          value: "{storage_class}"
        - name: REMOTE_PATH
          value: "{remote_path}"
        - name: SOURCE_PATH
          value: "{source_path}"
        - name: TEMP_NAMESPACE
          value: "{namespace}"
        volumeMounts:
        - name: src
          mountPath: {MOUNT_PATH}
        - name: custom
          mountPath: /opt/custom
        - name: ssh
          mountPath: /tmp/ssh_key
          subPath: ssh_key
      volumes:
      - name: src
        persistentVolumeClaim:
          claimName: {pvc_backup}
      - name: custom
        configMap:
          name: {cm}
          items: [{{key: {key}, path: {key}}}]
      - name: ssh
        secret:
          secretName: backup-ssh-key
          items:
            - key: ssh_key
              path: ssh_key
              mode: 0644
""")

        try:
            result = wait_for_job(src_ns, job)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"pvc/{pvc_backup}", f"volumesnapshot/{snapshot}", f"configmap/{cm}")
            return

        update_status(namespace, name, result, f"Job {job} finished: {result}")
        log_tail = get_job_logs(src_ns, job, tail=100)

        # Cleanup
        update_status(namespace, name, "CleaningUp", "Removing temporary resources")
        cleanup(src_ns, f"job/{job}", f"pvc/{pvc_backup}", f"volumesnapshot/{snapshot}", f"configmap/{cm}", "secret/backup-ssh-key")

        duration = time.time() - start
        backup_duration.labels(namespace=src_ns, method=method).observe(duration)
        backup_job_duration.labels(namespace=src_ns, method=method, job=job).observe(duration)

        completed_at = datetime.utcnow().isoformat() + "Z"
        backup_name = f"{app}-{ts}-{suffix}"
        minutes, seconds = divmod(int(duration), 60)
        pretty_duration = f"{minutes}m{seconds}s"

        safe_log_tail = log_tail.replace("\r", "").replace("\t", "    ")
        safe_log_tail = "\n".join([line if line.strip() else "" for line in safe_log_tail.splitlines()])
        indented_log = textwrap.indent(safe_log_tail, "    ")

        backup_manifest = f"""
apiVersion: backup.techmonkeys.se/v1
kind: Backup
metadata:
  name: {backup_name}
  namespace: {src_ns}
  labels:
{labels_yaml(job, name, ts, indent=4)}
spec:
  requestRef:
    name: {name}
    namespace: {namespace}
  targetNamespace: {src_ns}
  method: {method}
  customScript:
    preset: {cm}
status:
  phase: "{result}"
  message: "Backup completed via operator"
  completedAt: "{completed_at}"
  duration: "{pretty_duration}"
  snapshotName: "{snapshot}"
  remotePath: "{remote_path}"
  size: "{size}"
  logTail: |
{indented_log}
"""
        run(["kubectl", "apply", "-f", "-"], input=backup_manifest)
        print(f"[DONE] ✅ Backup {backup_name} completed in {pretty_duration}")

    except Exception as e:
        reason = e.__class__.__name__
        backup_failures.labels(namespace=src_ns, method=method, reason=reason).inc()
        update_status(namespace, name, "Failed", str(e))
        print(f"[ERROR] Backup failed: {e}")
        raise
