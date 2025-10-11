import json
import textwrap
import time
from datetime import datetime

import kopf
import yaml

print("[DEBUG] importing", __name__)

from ..config import (
    WORKER_IMAGE,
    SERVICE_ACCOUNT,
    IMAGE_PULL_SECRET,
    DEFAULT_STORAGE,
    MOUNT_PATH,
    OPERATOR_NAMESPACE,
)
from ..metrics import (
    backup_total,
    backup_failures,
    backup_duration,
    backup_job_total,
    backup_job_duration,
)
from ..utils.kubectl import run, cleanup
from ..utils.waiters import wait_for_snapshot, wait_for_pvc_bound, wait_for_job
from ..utils.rbac import ensure_rbac, copy_secret, copy_configmap
from ..utils.status import update_status, labels_yaml
from ..utils.misc import rand_suffix, get_pvc_size, get_job_logs

@kopf.on.create("backup.techmonkeys.se", "v1", "backuprequests")
@kopf.on.resume("backup.techmonkeys.se", "v1", "backuprequests")
def handle_backup(spec, name, namespace, **_):
    """
    Main BackupRequest handler.
    Mirrors the original monolithic logic, now using helpers & config modules.
    """
    start = time.time()
    print(f"\n[INFO] === Handling BackupRequest {namespace}/{name} ===")

    # -------- Spec parsing --------
    src_ns = spec["sourceNamespace"]
    src_pvc = spec["sourcePVC"]
    snapshot_class = spec["snapshotClass"]
    storage_class = spec.get("storageClass", DEFAULT_STORAGE)
    remote_path = spec["remotePath"]
    method = spec.get("transferMethod", "rsync").lower()
    source_path = spec.get("sourcePath", "/mnt/source")
    custom_script = spec.get(
        "customScript",
        {"preset": "rsync-snapshot-default-backup-script", "key": "backup.sh"},
    )

    # -------- Names --------
    app = name
    ts_short = datetime.utcnow().strftime("%y%m%d-%H%M")  # YYMMDD-HHMM
    suffix = rand_suffix()
    base = app[:20]  # keep headroom for 63-char Kubernetes name limits

    snapshot = f"{base}-snap-{ts_short}-{suffix}"[:63]
    pvc_backup = f"{base}-pvc-{ts_short}-{suffix}"[:63]
    job = f"bkp-{base}-{ts_short}-{suffix}"[:63]

    # -------- Metrics: counters --------
    backup_total.labels(namespace=src_ns, method=method).inc()
    backup_job_total.labels(namespace=src_ns, method=method, job=job).inc()

    try:
        # -------- Status: Starting --------
        update_status(namespace, name, "Starting", "Initializing backup process")

        # -------- RBAC & secrets --------
        ts_long = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        ensure_rbac(src_ns, job, name, ts_long)

        # Copy required secrets from operator namespace → source namespace
        copy_secret(IMAGE_PULL_SECRET, OPERATOR_NAMESPACE, src_ns)
        copy_secret("backup-ssh-key", OPERATOR_NAMESPACE, src_ns)

        # -------- Create VolumeSnapshot --------
        update_status(namespace, name, "Snapshotting", f"Creating snapshot {snapshot}")
        run(
            ["kubectl", "-n", src_ns, "apply", "-f", "-"],
            input=f"""
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: {snapshot}
  labels:
{labels_yaml(job, name, ts_long, indent=4)}
spec:
  volumeSnapshotClassName: {snapshot_class}
  source:
    persistentVolumeClaimName: {src_pvc}
""",
        )

        try:
            wait_for_snapshot(src_ns, snapshot)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"volumesnapshot/{snapshot}")
            return

        update_status(namespace, name, "SnapshotReady", f"Snapshot {snapshot} ready")

        # -------- Clone PVC from snapshot --------
        size = get_pvc_size(src_ns, src_pvc)
        update_status(namespace, name, "Cloning", f"Creating PVC {pvc_backup} from snapshot")
        run(
            ["kubectl", "-n", src_ns, "apply", "-f", "-"],
            input=f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {pvc_backup}
  labels:
{labels_yaml(job, name, ts_long, indent=4)}
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
""",
        )

        try:
            wait_for_pvc_bound(src_ns, pvc_backup)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"volumesnapshot/{snapshot}", f"pvc/{pvc_backup}")
            return

        # -------- ConfigMap containing backup script --------
        cm = custom_script.get("preset") or "rsync-snapshot-default-backup-script"
        key = custom_script.get("key", "backup.sh")
        copy_configmap(cm, key, OPERATOR_NAMESPACE, src_ns)

        # -------- Create Job --------
        update_status(namespace, name, "Running", f"Starting job {job}")
        run(
            ["kubectl", "-n", src_ns, "apply", "-f", "-"],
            input=f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: {job}
  labels:
{labels_yaml(job, name, ts_long, indent=4)}
spec:
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
{labels_yaml(job, name, ts_long, indent=8)}
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
""",
        )

        try:
            result = wait_for_job(src_ns, job)
        except FileNotFoundError as e:
            update_status(namespace, name, "Failed", str(e))
            cleanup(src_ns, f"pvc/{pvc_backup}", f"volumesnapshot/{snapshot}", f"configmap/{cm}")
            return

        update_status(namespace, name, result, f"Job {job} finished: {result}")
        log_tail = get_job_logs(src_ns, job, tail=100)

        # -------- Cleanup temp resources --------
        update_status(namespace, name, "CleaningUp", "Removing temporary resources")
        cleanup(
            src_ns,
            f"job/{job}",
            f"pvc/{pvc_backup}",
            f"volumesnapshot/{snapshot}",
            f"configmap/{cm}",
            "secret/backup-ssh-key",
        )

        # -------- Metrics: histogram durations --------
        duration = time.time() - start
        backup_duration.labels(namespace=src_ns, method=method).observe(duration)
        backup_job_duration.labels(namespace=src_ns, method=method, job=job).observe(duration)

        # -------- Create Backup CR (spec only) + status patch --------
        completed_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        backup_name = f"{app}-{ts_short}-{suffix}"
        minutes, seconds = divmod(int(duration), 60)
        pretty_duration = f"{minutes}m{seconds}s"

        safe_log_tail = (log_tail or "").replace("\r", "").replace("\t", "    ")
        safe_log_tail = "\n".join([line if line.strip() else "" for line in safe_log_tail.splitlines()])
        indented_log = textwrap.indent(safe_log_tail, "    ")

        backup_manifest = f"""
apiVersion: backup.techmonkeys.se/v1
kind: Backup
metadata:
  name: {backup_name}
  namespace: {src_ns}
  labels:
{labels_yaml(job, name, ts_long, indent=4)}
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

        # Apply spec (without status) then patch status subresource
        backup_spec_full = yaml.safe_load(backup_manifest)
        backup_manifest_without_status = yaml.safe_dump(
            {
                "apiVersion": backup_spec_full["apiVersion"],
                "kind": backup_spec_full["kind"],
                "metadata": backup_spec_full["metadata"],
                "spec": backup_spec_full["spec"],
            }
        )

        run(["kubectl", "apply", "-f", "-"], input=backup_manifest_without_status)
        print(f"[INFO] Backup {backup_name} resource created without status.")

        status_patch = json.dumps({"status": backup_spec_full["status"]})
        run(
            ["kubectl", "-n", src_ns, "patch", "backup", backup_name, "--type=merge", "--subresource=status", "-p", status_patch]
        )
        print(f"[DONE] ✅ Backup {backup_name} completed in {pretty_duration}")

        # Mark request as completed and delete the BackupRequest (move semantics)
        try:
            update_status(namespace, name, "Completed", f"Backup moved to Backups CR ({backup_name})")
            run(["kubectl", "-n", namespace, "delete", "backuprequest", name, "--ignore-not-found=true"])
            print(f"[INFO] BackupRequest {name} moved → Backup {backup_name} and deleted.")
        except Exception as e:
            print(f"[WARN] Could not finalize BackupRequest {name}: {e}")

    except Exception as e:
        reason = e.__class__.__name__
        backup_failures.labels(namespace=src_ns, method=method, reason=reason).inc()
        update_status(namespace, name, "Failed", str(e))
        print(f"[ERROR] Backup failed: {e}")
        raise
