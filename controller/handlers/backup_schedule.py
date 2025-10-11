import json
from datetime import datetime, timezone

import kopf
import croniter

print("[DEBUG] importing", __name__)

from ..config import DEFAULT_STORAGE
from ..utils.kubectl import run
from ..utils.misc import rand_suffix


@kopf.timer("backup.techmonkeys.se", "v1", "backupschedules", interval=60.0)
def handle_schedule(spec, status, name, namespace, **_):
    """
    Periodically reconciles BackupSchedule objects and creates BackupRequest
    when the cron schedule ticks.
    """
    schedule = spec.get("schedule")
    target_ns = spec.get("targetNamespace")
    src_pvc = spec.get("sourcePVC")
    snapshot_class = spec.get("snapshotClass")
    storage_class = spec.get("storageClass", DEFAULT_STORAGE)
    source_path = spec.get("sourcePath", "")
    remote_path = spec.get("remotePath")
    method = spec.get("transferMethod", "rsync")
    custom_script = spec.get("customScript", {"preset": "rsync-snapshot-default-backup-script"})
    retention = spec.get("retention", 7)  # currently unused, reserved for future

    # Timestamps
    now = datetime.now(timezone.utc)
    last_run_str = status.get("lastRunTime") if status else None
    last_run = datetime.fromisoformat(last_run_str.replace("Z", "+00:00")) if last_run_str else None

    # Compute next run using croniter
    itr = croniter.croniter(schedule, last_run or now)
    next_run = itr.get_next(datetime)

    def fmt(dt):
        if not dt:
            return None
        return dt.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"

    next_run_str = fmt(next_run)
    last_run_fmt = fmt(last_run)

    patch = {"status": {"phase": "Active", "nextRunTime": next_run_str, "lastRunTime": last_run_fmt}}

    # If it's time (or first run), trigger a BackupRequest
    if last_run is None or now >= next_run:
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        suffix = rand_suffix()
        req_name = f"{name}-{ts}-{suffix}"

        print(f"[INFO] Triggering BackupRequest {req_name} from schedule {name}")

        backup_req = f"""
apiVersion: backup.techmonkeys.se/v1
kind: BackupRequest
metadata:
  name: {req_name}
  namespace: {namespace}
spec:
  sourceNamespace: {target_ns}
  sourcePVC: {src_pvc}
  sourcePath: {json.dumps(source_path)}
  snapshotClass: {snapshot_class}
  storageClass: {storage_class}
  remotePath: {remote_path}
  transferMethod: {method}
  customScript:
    preset: {custom_script.get('preset')}
"""

        run(["kubectl", "apply", "-f", "-"], input=backup_req)

        patch["status"].update(
            {
                "lastRequest": req_name,
                "lastRunTime": fmt(now),
                "nextRunTime": fmt(croniter.croniter(schedule, now).get_next(datetime)),
            }
        )

    run(
        [
            "kubectl",
            "-n",
            namespace,
            "patch",
            "backupschedule",
            name,
            "--type=merge",
            "--subresource=status",
            "-p",
            json.dumps(patch),
        ]
    )
