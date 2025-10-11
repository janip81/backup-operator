import json
from datetime import datetime
from .kubectl import run

def update_status(namespace, name, phase, message=None, extra=None):
    now = datetime.utcnow().isoformat() + "Z"
    status_block = {"phase": phase, "lastUpdated": now}
    if message:
        status_block["message"] = message
    if extra and isinstance(extra, dict):
        status_block.update(extra)
    patch = {"status": status_block}

    try:
        run([
            "kubectl", "-n", namespace,
            "patch", "backuprequest", name,
            "--type=merge", "--subresource=status",
            "-p", json.dumps(patch)
        ])
        print(f"[INFO] Status updated: {phase} - {message or ''}")
    except Exception as e:
        print(f"[WARN] Could not patch status for {name}: {e}")

def common_labels(job, req_name, ts):
    return {
        "app.kubernetes.io/created-by": "backup-operator",
        "backup.techmonkeys.se/job": job,
        "backup.techmonkeys.se/request": req_name,
        "backup.techmonkeys.se/timestamp": ts,
    }

def labels_yaml(job, req_name, ts, indent=2):
    pad = " " * indent
    labels = common_labels(job, req_name, ts)
    return "\n".join([f"{pad}{k}: {v}" for k, v in labels.items()])
