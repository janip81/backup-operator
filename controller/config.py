"""Operator configuration from environment variables."""
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


GROUP = "backup.techmonkeys.se"
VERSION = "v1"

WORKER_IMAGE = os.getenv("WORKER_IMAGE", "ghcr.io/janip81/backup-operator-worker:0.4.0")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT", "backup-runner")
IMAGE_PULL_SECRET = os.getenv("IMAGE_PULL_SECRET", "")
DEFAULT_STORAGE = os.getenv("DEFAULT_STORAGE_CLASS", "")
MOUNT_PATH = os.getenv("MOUNT_PATH", "/mnt/source")
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "backup-operator")

# --- Load and retry bounds (#8) ---
RECONCILE_INTERVAL = float(os.getenv("RECONCILE_INTERVAL_SECONDS", "10"))
API_CONNECT_TIMEOUT = float(os.getenv("API_CONNECT_TIMEOUT_SECONDS", "5"))
API_READ_TIMEOUT = float(os.getenv("API_READ_TIMEOUT_SECONDS", "30"))
API_QPS = float(os.getenv("API_QPS", "5"))  # shared client-side rate limit for operator API calls
API_BURST = _int("API_BURST", 10)
MAX_CONCURRENT_RUNS = _int("MAX_CONCURRENT_RUNS", 1)  # active runs across the cluster
MAX_SNAPSHOTS_PER_PVC = _int("MAX_SNAPSHOTS_PER_PVC", 1)  # operator-owned snapshots per source PVC
DEFAULT_MAX_ATTEMPTS = _int("DEFAULT_MAX_ATTEMPTS", 3)
RETRY_BASE_SECONDS = _int("RETRY_BASE_SECONDS", 120)
RETRY_MAX_SECONDS = _int("RETRY_MAX_SECONDS", 3600)
API_ERROR_BUDGET = _int("API_ERROR_BUDGET", 20)  # consecutive transient API errors before the attempt fails
ADMISSION_DELAY = _int("ADMISSION_DELAY_SECONDS", 30)  # after startup, before admitting new runs
SNAPSHOT_TIMEOUT = _int("SNAPSHOT_TIMEOUT_SECONDS", 900)
CLONE_TIMEOUT = _int("CLONE_TIMEOUT_SECONDS", 600)
JOB_TIMEOUT = _int("JOB_TIMEOUT_SECONDS", 7200)
CLEANUP_TIMEOUT = _int("CLEANUP_TIMEOUT_SECONDS", 900)
