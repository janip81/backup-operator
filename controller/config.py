import os

# Configuration (env vars)
WORKER_IMAGE       = os.getenv("WORKER_IMAGE", "ghcr.io/janip81/backup-operator-worker:latest")
SERVICE_ACCOUNT    = os.getenv("SERVICE_ACCOUNT", "backup-runner")
IMAGE_PULL_SECRET  = os.getenv("IMAGE_PULL_SECRET", "ghcr-creds")
DEFAULT_STORAGE    = os.getenv("DEFAULT_STORAGE_CLASS", "vsphere-csi")
MOUNT_PATH         = os.getenv("MOUNT_PATH", "/mnt/source")
OPERATOR_NAMESPACE = os.getenv("OPERATOR_NAMESPACE", "backup-operator")
