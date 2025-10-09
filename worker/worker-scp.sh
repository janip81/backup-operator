#!/usr/bin/env bash
set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "[INFO] Starting backup worker"
log "[INFO] Namespace: ${SOURCE_NAMESPACE}"
log "[INFO] PVC: ${SOURCE_PVC}"
log "[INFO] Remote path: ${REMOTE_PATH}"
log "[INFO] Mount path: ${MOUNT_PATH:-/mnt/source}"
log "[INFO] Source path: ${SOURCE_PATH:-${MOUNT_PATH}/backups}"

# Defaults
MOUNT_PATH="${MOUNT_PATH:-/mnt/source}"
SOURCE_PATH="${SOURCE_PATH:-${MOUNT_PATH}/backups}"
SSH_KEY_PATH="${SSH_KEY_PATH:-/tmp/ssh_key}"
DATE=$(date +"%Y%m%d-%H%M%S")

# Prepare SSH key
if [[ -f "$SSH_KEY_PATH" ]]; then
  cp "$SSH_KEY_PATH" /tmp/id_rsa
  chmod 600 /tmp/id_rsa
  SSH_KEY_PATH="/tmp/id_rsa"
  log "[INFO] Copied SSH key to ${SSH_KEY_PATH} with 600 permissions"
else
  log "[ERROR] SSH key not found at ${SSH_KEY_PATH}"
  exit 1
fi

# Verify source path
if [[ ! -d "${SOURCE_PATH}" ]]; then
  log "[ERROR] Source path ${SOURCE_PATH} does not exist!"
  exit 1
fi

# List source contents
log "[INFO] Listing contents of ${SOURCE_PATH}:"
ls -alh "${SOURCE_PATH}" || true

# Parse remote components
REMOTE_HOST="${REMOTE_PATH%%:*}"
REMOTE_DIR="${REMOTE_PATH#*:}"

# Create remote directory
log "[INFO] Ensuring remote directory exists: ${REMOTE_DIR}"
ssh -i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=no "${REMOTE_HOST}" "mkdir -p '${REMOTE_DIR}'" || {
  log "[ERROR] Failed to create remote directory ${REMOTE_DIR}"
  exit 1
}

# Copy all files recursively
log "[INFO] Copying ${SOURCE_PATH} → ${REMOTE_PATH}"
scp -i "${SSH_KEY_PATH}" -o StrictHostKeyChecking=no -r -p \
  "${SOURCE_PATH}/"* "${REMOTE_PATH}/" || {
  log "[ERROR] SCP transfer failed."
  exit 1
}

log "[INFO] Backup completed successfully at $(date)"
