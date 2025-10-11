# 🧩 Backup Operator

[![Build and Release](https://github.com/janip81/backup-operator/actions/workflows/build-release.yaml/badge.svg)](https://github.com/janip81/backup-operator/actions/workflows/build-release.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/container-ghcr.io%2Fjanip81%2Fbackup--operator-blue)](https://github.com/janip81/backup-operator/pkgs/container/backup-operator-controller)


A lightweight Kubernetes operator written in Python using **Kopf** for handling snapshot-based backups of PVCs.  
Designed to work seamlessly across namespaces and clusters.

---

## 🚀 Overview

The **Backup Operator** automates creating, monitoring, and cleaning up **VolumeSnapshots** and temporary Jobs that back up application data to remote locations.

The idea came from that i always forget my own backups, i can be up and running from a distaster at home with Cluster-api in 10-15minutes but i always forget backups in my own applications
and as i run ArgoCD for deployment of everything including my workload clusters velero did not feel like a good match as i dont want full PVC backups and my deployments are handles by gitops.

So this project was born to solve that problem for me, it is not perfect but it works for me and maybe it can work for you too. It does snapshot and runs a custom bash script inside the worker
container that rsync/scps the data to a remote location, it can be anything that is reachable from the cluster. and i dont need to save full 50gb PVC backups to my NAS every time i do a backup.

It was designed primarily for Home Assistant, Zigbee2MQTT, and other workloads running on PVC-backed persistent storage, but it works generically for any namespace or application.

---

## 🧩 Features

- ✅ **Snapshot-based backups** using a `VolumeSnapshotClass`
- ✅ Supports **vSphere-CSI**, Longhorn, and other CSI drivers
- ✅ Handles **WaitForFirstConsumer** binding (auto pod trigger)
- ✅ Automatic **ServiceAccount + RBAC creation** per namespace
- ✅ Graceful error handling if resources are deleted mid-backup
- ✅ **Prometheus metrics** exposed on `:8080/metrics`
- ✅ Minimal dependencies — only Python + Kopf + Kubernetes client

---

## 🧱 Architecture

```
[CustomResource] --> [Backup Operator (Kopf)]
                        ├── Creates VolumeSnapshot
                        ├── Waits until snapshot Ready
                        ├── Creates temporary PVC clone
                        ├── Triggers binder pod (if needed)
                        ├── Launches backup Job (worker)
                        ├── Rsyncs data to remote storage
                        ├── Cleans up Job, snapshot, PVC, and secrets
```

---

## 🧾 Example: BackupRequest Custom Resource

```yaml
apiVersion: backup.techmonkeys.se/v1
kind: BackupRequest
metadata:
  name: home-assistant
  namespace: backup-operator
spec:
  sourceNamespace: home-assistant
  sourcePVC: home-assistant-home-assistant
  snapshotClass: vsphere-csi-snapclass
  storageClass: vsphere-csi
  remotePath: backup@starbase.threshold.se:/s3/test-backup/hass/
  customScript:
    preset: rsync-snapshot-default-backup-script
    key: backup.sh
```

---

## ⚙️ Environment Variables (Controller)

| Variable | Description | Default |
|-----------|--------------|----------|
| `WORKER_IMAGE` | Image used for worker job | `ghcr.io/janip81/backup-operator-worker:latest` |
| `SERVICE_ACCOUNT` | ServiceAccount for job execution | `backup-runner` |
| `IMAGE_PULL_SECRET` | Secret for pulling private images | `ghcr-creds` |
| `DEFAULT_STORAGE_CLASS` | Default StorageClass | `vsphere-csi` |
| `MOUNT_PATH` | Mount path inside the worker | `/mnt/source` |
| `OPERATOR_NAMESPACE` | Namespace where operator runs | `backup-operator` |

---

## ⚙️ Environment Variables (Worker Job)

| Variable | Description | Default |
|-----------|--------------|----------|
| `BACKUP_METHOD` | Always `snapshot` | `snapshot` |
| `PVC_NAME` | Name of source PVC | — |
| `SNAPSHOT_CLASS` | SnapshotClass used | — |
| `TEMP_NAMESPACE` | Namespace of BackupRequest | — |
| `REMOTE_PATH` | Remote rsync destination | — |
| `SOURCE_PATH` | Path to data inside PVC | `/mnt/source/backups` |
| `STORAGE_CLASS` | StorageClass for temporary PVC | `vsphere-csi` |
| `MOUNT_PATH` | Mount path of PVC inside container | `/mnt/source` |
| `SSH_KEY_PATH` | SSH private key path | `/tmp/ssh_key` |

---

## 🧰 Cleanup Behavior

After successful or failed backups, temporary resources are removed automatically:

- ✅ Snapshots deleted after job completion
- ✅ Temporary PVCs and jobs removed
- ✅ Copied secrets and ConfigMaps cleaned up
- ✅ Status updated on BackupRequest CR

Example logs:

```
[DONE] ✅ Backup home-assistant-20251010-182807 completed
[INFO] Cleaning up resources in home-assistant...
[INFO] Snapshot home-assistant-home-assistant-snap-20251010-182807 deleted
```

---

## 🧑‍💻 Development & Testing

Run locally (for debug):

```bash
kopf run --standalone controller/__main__.py --verbose
```

Deploy to cluster:

```bash
kubectl apply -f manifests/crds/
kubectl apply -f manifests/
```

---

## 🧪 Version

**v0.2.0** — stable release  
Includes:
- Automatic handling of WaitForFirstConsumer PVCs
- Non-root SSH key fix
- Graceful error handling for deleted PVCs/snapshots/jobs
- Prometheus metrics support
- Functional baseline for production

---

## 🧭 Next Steps / TODO (v0.3.0)

Planned improvements for the next release:

- [ ] 🧩 **Incremental backups** — track and upload only changed files between runs  
- [ ] ♻️ **Automatic restore workflow** — ability to restore PVCs directly via `BackupRequest` CR  
- [ ] 🔒 **Enhanced security** — restricted service account roles and scoped RBAC per namespace  
- [ ] 💾 **Configurable retention policy** — define how many snapshots or backups to keep  
- [ ] 📦 **Custom storage support** — add native support for MinIO, S3, or NFS targets  
- [ ] 📊 **Expanded Prometheus metrics** — job durations, errors, and per-namespace stats  
- [ ] ⚙️ **Operator configuration via ConfigMap** — allow dynamic runtime configuration without rebuilding the image  
- [ ] 🐛 **More resilient cleanup** — handle stuck jobs, orphaned PVCs, or snapshot timeouts  
- [ ] 🚀 **CI/CD automation** — auto-tag releases from GitHub Actions on version bumps  
- [ ] 🧰 **Helm chart packaging** — make deployment and upgrades easier via Helm  
- [ ] 🧱 **CRD refinements** — support for `spec.schedule`, custom backup methods, and restore validation

---

💡 *Want to contribute?*  
Check out the [CONTRIBUTING.md](CONTRIBUTING.md) (coming soon) and open a feature branch with your proposal.


© 2025 Jani Pesonen — TechMonkeys
