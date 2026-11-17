# 🧾 Changelog

All notable changes to **backup-operator** will be documented in this file.  
This project follows [Semantic Versioning](https://semver.org/).

---

## [v0.4.0] - unreleased
### Overview
Reliability and security rework (issues #8–#15) and a new storage model: restic snapshots with plugins.
**Breaking:** see docs/upgrade-0.4.md.

#### Added
- Plugins `files`, `archives`, `sqlite`, `custom`; restic repositories (S3/REST over https, SFTP with
  known_hosts); retention; optional verification; restore mode and example (#12)
- Backup records for every finished run, written before cleanup; metrics rebuilt from cluster state;
  PrometheusRule with unit tests (#11)
- Schedule validation with a Ready condition, history limits, time zones, catch-up and overlap
  policies (#9, #15)
- kustomize base and monitoring component (#15)
- Runaway guards: metrics and alerts for snapshot storms, piling-up requests, API errors and
  restarts; Grafana dashboard (`k8s/monitoring/grafana`) (#11)

#### Changed
- Controller: resumable state machine with persisted attempts, backoff, admission limits and API rate
  limiting; Python Kubernetes client with timeouts instead of kubectl (#8)
- Ownership-checked cleanup with UID preconditions and a delete finalizer (#10)
- Least-privilege RBAC, hardened pods, no credentials outside the Secret (#13)
- CI validates PRs, tests the worker image, publishes immutable tags (#14)

#### Fixed (after first deployment)
- Metrics count only v0.4 Backup records, and deleting records lowers or removes a source's last
  success/failure; counters start at 0 for known sources so the first failure shows as an increase
- Dashboard: cluster selector (`k8s_cluster`) on every query, schedules table, whole-number counts,
  operator memory no longer doubled

#### Removed
- rsync/scp transfer, script presets, per-namespace RBAC and secret copying

---

## [v0.3.0] - 2025-10-12
### 🚀 Overview
Introduced automated backup scheduling via the new `BackupSchedule` CRD.

#### ✨ New Features
- Cron-based scheduling for automated backups.
- Namespace isolation for schedules.
- Custom script support per schedule.
- Modularized controller handlers for better maintainability.

#### 🛠 Improvements
- Simplified controller imports and improved log clarity.
- Cleaner CRD reconciliation and startup flow.
- Improved structure for `controller/handlers/` modules.

#### 🧹 Internal
- Updated development branch workflow (`develop` → `main` → tag → reset).
- CI/CD validated for tag-based releases.

---

## [v0.2.1] - 2025-10-11
### 🧩 Overview
Stability release focusing on snapshot reliability and controller cleanup.

#### 🛠 Fixes
- Resolved snapshot readiness timeout issues.
- Improved error handling for backup worker jobs.
- Enhanced cleanup logic after completed backups.

#### 🧹 Internal
- Updated Prometheus metrics for operator events.
- Refined job cleanup and PVC snapshot handling.

---

## [v0.2.0] - 2025-10-01
### 🚀 Initial Stable Release
The first stable release of **backup-operator**.

#### ✨ Features
- Implemented `BackupRequest` CRD for on-demand backups.
- Added rsync-based snapshot backup workflow.
- Support for `vsphere-csi` snapshot and restore.
- Automated job creation and cleanup.

#### 🧰 Components
| Component | Image |
|------------|-------|
| Controller | `ghcr.io/janip81/backup-operator-controller:v0.2.0` |
| Worker     | `ghcr.io/janip81/backup-operator-worker:v0.2.0` |

---

*Generated on 2025-10-12*
