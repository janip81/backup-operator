# 🧾 Changelog

All notable changes to **backup-operator** will be documented in this file.  
This project follows [Semantic Versioning](https://semver.org/).

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
