# 🧩 Backup Operator

[![Build and Release](https://github.com/janip81/backup-operator/actions/workflows/build-release.yaml/badge.svg)](https://github.com/janip81/backup-operator/actions/workflows/build-release.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/container-ghcr.io%2Fjanip81%2Fbackup--operator-blue)](https://github.com/janip81/backup-operator/pkgs/container/backup-operator-controller)

A small Kubernetes operator (Python, [kopf](https://kopf.readthedocs.io/)) that backs up the data on
PersistentVolumes: it snapshots the volume, lets a **plugin** pick what to keep, and stores it as a
versioned, encrypted [restic](https://restic.net/) snapshot in S3, a REST server or over SFTP.

> **Status (2026-11):** v0.4 is a rework of the operator (issues #8–#15). It is not released yet;
> v0.4 has been tested with unit tests, worker integration tests and on a disposable kind cluster, not
> yet in production. Try it on a non-production cluster first, and in production only after the
> v0.4.0 release and a supervised first run (see
> [docs/operations.md](docs/operations.md)). v0.3 objects must be migrated, see
> [docs/upgrade-0.4.md](docs/upgrade-0.4.md).

## Why

The idea came from that i always forget my own backups, i can be up and running from a distaster at home with Cluster-api in 10-15minutes but i always forget backups in my own applications
and as i run ArgoCD for deployment of everything including my workload clusters velero did not feel like a good match as i dont want full PVC backups and my deployments are handles by gitops.

So this project was born to solve that problem for me, it is not perfect but it works for me and maybe it can work for you too. Each run snapshots the volume, mounts a read-only copy in a short-lived
worker, and a plugin decides what to store, so i dont need to save full 50gb PVC backups to my NAS every time i do a backup.

## How it works

```
BackupSchedule ──(one per cron tick)──► BackupRequest ──► VolumeSnapshot ──► temporary PVC (read-only)
                                                                                   │
                       Backup record ◄── result ◄── worker Job: plugin ──► restic snapshot ──► repository
```

- **BackupSchedule** creates at most one **BackupRequest** per cron tick (no catch-up storms, no
  overlapping runs by default).
- The controller runs each request as a resumable state machine with bounded retries and
  concurrency limits. Temporary objects are owned by the run and removed afterwards.
- The **worker** has no Kubernetes API access. It runs the plugin, stores a restic snapshot, marks it
  complete, applies retention and reports a summary.
- A **Backup** record is written for every finished run (success or failure) *before* cleanup, with
  the restic snapshot ID and sizes.

## Plugins

| Plugin | Use it for | Settings (`plugin.config`) |
|---|---|---|
| `files` | Volumes without their own backup: the whole volume or chosen paths | `paths`, `exclude` |
| `archives` | Apps that write their own archives (Home Assistant, game servers, …) | `dir`, `pattern`, `newest`, `recursive`, `allowEmpty` |
| `sqlite` | Apps with SQLite: consistent copies (WAL applied, integrity-checked) plus all other files | `pattern`, `includeOther`, `exclude` |
| `custom` | Your own script from a ConfigMap in the app's namespace | see [examples/custom-plugin.yaml](examples/custom-plugin.yaml) |

A snapshot is crash-consistent, not application-consistent. Prefer `archives` for apps that can
export themselves, and `sqlite` for SQLite databases.

## Quick start

```bash
# 1. Install (CRDs, namespace, RBAC, controller). Monitoring is optional (Prometheus Operator).
kubectl apply -k k8s/base
kubectl apply -k k8s/monitoring

# 2. Repository credentials in the app's namespace (see examples/credentials-secret.yaml)
kubectl -n my-app create secret generic backup-repository \
  --from-literal=RESTIC_PASSWORD=... --from-literal=AWS_ACCESS_KEY_ID=... --from-literal=AWS_SECRET_ACCESS_KEY=...

# 3. A schedule (see examples/)
kubectl apply -f examples/schedule-files.yaml
kubectl -n my-app get backupschedules       # READY must be True
kubectl -n my-app get backups               # one record per finished run
```

With Argo CD, point an Application at `k8s/base` (and `k8s/monitoring`) at a release tag and pin the
image in a kustomize overlay.

## Restore

Restores go into a new PVC with a one-off Job: [examples/restore-job.yaml](examples/restore-job.yaml).
`latest` means the newest *complete* snapshot of that source in that namespace. Snapshot IDs are in
the Backup records (`.status.artifact.shortId`). Details: [docs/operations.md](docs/operations.md).

## Security

- The operator only reads Secrets/ConfigMaps to validate them; it never copies credentials. The
  worker mounts the app's credentials Secret as files; nothing secret goes into env vars, arguments,
  logs, records or metrics.
- Repository URLs can't carry credentials; plain HTTP is refused; SFTP requires `known_hosts`.
- Worker pods: no service account token, read-only root filesystem, all capabilities dropped except
  `DAC_READ_SEARCH` (to read any file on the snapshot copy). Set `worker.runAsUser` to run
  non-root without capabilities where the data is readable by that user (Pod Security "restricted").
  The default needs Pod Security "privileged" in the app's namespace.

## Monitoring

Metrics on `:8080/metrics` (`backup_operator_*`): last success/failure per source, runs and failed
attempts by reason, durations, active runs, operator-owned snapshots, schedule readiness.
`k8s/monitoring/prometheusrule.yaml` alerts on stale backups, failed runs, schedules that aren't
ready, accumulating snapshots, stuck cleanup and a missing operator, and early on runaway behaviour
(snapshot storms, piling-up requests, API errors, restarts) before it can load the cluster.
`k8s/monitoring/grafana` is a Grafana dashboard ([docs/operations.md](docs/operations.md#dashboard)).

## Configuration (controller environment)

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_IMAGE` | `ghcr.io/janip81/backup-operator-worker:0.4.0` | Worker image (pin a release) |
| `MAX_CONCURRENT_RUNS` | `1` | Active runs across the cluster |
| `MAX_SNAPSHOTS_PER_PVC` | `1` | Operator-owned snapshots allowed per source PVC |
| `DEFAULT_MAX_ATTEMPTS` | `3` | Attempts per request (override with `spec.maxAttempts`) |
| `RETRY_BASE_SECONDS` / `RETRY_MAX_SECONDS` | `120` / `3600` | Backoff between attempts |
| `API_QPS` / `API_BURST` | `5` / `10` | Client-side API rate limit |
| `SNAPSHOT_TIMEOUT_SECONDS` / `JOB_TIMEOUT_SECONDS` | `900` / `7200` | Phase timeouts |
| `SCHEDULE_STARTING_DEADLINE_SECONDS` | `3600` | Ticks later than this are skipped, not run late |
| `DEFAULT_STORAGE_CLASS` | the source PVC's class | StorageClass for the temporary PVC (a run's `storageClass` wins) |
| `WORKER_OUTPUT_LIMIT` / `WORKER_TMP_LIMIT` | `10Gi` / `2Gi` | Worker scratch space |
| `INVENTORY_RESYNC_SECONDS` | `600` | Full relist behind the snapshot/request inventory metrics |

## Development

```bash
make check          # ruff + pytest (container)
make worker-test    # worker integration tests: real image, restic, restore + hash comparison
make rules-test     # alert rule unit tests (promtool)
```

See [CONTRIBUTING.md](CONTRIBUTING.md); coding agents: [AGENTS.md](AGENTS.md).

© 2025 Jani Pesonen — TechMonkeys
