# Backup Operator — Agent Guide

Shared instructions for coding agents (Claude, Codex) working in this repository.
Claude loads this file through `CLAUDE.md`.

This repository is **public**. Never commit secrets, kubeconfigs, internal hostnames,
IP addresses or real destination paths. Use placeholders such as `backup@nas.example:/backups/app/`.

Kubernetes backup operator built with [kopf](https://kopf.readthedocs.io/) (Python).
Images: `ghcr.io/janip81/backup-operator-{controller,worker}`.

## Current status

- v0.4 (issues #8–#15) is a rework that is not released yet. The owner's cluster still runs nothing
  from it. Don't deploy it anywhere or create BackupRequests in a live cluster without the owner's
  explicit approval.
- Upgrade path from v0.3: docs/upgrade-0.4.md. Operating and restore checks: docs/operations.md.

## Repository layout

```
controller/                 kopf operator
  engine.py                 BackupRequest state machine (admission, phases, recording, cleanup)
  scheduling.py             BackupSchedule ticks, validation, history
  jobspec.py                snapshot / temporary PVC / worker Job builders, repository URL rules
  kube.py                   Kubernetes API wrapper: timeouts, rate limit, normalized errors
  metrics.py                Prometheus metrics
  handlers/                 kopf wiring (timers, delete finalizer, metric events)
worker/                     restic worker image: bo-worker (backup/restore) and plugins/
k8s/base/                   kustomize base: CRDs, namespace, RBAC, Deployment, Service
k8s/monitoring/             ServiceMonitor and PrometheusRule (optional)
examples/                   schedules per plugin, credentials, one-off backup, restore Job
docs/                       upgrade notes and operations
tests/                      pytest (fake API), worker integration tests, promtool rule tests
hack/                       helper scripts (rule tests)
```

## CRDs (`backup.techmonkeys.se/v1`)

| Kind | Purpose |
|------|---------|
| `BackupSchedule` | Cron (+ time zone) → one BackupRequest per tick from `spec.template`; Ready condition |
| `BackupRequest` | One run: snapshot → temporary PVC → worker Job → Backup record → cleanup. Spec is immutable. |
| `Backup` | Result of a finished run (restic snapshot ID, sizes, reason). Removed when retention prunes its snapshot. |

## Hard-won lessons (do not regress)

- **Unbounded retries are dangerous.** A retry that creates new snapshots without removing the previous
  attempt's can build up thousands of VolumeSnapshots and overload the API server and etcd.
  Every retry path must reuse or clean up its own resources and must be bounded (#8).
- **CSI drivers can cap snapshots per volume.** vSphere CSI, for example, has a configurable per-volume maximum
  with a low default. Once a volume is at that maximum, `CreateSnapshot` keeps failing with `FailedPrecondition`.
  Treat this as a terminal error, not something to retry.
- **Never remove VolumeSnapshot finalizers by force.** That skips CSI `DeleteSnapshot` and leaves snapshots
  behind in the storage backend that Kubernetes can no longer see.
- Snapshot readiness is not application consistency. Apps that produce their own archives
  (e.g. Home Assistant) are the intended sources.

## Working rules

- Work on a feature branch and open a PR against `main`. Never push to `main` or `develop` directly;
  the owner reviews and merges.
- Use Conventional Commit messages (`fix:`, `feat:`, `docs:`, `chore:`, `refactor:`).
- Link work to issues with `Fixes #N` / `Refs #N`.
- Security issues are titled `[Security follow-up] ...` and state only goals and acceptance criteria.
  Concrete evidence stays out of GitHub.
- Before claiming a fix, test failure paths against a disposable cluster (kind or the owner's dev cluster):
  API errors, snapshot timeouts, controller restarts, two runs in one namespace. Never test against production.
- Don't push images by hand to CI-owned tags (`develop`, `latest`, `v*`).
- Agent reviews: independent review comments on PRs or issues are welcome. Mark them with the agent
  name and the reviewed commit SHA. The calling agent verifies findings before acting on them.

## Local development

```bash
make check                                           # ruff + pytest in a container, same as CI
make worker-test                                     # worker integration tests (docker)
make rules-test                                      # alert rule tests (promtool)
make build                                           # local images tagged :local
make run-operator                                    # against a disposable cluster only
```

## Troubleshooting (read-only first)

```bash
kubectl get backupschedules,backuprequests,backups -A
kubectl get volumesnapshots -A --no-headers | wc -l
kubectl logs -n backup-operator deploy/backup-operator
```

If the VolumeSnapshot count is growing without bound, scale the operator to 0 first and ask the owner.
Cluster-level recovery such as deleting etcd keys is an owner decision and is not documented here.
