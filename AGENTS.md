# Backup Operator — Agent Guide

Shared instructions for coding agents (Claude, Codex) working in this repository.
Claude loads this file through `CLAUDE.md`.

This repository is **public**. Never commit secrets, kubeconfigs, internal hostnames,
IP addresses or real destination paths. Use placeholders such as `backup@nas.example:/backups/app/`.

Kubernetes backup operator built with [kopf](https://kopf.readthedocs.io/) (Python).
Images: `ghcr.io/janip81/backup-operator-{controller,worker}`.

## Current status

- **The operator is paused and not production-ready.** Don't run it against a live cluster or create
  BackupRequests there without the owner's explicit approval.
- Known problems are tracked as GitHub issues, and **all** of them must be resolved before it is re-enabled:
  - #8 (P0): bound retries and API load
  - #9: idempotent schedules
  - #10: cleanup ownership
  - #11: honest outcomes and alerts
  - #12: retention and versioned recovery points
  - #13: credential handling
  - #14: build pipeline and image provenance
  - #15: GitOps deployment and schedule configuration
- Don't build or push the CI-owned image tags by hand (see #14).

## Repository layout

```
controller/                  kopf operator
  __main__.py                entrypoint; imports the handlers, starts metrics on :8080
  config.py                  env configuration (WORKER_IMAGE, SERVICE_ACCOUNT, ...)
  metrics.py                 Prometheus counters/histograms
  handlers/backup_request.py BackupRequest pipeline (most logic)
  handlers/backup_schedule.py BackupSchedule timer -> creates BackupRequests
  utils/kubectl.py           subprocess kubectl wrapper: run(), cleanup()
  utils/waiters.py           polling waits for snapshot / PVC / job
  utils/rbac.py              per-namespace RBAC, secret/configmap copies
  utils/status.py            status patch helpers, common labels
  requirements.txt           controller runtime dependencies (the only list used by images)
requirements-dev.txt         lint/test tools for `make check` and CI
tests/                       pytest suite
worker/                      transfer container (rsync/scp/sftp scripts)
k8s/manifests/               namespace, RBAC, deployment, CRDs, script presets, examples
.github/workflows/           image build workflow (see #14 for known gaps)
```

## CRDs (`backup.techmonkeys.se/v1`)

| Kind | Purpose |
|------|---------|
| `BackupSchedule` | 60 s timer; creates a `BackupRequest` when its cron expression is due. `retention` is **not implemented** (#12). |
| `BackupRequest` | One run: VolumeSnapshot → PVC clone → worker Job → cleanup → `Backup` record, then the request is deleted. |
| `Backup` | Record of a finished run. A worker failure can still be recorded misleadingly (#11). |

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
