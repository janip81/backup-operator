# Backup Operator — Agent Guide

Shared instructions for coding agents (Claude, Codex) working in this repository.
Claude loads this file through `CLAUDE.md`.

This repository is **public**. Never commit secrets, kubeconfigs, internal hostnames,
IP addresses or real destination paths. Use placeholders such as `backup@nas.example:/backups/app/`.

Kubernetes backup operator built with [kopf](https://kopf.readthedocs.io/) (Python).
Images: `ghcr.io/janip81/backup-operator-{controller,worker}`.

## Current status (2026-11-16)

- **The operator is paused** (0 replicas) in the owner's cluster, and has been since 2026-03-24.
  Don't scale it up or create BackupRequests in a live cluster without the owner's explicit approval.
- The known problems are tracked as GitHub issues and must be resolved before re-enabling:
  - #8 (P0): bound retries and API load
  - #9: idempotent schedules
  - #10: cleanup ownership
  - #11: honest outcomes and alerts
  - #12: retention and versioned recovery points
  - #13: credential handling
  - #14: build pipeline and image provenance
  - #15: GitOps deployment and schedule config
- The image currently tagged `develop` contains an emergency fix from 2026-03 that was never committed
  (see #14). Don't rebuild or overwrite `develop` until that has been resolved.

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
  requirements.txt           dependencies actually installed in the controller image
worker/                      transfer container (rsync/scp/sftp scripts)
k8s/manifests/               namespace, RBAC, deployment, CRDs, script presets, examples
.github/workflows/           image build workflow (see #14 for known gaps)
```

`requirements.txt` in the repository root is a stale local venv freeze. The images don't use it.

## CRDs (`backup.techmonkeys.se/v1`)

| Kind | Purpose |
|------|---------|
| `BackupSchedule` | 60 s timer; creates a `BackupRequest` when its cron expression is due. `retention` is **not implemented** (#12). |
| `BackupRequest` | One run: VolumeSnapshot → PVC clone → worker Job → cleanup → `Backup` record, then the request is deleted. |
| `Backup` | Record of a finished run. A worker failure can still be recorded misleadingly (#11). |

## Hard-won lessons (do not regress)

- **2026-03-12 etcd incident:** retries that left the previous snapshot behind built up to about 13,400
  VolumeSnapshots. The snapshot controller's paginated listing then overloaded etcd and the API servers.
  Any retry path must reuse or clean up its own resources and must be bounded (#8).
- **The vSphere CSI driver allows at most 3 snapshots per volume.** Once a volume is at the limit,
  `FailedPrecondition ... reaches the configured maximum` keeps occurring; retrying makes it worse.
- **Never remove VolumeSnapshot finalizers by force.** That skips CSI `DeleteSnapshot` and leaves
  snapshots behind in vSphere that Kubernetes can no longer see.
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
python -m venv .venv && . .venv/bin/activate
pip install -r controller/requirements.txt
kopf run -m controller --all-namespaces --verbose   # against a disposable cluster only
make build TAG=local                                 # local images; do not push CI tags
```

## Troubleshooting (read-only first)

```bash
kubectl get backupschedules,backuprequests,backups -A
kubectl get volumesnapshots -A --no-headers | wc -l
kubectl logs -n backup-operator deploy/backup-operator
```

If the VolumeSnapshot count is growing without bound, scale the operator to 0 first and ask the owner.
Cluster-level recovery such as deleting etcd keys is an owner decision and is not documented here.
