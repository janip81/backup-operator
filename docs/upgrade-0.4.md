# Upgrading from v0.3 to v0.4

v0.4 changes the API and the storage format. There is no in-place migration: recreate schedules in
the new format. The v0.4 handlers ignore v0.3-format objects, and those objects can't be modified
under the v0.4 schema, so remove them **before** installing the v0.4 CRDs.

## What changed

| v0.3 | v0.4 |
|---|---|
| rsync/scp of a path to an SSH destination (`remotePath`, `transferMethod`, `customScript` presets) | restic repository (`repository.url` + `credentialsSecret`) and a `plugin` |
| SSH key copied from the operator namespace into each app namespace | credentials Secret lives in the app namespace and is only mounted into the worker |
| no host key checking | SFTP requires `known_hosts`; S3/REST must use https |
| `retention` accepted but ignored | `retention.keepLast/Daily/Weekly/Monthly`, applied after successful runs |
| BackupSchedule flat fields | `spec.template` holds the BackupRequest spec |
| requests deleted after a run; failures not recorded | requests kept (history limits); every finished run gets a Backup record |
| image tags `latest`/`develop` | immutable release tags (`0.4.0`) |

## Steps

1. **Stop the old operator** (if it still runs): `kubectl -n backup-operator scale deploy/backup-operator --replicas=0`.
2. **Check for leftovers** from failed v0.3 runs and remove them normally (never by removing finalizers):
   `kubectl get volumesnapshots -A -l app.kubernetes.io/created-by=backup-operator`.
3. **Note your schedules** (`kubectl get backupschedules -A -o yaml`), then delete the v0.3 objects:
   `kubectl delete backupschedules,backuprequests -A --all` (only if nothing else uses these CRDs).
4. **Old Backup records** (v0.3) stay readable, but they may contain worker log excerpts. Review and
   delete them when you no longer need them: `kubectl delete backups -n <ns> <name>`.
5. **Credentials:** v0.3 copied an SSH key into application namespaces and didn't verify host keys.
   Remove the copied Secrets, and rotate that key if it is still trusted anywhere.
6. **Install v0.4** (`kubectl apply -k k8s/base`, optionally `k8s/monitoring`) or update your GitOps
   source to the v0.4.0 tag. Remove the old per-namespace `backup-runner` ServiceAccounts/Roles.
7. **Create a credentials Secret per application namespace** and recreate the schedules in the new
   format (see `examples/`). Pick a plugin per volume.
8. **Do a supervised first run and a restore check** per schedule: [operations.md](operations.md).
