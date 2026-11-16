# Operations

## Supervised first run (per schedule)

1. Apply the schedule and check readiness:
   `kubectl -n <ns> get backupschedule <name>` → `READY True`. If not, the Ready condition says why
   (`kubectl -n <ns> get backupschedule <name> -o jsonpath='{.status.conditions}'`).
2. Trigger a run without waiting for the cron tick with a one-off request using the same template
   (see `examples/backup-once.yaml`). Give it the label `backup.techmonkeys.se/schedule: <schedule name>`
   so it uses the schedule's source identity (without the label, an ad-hoc request is identified by
   its PVC name). Follow it:
   `kubectl -n <ns> get backuprequest <name> -w` (Pending → Snapshotting → Cloning → Running →
   Recording → CleaningUp → Succeeded).
3. Check the record: `kubectl -n <ns> get backup <name> -o yaml` → `status.phase: Succeeded`,
   `status.artifact.snapshotId`, `filesNew`, `totalBytes` plausible for the volume.
4. Check that nothing is left behind: `kubectl -n <ns> get volumesnapshots,pvc,jobs -l app.kubernetes.io/managed-by=backup-operator`
   should be empty once the request is Succeeded.

## Restore check (before relying on a schedule)

1. Restore the exact snapshot of the supervised run into a **new** PVC with `examples/restore-job.yaml`:
   set `RESTORE_SNAPSHOT` to `status.artifact.shortId` from its Backup record. (For `latest`, set
   `BACKUP_SOURCE` to the source shown in the record, `status.source`, and `BACKUP_HOST` to the source
   namespace, `spec.sourceNamespace`.)
2. Mount the restored PVC in a throwaway pod and compare with the live data, e.g. file counts and
   checksums of a few important files; for `sqlite`, open the database and run `PRAGMA integrity_check`.
3. Delete the throwaway pod, Job and PVC.

Repeat the restore check after changing plugins, repositories or retention, and periodically.

## When something goes wrong

| Symptom | Where to look |
|---|---|
| Schedule `READY False` | `.status.conditions` (missing PVC, Secret, invalid URL) |
| Request `Failed` | `.status.reason`, `.status.message`, `.status.attempts`; the Backup record has the same reason |
| Request stays `Pending` / `Queued` | another run is active for the same PVC, the concurrency limit, or a leftover snapshot (`SnapshotCeiling`) |
| `CleaningUp` for a long time | `.status.lastError`; a temporary object can't be deleted (check finalizers and the CSI driver, never remove finalizers by hand) |
| `BackupSnapshotsAccumulating` alert | operator-owned snapshots outside active runs; scale the operator to 0 and investigate before deleting anything |

Deleting a BackupRequest removes its temporary objects (finalizer). Deleting a Backup record does
not delete the restic snapshot; restic retention does that.
