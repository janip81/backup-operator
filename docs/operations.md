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

## Apps that are often stopped

Set `whenSourceUnused: Skip` on the schedule. At each tick the operator checks whether a pod (running
or starting) mounts the source PVC. If none does, for example a game server scaled to 0, the tick is
skipped: `PHASE Paused`, `status.lastSkipped.reason: SourceUnused`, and staleness alerts don't fire.
The first tick after a pod uses the volume again runs normally. Changes made between the last backup
and the scale-down are not captured; start a one-off BackupRequest before scaling down if you need
them. `suspend: true` remains the manual pause.

## When something goes wrong

| Symptom | Where to look |
|---|---|
| Schedule `READY False` | `.status.conditions` (missing PVC, Secret, invalid URL) |
| Request `Failed` | `.status.reason`, `.status.message`, `.status.attempts`; the Backup record has the same reason |
| Request stays `Pending` / `Queued` | another run is active for the same PVC, the concurrency limit, or a leftover snapshot (`SnapshotCeiling`) |
| `CleaningUp` for a long time | `.status.lastError`; a temporary object can't be deleted (check finalizers and the CSI driver, never remove finalizers by hand) |
| `BackupSnapshotsAccumulating` alert | operator-owned snapshots outside active runs; scale the operator to 0 and investigate before deleting anything |
| `BackupSnapshotStorm`, `BackupClusterSnapshotsGrowing`, `BackupRequestsPilingUp`, `BackupRequestsGrowing` | snapshots or requests are created faster than normal. Scale the operator to 0 first (`kubectl -n backup-operator scale deploy/backup-operator --replicas=0`), then look at the newest requests' `.status.reason` and the CSI snapshotter logs |
| `BackupSnapshotDeletionStuck`, `BackupOrphanSnapshotContents` | the snapshot controller or CSI driver isn't removing snapshots; backend snapshots may be left and count against per-volume limits. Check VolumeSnapshotContent events and the CSI controller logs |
| `BackupSnapshotNotRemoved`, `BackupRequestStuck` | a run holds its snapshot or slot far longer than its timeouts; check that request's phase and `lastError` |
| `BackupOperatorForbidden`, `BackupOperatorApiErrors` | 403: the ClusterRole doesn't match the controller, re-apply `k8s/base`; 429/5xx/`network`: API server or etcd trouble |

The inventory metrics (`backup_operator_cluster_volume_snapshots`, orphaned contents, deletion age)
come from watching every VolumeSnapshot and VolumeSnapshotContent in the cluster, not only the
operator's own, so leftovers from older versions or manual runs show up too. Thresholds that
depend on your cluster (total snapshots, request history) are in `k8s/monitoring/prometheusrule.yaml`.
The ServiceMonitor sets `honorLabels: true` so `namespace` stays the backed-up app's namespace.

## Dashboard

`k8s/monitoring/grafana` is a Grafana dashboard as a ConfigMap for the Grafana sidecar
(`grafana_dashboard: "1"`); apply it in the cluster where Grafana runs. Rows: an overview of firing
alerts and readiness, per-source age of the last success and run results, and the runaway guards
(snapshots created per hour, owned snapshots, requests by phase, API errors, stuck cleanups,
restarts). The JSON is generated: edit `hack/gen-dashboard.py` and run `make dashboard`.

Deleting a BackupRequest removes its temporary objects (finalizer). Deleting a Backup record does
not delete the restic snapshot; restic retention does that.
