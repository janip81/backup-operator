from prometheus_client import Counter, Histogram

backup_total = Counter("backup_operator_backups_total", "Total backups started", ["namespace", "method"])
backup_failures = Counter("backup_operator_backups_failed_total", "Failed backups", ["namespace", "method", "reason"])
backup_duration = Histogram("backup_operator_backup_duration_seconds", "Duration of backup jobs", ["namespace", "method"])
backup_job_total = Counter("backup_operator_backups_job_total", "Total backups per job", ["namespace", "method", "job"])
backup_job_duration = Histogram("backup_operator_backups_job_duration_seconds", "Backup duration per job", ["namespace", "method", "job"])
