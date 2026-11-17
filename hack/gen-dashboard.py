#!/usr/bin/env python3
"""Generate k8s/monitoring/grafana/backup-operator.json (run: make dashboard). Edit this file, not the JSON."""
import json
import pathlib
import re

DS = {"type": "prometheus", "uid": "${datasource}"}
NS = 'namespace=~"$namespace"'
# Central Prometheus setups hold several clusters, told apart by k8s_cluster. "All" matches series
# without the label too, so single-cluster setups work unchanged.
CLUSTER = 'k8s_cluster=~"$cluster"'
_METRIC = re.compile(r'\b(backup_operator_[a-z_]+|ALERTS|kube_[a-z_]+|container_[a-z_]+)(\{)?')


def with_cluster(expr):
    """Add the cluster matcher to every metric selector in a query."""
    return _METRIC.sub(lambda m: f"{m.group(1)}{{{CLUSTER}, " if m.group(2) else f"{m.group(1)}{{{CLUSTER}}}", expr)
_id = 0
_y = 0


def nid():
    global _id
    _id += 1
    return _id


def target(expr, legend="", instant=False, fmt=None):
    t = {"datasource": DS, "expr": with_cluster(expr), "legendFormat": legend, "refId": chr(64 + nid() % 26 or 26)}
    if instant:
        t["instant"], t["range"] = True, False
    if fmt:
        t["format"] = fmt
    return t


def row(title):
    global _y
    p = {"type": "row", "title": title, "id": nid(), "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y}}
    _y += 1
    return p


def place(panels, h):
    global _y
    x = 0
    for p in panels:
        p["gridPos"] = {"h": h, "w": p.pop("_w"), "x": x, "y": _y}
        x += p["gridPos"]["w"]
    _y += h
    return panels


def stat(title, expr, w=4, unit="none", thresholds=None, desc=""):
    steps = thresholds or [{"color": "green", "value": None}]
    return {"type": "stat", "title": title, "description": desc, "id": nid(), "datasource": DS, "_w": w,
            "targets": [target(expr, instant=True)],
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}},
                            "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none"}}


def ts(title, targets, w=12, unit="none", stack=False, bars=False, desc=""):
    custom = {"drawStyle": "bars" if bars else "line", "fillOpacity": 60 if bars else 10, "lineWidth": 1,
              "stacking": {"mode": "normal" if stack else "none"}, "showPoints": "never"}
    return {"type": "timeseries", "title": title, "description": desc, "id": nid(), "datasource": DS, "_w": w,
            "targets": targets,
            "fieldConfig": {"defaults": {"unit": unit, "custom": custom}, "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def table(title, queries, w=12, desc="", overrides=None):
    """Instant queries merged into one row per (namespace, source)."""
    targets = []
    for expr, ref in queries:
        t = target(expr, instant=True, fmt="table")
        t["refId"] = ref
        targets.append(t)
    return {"type": "table", "title": title, "description": desc, "id": nid(), "datasource": DS, "_w": w,
            "targets": targets,
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "organize", "options": {
                    "excludeByName": {"Time": True},
                    "indexByName": {"namespace": 0, "source": 1},
                    "renameByName": {f"Value #{ref}": ref for _, ref in queries}}},
                {"id": "filterFieldsByName", "options": {"include": {
                    "names": ["namespace", "source", *[ref for _, ref in queries]]}}}],
            "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides or []},
            "options": {"showHeader": True, "sortBy": [{"displayName": "namespace"}]}}


def red_over(v):
    return [{"color": "green", "value": None}, {"color": "red", "value": v}]


panels = []
panels.append(row("Overview"))
panels += place([
    stat("Firing backup alerts", 'count(ALERTS{alertstate="firing", alertname=~"Backup.*"}) or vector(0)',
         thresholds=red_over(1), desc="Alerts from the backup-operator PrometheusRule that are firing now."),
    stat("Schedules not ready", f'count(backup_operator_schedule_ready{{{NS}}} == 0) or vector(0)',
         thresholds=red_over(1)),
    stat("Stale sources",
         f'count((time() - backup_operator_last_success_timestamp_seconds{{{NS}}}) > on(namespace, source) '
         f'group_left() (1.5 * backup_operator_schedule_interval_seconds)) or vector(0)',
         thresholds=red_over(1), desc="Newest successful backup older than 1.5x the schedule interval."),
    stat("Active runs", "max(backup_operator_active_runs)", desc="Runs holding a concurrency slot."),
    stat("Operator snapshots", f"sum(backup_operator_owned_snapshots{{{NS}}}) or vector(0)",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 3}, {"color": "red", "value": 6}],
         desc="VolumeSnapshots created by the operator that still exist. Normally 0, or 1 per active run."),
    stat("Unfinished requests", 'sum(backup_operator_requests{phase!~"Succeeded|Failed|Legacy"}) or vector(0)',
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 5}, {"color": "red", "value": 10}]),
], 4)

panels.append(row("Per source"))
panels += place([
    {**ts("Time since last successful backup", [
        target(f"time() - backup_operator_last_success_timestamp_seconds{{{NS}}}", "{{namespace}}/{{source}}")],
        w=12, unit="s"), "type": "bargauge",
     "options": {"orientation": "horizontal", "displayMode": "gradient",
                 "reduceOptions": {"calcs": ["lastNotNull"]}},
     "fieldConfig": {"defaults": {"unit": "s", "thresholds": {"mode": "absolute", "steps": [
         {"color": "green", "value": None}, {"color": "orange", "value": 86400}, {"color": "red", "value": 172800}]}},
         "overrides": []}},
    table("Schedules", [
        (f"max by (namespace, source) (backup_operator_schedule_ready{{{NS}}})", "Ready"),
        (f"max by (namespace, source) (time() - backup_operator_last_success_timestamp_seconds{{{NS}}})",
         "Last success"),
        (f"max by (namespace, source) (time() - backup_operator_last_failure_timestamp_seconds{{{NS}}})",
         "Last failure"),
        (f"max by (namespace, source) (backup_operator_schedule_interval_seconds{{{NS}}})", "Interval"),
    ], w=12, desc="Ready: source volume, credentials and repository settings are valid. Times are ages.",
        overrides=[
            {"matcher": {"id": "byName", "options": "Ready"}, "properties": [
                {"id": "mappings", "value": [{"type": "value", "options": {
                    "1": {"text": "Ready", "color": "green"}, "0": {"text": "Not ready", "color": "red"}}}]},
                {"id": "custom.cellOptions", "value": {"type": "color-background"}}]},
            *[{"matcher": {"id": "byName", "options": n}, "properties": [{"id": "unit", "value": "s"}]}
              for n in ("Last success", "Last failure", "Interval")]]),
], 8)
panels += place([
    ts("Runs per day by result", [target(
        f"round(sum by (namespace, source, result) (increase(backup_operator_runs_total{{{NS}}}[1d])))",
        "{{namespace}}/{{source}} {{result}}")], w=8, bars=True, stack=True),
    ts("Failed attempts by reason (per hour)", [target(
        f"round(sum by (reason) (increase(backup_operator_attempt_failures_total{{{NS}}}[1h])))", "{{reason}}")],
        w=8, bars=True, stack=True),
    ts("Run duration p95", [target(
        f"histogram_quantile(0.95, sum by (namespace, source, le) "
        f"(rate(backup_operator_run_duration_seconds_bucket{{{NS}}}[1d])))", "{{namespace}}/{{source}}")],
        w=8, unit="s"),
], 8)

panels.append(row("Runaway guards (what overloaded a cluster before)"))
panels += place([
    stat("VolumeSnapshots in cluster", "sum(backup_operator_cluster_volume_snapshots) or vector(0)", w=6,
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 50}, {"color": "red", "value": 100}],
         desc="Every VolumeSnapshot, whoever created it."),
    stat("Orphan snapshot contents", "max(backup_operator_orphan_snapshot_contents) or vector(0)", w=6,
         thresholds=red_over(1), desc="VolumeSnapshotContents older than 15 minutes without a VolumeSnapshot."),
    stat("Oldest unfinished request",
         "time() - (max(backup_operator_oldest_unfinished_request_created_timestamp_seconds) > 0) or vector(0)",
         w=6, unit="s",
         thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 10800},
                     {"color": "red", "value": 21600}]),
    stat("Stuck cleanups", "max(backup_operator_cleanup_stuck_requests) or vector(0)", w=6, thresholds=red_over(1)),
], 4)
panels += place([
    ts("Snapshots created per hour, per volume", [target(
        f"round(sum by (namespace, pvc) (increase(backup_operator_snapshots_created_total{{{NS}}}[1h])))",
        "{{namespace}}/{{pvc}}")], w=8, desc="BackupSnapshotStorm fires above 4 per hour for one volume."),
    ts("Operator-owned snapshots per volume", [target(
        f"backup_operator_owned_snapshots{{{NS}}}", "{{namespace}}/{{pvc}}")], w=8,
        desc="BackupSnapshotsAccumulating fires above 1 for 30 minutes."),
    ts("All VolumeSnapshots per namespace", [target(
        "sum by (namespace) (backup_operator_cluster_volume_snapshots)", "{{namespace}}")], w=8, stack=True,
        desc="Independent of operator labels; BackupClusterSnapshotsGrowing fires at +20 per hour."),
], 8)
panels += place([
    ts("Snapshot deletion pending for", [target(
        "time() - backup_operator_snapshot_deleting_since_timestamp_seconds", "{{namespace}}/{{pvc}}")],
        w=8, unit="s", desc="A snapshot the CSI driver can't delete. BackupSnapshotDeletionStuck fires after 30 minutes."),
    ts("BackupRequests by phase", [target("sum by (phase) (backup_operator_requests)", "{{phase}}")],
       w=8, stack=True),
    ts("Kubernetes API errors per second", [target(
        "sum by (code) (rate(backup_operator_api_errors_total[5m]))", "{{code}}"),
        target("sum(rate(backup_operator_api_calls_total[5m]))", "all calls")], w=8, unit="reqps"),
], 8)
panels += place([
    ts("Operator restarts (kube-state-metrics)", [target(
        'sum(increase(kube_pod_container_status_restarts_total{container="controller", '
        'pod=~"backup-operator-.*"}[1h]))', "restarts/h")], w=12),
    ts("Operator memory (cAdvisor)", [target(
        # max: cAdvisor reports the container and its cgroup scope separately; a sum doubles it
        'max by (pod) (container_memory_working_set_bytes{container="controller", pod=~"backup-operator-.*"})',
        "{{pod}}")], w=12, unit="bytes"),
], 6)

dash = {
    "uid": "backup-operator", "title": "Backup Operator", "tags": ["backup", "backup-operator"],
    "schemaVersion": 39, "version": 1, "editable": True, "refresh": "1m",
    "time": {"from": "now-7d", "to": "now"}, "timezone": "browser", "graphTooltip": 1,
    "templating": {"list": [
        {"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Data source",
         "current": {}, "hide": 0},
        {"name": "cluster", "type": "query", "datasource": DS, "label": "Cluster",
         "query": {"query": "label_values(backup_operator_active_runs, k8s_cluster)", "refId": "cluster"},
         "definition": "label_values(backup_operator_active_runs, k8s_cluster)",
         "refresh": 2, "multi": True, "includeAll": True, "allValue": ".*", "current": {}, "hide": 0},
        {"name": "namespace", "type": "query", "datasource": DS, "label": "Namespace",
         "query": {"query": f"label_values(backup_operator_schedule_interval_seconds{{{CLUSTER}}}, namespace)",
                   "refId": "ns"},
         "definition": f"label_values(backup_operator_schedule_interval_seconds{{{CLUSTER}}}, namespace)",
         "refresh": 2, "multi": True, "includeAll": True, "allValue": ".*", "current": {}, "hide": 0},
    ]},
    "annotations": {"list": []},
    "panels": panels,
}
out = pathlib.Path(__file__).resolve().parent.parent / "k8s/monitoring/grafana/backup-operator.json"
out.write_text(json.dumps(dash, indent=2) + "\n")
print(out)
