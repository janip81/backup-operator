"""Runaway-guard metrics and the Grafana dashboard: the signals that would have caught the v0.3 pile-up."""
import json
import pathlib
import re
import subprocess
import sys
from datetime import UTC, datetime

import pytest
from kubernetes.client.exceptions import ApiException

from controller import engine, metrics
from controller import kube as kube_mod
from controller.handlers import observability
from tests.fakekube import FakeKube
from tests.test_engine import T0, make_req, run

ROOT = pathlib.Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "k8s/monitoring/grafana/backup-operator.json"


def _req(name, phase=None, v04=True):
    body = {"metadata": {"name": name, "namespace": "app"}, "spec": {}}
    if v04:
        body["spec"]["repository"] = {"url": "s3:https://s3.example.com/bucket"}
    if phase:
        body["status"] = {"phase": phase}
    return body


def _phase(p):
    return metrics.REQUESTS.labels(phase=p)._value.get()


def test_requests_counted_by_phase_including_legacy():
    observability.request_event(event={"type": "ADDED"}, body=_req("mon-a"))
    observability.request_event(event={"type": "ADDED"}, body=_req("mon-b", "Running"))
    observability.request_event(event={"type": "ADDED"}, body=_req("mon-old", "Failed", v04=False))
    pending, running, legacy = _phase("Pending"), _phase("Running"), _phase("Legacy")
    observability.request_event(event={"type": "MODIFIED"}, body=_req("mon-a", "Succeeded"))
    assert _phase("Pending") == pending - 1
    observability.request_event(event={"type": "DELETED"}, body=_req("mon-b", "Running"))
    assert _phase("Running") == running - 1  # the gauge drops to 0, it doesn't keep the old count
    assert legacy >= 1


def _snap(name, ns="inv", pvc="data", owned=True, created="2026-11-17T03:00:00Z", deleting=None):
    md = {"name": name, "namespace": ns, "creationTimestamp": created}
    if owned:
        md["labels"] = {"app.kubernetes.io/managed-by": "backup-operator"}
    if deleting:
        md["deletionTimestamp"] = deleting
    return {"metadata": md, "spec": {"source": {"persistentVolumeClaimName": pvc}}}


def _val(gauge, **labels):
    for metric in gauge.collect():
        for sample in metric.samples:
            if sample.labels == labels:
                return sample.value
    return None


def test_snapshot_inventory_per_volume_includes_unlabelled_snapshots():
    ev = observability.snapshot_event
    ev(event={"type": "ADDED"}, body=_snap("s1", created="2026-11-17T03:00:00Z"))
    ev(event={"type": "ADDED"}, body=_snap("s2", created="2026-11-17T02:00:00Z"))
    ev(event={"type": "ADDED"}, body=_snap("manual", owned=False, pvc="other"))
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="inv", pvc="data") == 2
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="inv", pvc="other") == 0
    assert _val(metrics.ALL_SNAPSHOTS, namespace="inv") == 3
    oldest = _val(metrics.OWNED_SNAPSHOT_OLDEST, namespace="inv", pvc="data")
    assert oldest == datetime(2026, 11, 17, 2, tzinfo=UTC).timestamp()

    ev(event={"type": "MODIFIED"}, body=_snap("manual", owned=False, pvc="other", deleting="2026-11-17T04:00:00Z"))
    assert _val(metrics.SNAPSHOT_DELETING_SINCE, namespace="inv", pvc="other") is not None
    for name, pvc, owned in (("s1", "data", True), ("s2", "data", True), ("manual", "other", False)):
        ev(event={"type": "DELETED"}, body=_snap(name, pvc=pvc, owned=owned))
    assert _val(metrics.ALL_SNAPSHOTS, namespace="inv") == 0
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="inv", pvc="data") == 0
    # timestamp series disappear instead of going stale (no false "old snapshot" alerts)
    assert _val(metrics.OWNED_SNAPSHOT_OLDEST, namespace="inv", pvc="data") is None
    assert _val(metrics.SNAPSHOT_DELETING_SINCE, namespace="inv", pvc="other") is None


def test_orphan_contents_after_grace_period():
    created = datetime(2026, 11, 17, 3, tzinfo=UTC).timestamp()
    observability.snapshot_event(event={"type": "ADDED"}, body=_snap("bound", ns="orph"))
    for name, ref in (("c-bound", "bound"), ("c-orphan", "gone")):
        observability.snapshot_content_event(event={"type": "ADDED"}, body={
            "metadata": {"name": name, "creationTimestamp": "2026-11-17T03:00:00Z"},
            "spec": {"volumeSnapshotRef": {"namespace": "orph", "name": ref}}})
    base = metrics.orphan_contents(created)  # within the grace period nothing counts
    assert metrics.orphan_contents(created + 3600) == base + 1
    observability.snapshot_content_event(event={"type": "DELETED"}, body={
        "metadata": {"name": "c-orphan"}, "spec": {}})
    assert metrics.orphan_contents(created + 3600) == base


def test_replaying_a_pile_up_is_linear():
    """~14,000 objects (the v0.3 incident) replayed at startup must not take quadratic time."""
    import time as _time
    start = _time.monotonic()
    for i in range(14000):
        observability.snapshot_event(event={"type": "ADDED"}, body=_snap(f"p{i}", ns="pile"))
        observability.request_event(event={"type": "ADDED"}, body={
            "metadata": {"name": f"p{i}", "namespace": "pile", "creationTimestamp": "2026-11-17T03:00:00Z"},
            "spec": {}})
    assert _time.monotonic() - start < 10
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="pile", pvc="data") == 14000
    for i in range(14000):
        observability.snapshot_event(event={"type": "DELETED"}, body=_snap(f"p{i}", ns="pile"))
        observability.request_event(event={"type": "DELETED"}, body={
            "metadata": {"name": f"p{i}", "namespace": "pile"}, "spec": {}})
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="pile", pvc="data") == 0


def test_oldest_unfinished_request():
    body = _req("mon-old-pending")
    body["metadata"]["creationTimestamp"] = "2001-01-01T00:00:00Z"
    observability.request_event(event={"type": "ADDED"}, body=body)
    assert metrics.oldest_unfinished() == datetime(2001, 1, 1, tzinfo=UTC).timestamp()
    body["status"] = {"phase": "Succeeded"}
    observability.request_event(event={"type": "MODIFIED"}, body=body)
    assert metrics.oldest_unfinished() != datetime(2001, 1, 1, tzinfo=UTC).timestamp()


def test_snapshot_creation_is_counted():
    kube = FakeKube()
    kube.add_source_pvc("app", "data")
    before = metrics.SNAPSHOTS_CREATED.labels(namespace="app", pvc="data")._value.get()
    run([make_req(uid="feedfacefeedface")], kube, engine.Admission(ready=True), T0)
    after = metrics.SNAPSHOTS_CREATED.labels(namespace="app", pvc="data")._value.get()
    assert after == before + len(kube.created["snapshot"])


def test_api_errors_counted_but_expected_statuses_are_not():
    k = kube_mod.Kube.__new__(kube_mod.Kube)
    k.limiter = kube_mod.RateLimiter(1000, 1000)
    k.timeout = (1, 1)

    def fail(status):
        def fn(*_, **__):
            raise ApiException(status=status)
        return fn

    c500 = metrics.API_ERRORS.labels(code="500")._value.get()
    c404 = metrics.API_ERRORS.labels(code="404")._value.get()
    with pytest.raises(kube_mod.TransientApiError):
        k._call(fail(500))
    assert k._call(fail(404), ok_status=(404,)) is None
    assert metrics.API_ERRORS.labels(code="500")._value.get() == c500 + 1
    assert metrics.API_ERRORS.labels(code="404")._value.get() == c404


def test_dashboard_is_generated_and_in_sync():
    committed = DASHBOARD.read_text()
    subprocess.run([sys.executable, str(ROOT / "hack/gen-dashboard.py")], check=True, capture_output=True)
    assert DASHBOARD.read_text() == committed, "run `make dashboard` and commit the JSON"


def _exprs():
    dash = json.loads(DASHBOARD.read_text())
    for p in dash["panels"]:
        for t in p.get("targets", []):
            yield p["title"], t["expr"]


def test_dashboard_queries_use_existing_metrics():
    defined = set(re.findall(r'"(backup_operator_[a-z_]+)"', (ROOT / "controller/metrics.py").read_text()))
    exported = defined | {f"{m}_bucket" for m in defined}
    for title, expr in _exprs():
        for name in re.findall(r"backup_operator_[a-z_]+", expr):
            assert name in exported, f"{title}: unknown metric {name}"


def test_alert_rules_use_existing_metrics():
    import yaml
    defined = set(re.findall(r'"(backup_operator_[a-z_]+)"', (ROOT / "controller/metrics.py").read_text()))
    rules = yaml.safe_load((ROOT / "k8s/monitoring/prometheusrule.yaml").read_text())
    for group in rules["spec"]["groups"]:
        for rule in group["rules"]:
            for name in re.findall(r"backup_operator_[a-z_]+", rule["expr"]):
                assert name in defined, f"{rule['alert']}: unknown metric {name}"
