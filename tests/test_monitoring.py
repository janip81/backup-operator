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


def test_heaps_stay_bounded_while_the_oldest_snapshot_is_stuck():
    ev = observability.snapshot_event
    ev(event={"type": "ADDED"}, body=_snap("stuck", ns="churn", owned=False, deleting="2026-11-17T01:00:00Z"))
    for i in range(10000):
        ev(event={"type": "ADDED"}, body=_snap(f"c{i}", ns="churn", deleting="2026-11-17T02:00:00Z"))
        ev(event={"type": "DELETED"}, body=_snap(f"c{i}", ns="churn"))
    g = metrics._snap_groups[("churn", "data")]
    assert len(g["names"]) == 1
    assert len(g["created"]) + len(g["deleting"]) <= 2 * len(g["names"]) + 32
    since = _val(metrics.SNAPSHOT_DELETING_SINCE, namespace="churn", pvc="data")
    assert since == datetime(2026, 11, 17, 1, tzinfo=UTC).timestamp()
    ev(event={"type": "DELETED"}, body=_snap("stuck", ns="churn", owned=False))
    assert ("churn", "data") not in metrics._snap_groups


class _ListKube:
    def __init__(self, objs):
        self.objs = objs

    def list_all(self, group, version, plural):
        return self.objs.get(plural, [])


def test_resync_removes_objects_whose_delete_was_missed():
    observability.snapshot_event(event={"type": "ADDED"}, body=_snap("missed", ns="rs"))
    observability.snapshot_event(event={"type": "ADDED"}, body=_snap("kept", ns="rs"))
    observability.request_event(event={"type": "ADDED"}, body={
        "metadata": {"name": "missed-req", "namespace": "rs"}, "spec": {"repository": {}}})
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="rs", pvc="data") == 2
    # the watch reconnected (410 Gone) and never delivered DELETED for "missed"
    snaps = [_snap(n, ns=other_ns) for n, other_ns in ((s[1], s[0]) for s in metrics._snaps) if n != "missed"]
    reqs = [{"metadata": {"name": k.split("/", 1)[1], "namespace": k.split("/", 1)[0]},
             "spec": {"repository": {}} if ph != "Legacy" else {},
             "status": {"phase": ph}} for k, (ph, _) in metrics._requests.items() if k != "rs/missed-req"]
    observability.resync_inventory(_ListKube({"volumesnapshots": snaps, "backuprequests": reqs}), 1_000_000.0)
    assert _val(metrics.OWNED_SNAPSHOTS, namespace="rs", pvc="data") == 1
    assert "rs/missed-req" not in metrics._requests
    assert metrics.INVENTORY_SYNCED._value.get() == 1_000_000.0
    first = metrics.INVENTORY_FIRST_SYNC._value.get()
    observability.resync_inventory(_ListKube({"volumesnapshots": snaps, "backuprequests": reqs}), 2_000_000.0)
    assert metrics.INVENTORY_FIRST_SYNC._value.get() == first  # set once per process


def test_list_all_follows_continue_tokens():
    pages = [{"items": [1, 2], "metadata": {"continue": "t1"}}, {"items": [3], "metadata": {}}]
    seen = []

    def fake(*args, **kw):
        seen.append(kw.get("_continue"))
        return pages[len(seen) - 1]

    k = kube_mod.Kube.__new__(kube_mod.Kube)
    k.limiter = kube_mod.RateLimiter(1000, 1000)
    k.timeout = (1, 1)
    k.custom = type("C", (), {"list_cluster_custom_object": staticmethod(fake)})()
    assert k.list_all("g", "v", "p") == [1, 2, 3]
    assert seen == [None, "t1"]


def test_operator_uses_its_own_finalizer(monkeypatch):
    """kopf strips "its" finalizer from watched objects no handler needs it for; with the shared default
    that would touch v0.3 requests and snapshots of other kopf operators."""
    import logging

    import kopf

    import controller.__main__ as main
    monkeypatch.setattr(main, "start_http_server", lambda port: None)
    settings = kopf.OperatorSettings()
    default = settings.persistence.finalizer
    main.startup(settings=settings, logger=logging.getLogger("t"))
    assert settings.persistence.finalizer == "backup.techmonkeys.se/cleanup" != default


class _RacingKube(_ListKube):
    """Delivers watch events while the relist is in flight (after its list snapshot was taken)."""

    def __init__(self, objs, during):
        super().__init__(objs)
        self.during = during

    def list_all(self, group, version, plural):
        items = super().list_all(group, version, plural)
        for fn in self.during.pop(plural, []):
            fn()
        return items


def test_relist_never_overrides_newer_watch_events():
    ev, cev, rev = observability.snapshot_event, observability.snapshot_content_event, observability.request_event
    ev(event={"type": "ADDED"}, body=_snap("deleted-during", ns="race"))
    cev(event={"type": "ADDED"}, body={"metadata": {"name": "race-c-del", "creationTimestamp": "2026-11-17T03:00:00Z"},
                                       "spec": {"volumeSnapshotRef": {"namespace": "race", "name": "x"}}})
    req = {"metadata": {"name": "finishing", "namespace": "race", "creationTimestamp": "2026-11-17T03:00:00Z"},
           "spec": {"repository": {}}, "status": {"phase": "Running"}}
    rev(event={"type": "ADDED"}, body=req)
    done = {**req, "status": {"phase": "Succeeded"}}
    others = [_snap(n, ns=ns) for ns, n in metrics._snaps if ns != "race"]
    lists = {
        # stale lists: taken before the watch events below
        "volumesnapshots": [*others, _snap("deleted-during", ns="race")],
        "volumesnapshotcontents": [{"metadata": {"name": "race-c-del", "creationTimestamp": "2026-11-17T03:00:00Z"},
                                    "spec": {"volumeSnapshotRef": {"namespace": "race", "name": "x"}}}],
        "backuprequests": [req],
    }
    during = {
        "volumesnapshots": [lambda: ev(event={"type": "ADDED"}, body=_snap("added-during", ns="race")),
                            lambda: ev(event={"type": "DELETED"}, body=_snap("deleted-during", ns="race"))],
        "volumesnapshotcontents": [lambda: cev(event={"type": "DELETED"},
                                               body={"metadata": {"name": "race-c-del"}, "spec": {}})],
        "backuprequests": [lambda: rev(event={"type": "MODIFIED"}, body=done)],
    }
    observability.resync_inventory(_RacingKube(lists, during), 3_000_000.0)
    assert ("race", "added-during") in metrics._snaps       # ADDED after an older list is kept
    assert ("race", "deleted-during") not in metrics._snaps  # DELETED isn't resurrected
    assert "race-c-del" not in metrics._contents
    assert metrics._requests["race/finishing"][0] == "Succeeded"  # not rolled back to Running
    # the next relist (with fresh lists) is authoritative again
    lists["volumesnapshots"] = others
    observability.resync_inventory(_ListKube(lists), 3_000_600.0)
    assert ("race", "added-during") not in metrics._snaps
