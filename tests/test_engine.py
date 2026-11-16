"""Reconciler behaviour under failures: bounded retries, resumption, admission and load limits (#8)."""
from datetime import UTC, datetime, timedelta

import pytest

from controller import config as cfg
from controller import engine
from tests.fakekube import FakeKube

T0 = datetime(2026, 11, 17, 3, 0, tzinfo=UTC)


def make_req(name="r1", pvc="data", ns="app", uid="0123456789abcdef", **spec):
    return {"metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"sourcePVC": pvc, "snapshotClass": "snap", "remotePath": "x", **spec},
            "status": {}}


def apply(body, patch):
    st = body.setdefault("status", {})
    for k, v in (patch or {}).items():
        if v is None:
            st.pop(k, None)
        else:
            st[k] = v


def run(bodies, kube, adm, now, cycles=200, step=timedelta(seconds=10)):
    """Drive the reconciler like kopf's timer: one call per request per interval."""
    for _ in range(cycles):
        for b in bodies:
            apply(b, engine.reconcile(b, kube, adm, now, rnd=lambda: 0.5))
        kube.settle()
        now += step
        if all((b.get("status") or {}).get("phase") in engine.TERMINAL for b in bodies):
            break
    return now


@pytest.fixture
def adm():
    return engine.Admission(ready=True)


@pytest.fixture
def kube():
    k = FakeKube()
    k.add_source_pvc("app", "data")
    k.add_source_pvc("app", "other")
    return k


def test_happy_path_uses_deterministic_names_and_cleans_up(kube, adm):
    body = make_req()
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Succeeded"
    assert kube.created["snapshot"] == ["bo-01234567-a1-snap"]
    assert kube.created["job"] == ["bo-01234567-a1-job"]
    assert not kube.objs["snapshot"] and not kube.objs["job"] and not kube.objs["secret"] and not kube.objs["configMap"]
    # only the source PVCs are left
    assert set(kube.objs["pvc"]) == {("app", "data"), ("app", "other")}
    assert ("app", "r1") in kube.objs["backups"]
    assert adm.active_count() == 0


def test_job_failures_retry_with_backoff_and_stop_at_max_attempts(kube, adm):
    kube.job_result = "Failed"
    body = make_req(maxAttempts=3)
    run([body], kube, adm, T0, cycles=2000)
    st = body["status"]
    assert st["phase"] == "Failed" and st["reason"] == "JobFailed" and st["attempt"] == 3
    # exactly one snapshot per attempt, never more than one alive at a time, all removed
    assert kube.created["snapshot"] == [f"bo-01234567-a{i}-snap" for i in (1, 2, 3)]
    assert kube.max_live_snapshots == 1
    assert not kube.objs["snapshot"]


def test_retry_waits_for_backoff_without_api_calls(kube, adm):
    kube.job_result = "Failed"
    body = make_req(maxAttempts=2)
    now = T0
    while body["status"].get("attempt", 1) == 1 or body["status"].get("phase") != "Pending":
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        kube.settle()
        now += timedelta(seconds=10)
    nxt = engine.parse_ts(body["status"]["nextAttemptAt"])
    assert nxt - now > timedelta(seconds=60)  # base 120s backoff
    calls = kube.calls
    assert engine.reconcile(body, kube, adm, now) is None
    assert kube.calls == calls


def test_transient_api_errors_resume_same_attempt(kube, adm):
    body = make_req()
    now = T0
    for _ in range(3):  # reach Snapshotting with the snapshot created
        apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    assert body["status"]["phase"] == "Snapshotting"
    kube.fail_next = 3
    for _ in range(3):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        assert body["status"]["phase"] == "Snapshotting"
        now = engine.parse_ts(body["status"]["nextAttemptAt"])
    assert body["status"]["apiErrors"] == 3
    run([body], kube, adm, now)
    assert body["status"]["phase"] == "Succeeded"
    assert body["status"].get("apiErrors", 0) == 0
    assert kube.created["snapshot"] == ["bo-01234567-a1-snap"]  # no duplicate after the outage


def test_api_error_budget_fails_the_attempt(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "API_ERROR_BUDGET", 2)
    body = make_req(maxAttempts=1)
    kube.fail_next = 10_000
    now = T0
    for _ in range(5):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        now += timedelta(hours=1)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "ApiErrors"


def test_snapshot_limit_is_terminal_not_retried(kube, adm):
    kube.snapshot_error = "rpc error: code = FailedPrecondition desc = number of snapshots reaches the configured maximum (3)"
    body = make_req(maxAttempts=5)
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "SnapshotLimit"
    assert kube.created["snapshot"] == ["bo-01234567-a1-snap"]
    assert not kube.objs["snapshot"]


def test_global_concurrency_limit_queues_second_run(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 1)
    kube.job_result = None  # first job keeps running
    a, b = make_req("a", uid="aaaaaaaa1"), make_req("b", pvc="other", uid="bbbbbbbb1")
    run([a, b], kube, adm, T0, cycles=10)
    assert a["status"]["phase"] == "Running"
    assert b["status"]["phase"] == "Pending" and b["status"]["reason"] == "Queued"
    kube.job_result = "Complete"
    run([a, b], kube, adm, T0 + timedelta(minutes=5))
    assert a["status"]["phase"] == b["status"]["phase"] == "Succeeded"
    assert kube.max_live_snapshots == 1


def test_one_active_run_per_source_pvc(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 5)
    kube.job_result = None
    a, b = make_req("a", uid="aaaaaaaa1"), make_req("b", uid="bbbbbbbb1")  # same PVC
    run([a, b], kube, adm, T0, cycles=10)
    assert b["status"]["reason"] == "Queued" and "Another run is active" in b["status"]["message"]


def test_snapshot_ceiling_blocks_new_snapshots(kube, adm):
    kube.objs["snapshot"][("app", "leftover")] = {"metadata": {"name": "leftover", "labels": {
        "app.kubernetes.io/managed-by": "backup-operator", "backup.techmonkeys.se/source-pvc": "data"}}}
    body = make_req()
    run([body], kube, adm, T0, cycles=5)
    assert body["status"]["reason"] == "SnapshotCeiling"
    assert kube.created["snapshot"] == []
    assert adm.active_count() == 0


def test_running_request_reregisters_after_restart(kube, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 1)
    adm = engine.Admission(ready=True)
    running = make_req("a", uid="aaaaaaaa1")
    running["status"] = {"phase": "Running", "attempt": 1, "phaseStartedAt": engine.ts(T0)}
    kube.objs["job"][("app", "bo-aaaaaaaa-a1-job")] = {"metadata": {"name": "bo-aaaaaaaa-a1-job"}, "status": {}}
    engine.reconcile(running, kube, adm, T0 + timedelta(seconds=10))
    new = make_req("b", pvc="other", uid="bbbbbbbb1")
    apply(new, engine.reconcile(new, kube, adm, T0 + timedelta(seconds=60)))
    assert new["status"]["reason"] == "Queued"


def test_missing_source_pvc_fails_without_creating_anything(kube, adm):
    body = make_req(pvc="nope", maxAttempts=2)
    run([body], kube, adm, T0, cycles=2000)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "SourceNotFound"
    assert kube.created["snapshot"] == []


def test_retry_delay_is_capped_with_jitter():
    assert engine.retry_delay(1, rnd=lambda: 0.5) == cfg.RETRY_BASE_SECONDS
    assert engine.retry_delay(50, rnd=lambda: 1.0) == pytest.approx(cfg.RETRY_MAX_SECONDS * 1.2)


# --- review round 1 (Codex): restart, cleanup outcome, queue load, rate limit ---

def test_restart_running_request_in_backoff_still_counts(kube, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 1)
    adm = engine.Admission(ready=True)  # e.g. bootstrap raced with a status write
    running = make_req("a", uid="aaaaaaaa1")
    running["status"] = {"phase": "Running", "attempt": 1, "phaseStartedAt": engine.ts(T0),
                         "nextAttemptAt": engine.ts(T0 + timedelta(hours=1))}
    new = make_req("b", pvc="other", uid="bbbbbbbb1")
    later = T0 + timedelta(minutes=5)  # a is still backing off
    assert engine.reconcile(running, kube, adm, later) is None
    apply(new, engine.reconcile(new, kube, adm, later))
    assert new["status"]["reason"] == "Queued" and kube.created["snapshot"] == []


def test_bootstrap_registers_active_runs():
    adm = engine.Admission()
    running = make_req("a", uid="aaaaaaaa1")
    running["status"] = {"phase": "CleaningUp"}
    adm.bootstrap([running, make_req("b", uid="bbbbbbbb1")])
    assert adm.active_count() == 1


@pytest.mark.parametrize("outcome,reason", [("succeeded", "CleaningUp"), ("failed", "SnapshotLimit")])
def test_cleanup_outage_preserves_outcome(kube, adm, monkeypatch, outcome, reason):
    monkeypatch.setattr(cfg, "API_ERROR_BUDGET", 2)
    body = make_req(maxAttempts=3)
    n = engine.Req.from_body(body).names()
    kube.objs["snapshot"][("app", n["snapshot"])] = {"metadata": {"name": n["snapshot"]}}
    body["status"] = {"phase": "CleaningUp", "attempt": 1, "outcome": outcome, "reason": reason,
                      "phaseStartedAt": engine.ts(T0), "startedAt": engine.ts(T0)}
    kube.fail_next = 50
    now = T0
    for _ in range(60):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        assert body["status"]["outcome"] == outcome and body["status"]["attempt"] == 1
        now += timedelta(minutes=10)
        if body["status"]["phase"] in engine.TERMINAL:
            break
    assert body["status"]["phase"] == ("Succeeded" if outcome == "succeeded" else "Failed")
    assert kube.created["snapshot"] == [] and kube.created["job"] == []


def test_queued_backlog_makes_no_api_calls(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 1)
    kube.job_result = None
    active = make_req("active", uid="aaaaaaaa1")
    run([active], kube, adm, T0, cycles=8)
    assert active["status"]["phase"] == "Running"
    backlog = [make_req(f"q{i}", pvc=f"pvc{i}", uid=f"{i:08d}x") for i in range(1000)]
    for b in backlog:  # first pass writes the Queued status once
        apply(b, engine.reconcile(b, kube, adm, T0 + timedelta(minutes=5)))
    calls = kube.calls
    patches = [engine.reconcile(b, kube, adm, T0 + timedelta(minutes=6)) for b in backlog]
    assert kube.calls == calls  # a full pass over the backlog: zero API calls
    assert all(p is None for p in patches)  # and zero status writes


def test_rate_limiter_bounds_aggregate_calls():
    from controller.kube import RateLimiter
    clock = [0.0]
    lim = RateLimiter(qps=5, burst=10, clock=lambda: clock[0], sleep=lambda s: clock.__setitem__(0, clock[0] + s))
    for _ in range(110):
        lim.acquire()
    assert clock[0] == pytest.approx(20.0, rel=0.01)  # 10 burst + 100 at 5/s


def test_no_admission_until_bootstrap_succeeds(kube, monkeypatch):
    """Bootstrap failed: a pending request reconciled before the active one must stay queued, forever."""
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 1)
    adm = engine.Admission()
    new = make_req("b", pvc="other", uid="bbbbbbbb1")
    for minutes in (1, 10, 600):
        apply(new, engine.reconcile(new, kube, adm, T0 + timedelta(minutes=minutes)))
        assert new["status"]["reason"] == "Queued"
    assert kube.calls == 0
    running = make_req("a", uid="aaaaaaaa1")
    running["status"] = {"phase": "Running"}
    adm.bootstrap([running, new])
    apply(new, engine.reconcile(new, kube, adm, T0 + timedelta(hours=11)))
    assert new["status"]["reason"] == "Queued" and "Concurrency limit" in new["status"]["message"]


# --- #10: per-run ownership and cleanup ---

def test_two_runs_same_namespace_dont_touch_each_others_objects(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "MAX_CONCURRENT_RUNS", 2)
    a, b = make_req("a", uid="aaaaaaaa1", maxAttempts=1), make_req("b", pvc="other", uid="bbbbbbbb1")
    kube.job_result = None  # jobs keep running until the test decides
    now = T0
    for _ in range(6):  # both reach Running
        for body in (a, b):
            apply(body, engine.reconcile(body, kube, adm, now))
        kube.settle()
        now += timedelta(seconds=10)
    assert a["status"]["phase"] == b["status"]["phase"] == "Running"
    kube.objs["job"][("app", "bo-aaaaaaaa-a1-job")]["status"] = {
        "conditions": [{"type": "Failed", "status": "True", "reason": "x"}]}
    for _ in range(6):  # a fails and cleans up while b keeps running
        for body in (a, b):
            apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    assert a["status"]["phase"] == "Failed"
    b_names = engine.Req.from_body(b).names()
    for kind in ("job", "pvc", "snapshot", "secret", "configMap"):
        assert ("app", b_names[kind]) in kube.objs[kind], kind
    kube.job_result = "Complete"
    run([b], kube, adm, now)
    assert b["status"]["phase"] == "Succeeded"


def test_foreign_object_with_our_name_is_never_used_or_deleted(kube, adm):
    body = make_req(maxAttempts=3)
    name = engine.Req.from_body(body).names()["snapshot"]
    kube.objs["snapshot"][("app", name)] = {"metadata": {"name": name, "labels": {"owner": "someone-else"}},
                                             "status": {"readyToUse": True}}
    kube.snapshot_error = None
    now = T0
    for _ in range(10):
        apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "NameConflict"
    assert ("app", name) in kube.objs["snapshot"]  # untouched
    assert kube.created["job"] == []


def test_cleanup_all_removes_every_attempt_but_nothing_else(kube):
    req = engine.Req.from_body(make_req(uid="aaaaaaaa1"))
    other = engine.Req.from_body(make_req("b", uid="bbbbbbbb1"))
    for r, attempt in ((req, 1), (req, 2), (other, 1)):
        r.status["attempt"] = attempt
        for kind, name in r.names(attempt).items():
            kube.objs[kind][("app", name)] = {"metadata": {"name": name, "labels": r.labels()}}
    assert engine.cleanup_all(req, kube)  # deletes issued
    assert engine.cleanup_all(req, kube) == []  # all gone
    left = {name for objs in kube.objs.values() for (_, name) in objs}
    assert {n for n in left if n.startswith("bo-aaaaaaaa")} == set()
    assert {n for n in left if n.startswith("bo-bbbbbbbb")} == set(other.names(1).values())


def test_reconcile_skips_requests_being_deleted(kube, adm):
    body = make_req()
    body["metadata"]["deletionTimestamp"] = engine.ts(T0)
    assert engine.reconcile(body, kube, adm, T0) is None and kube.calls == 0
