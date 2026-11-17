"""Reconciler behaviour under failures: bounded retries, resumption, admission and load limits (#8)."""
from datetime import UTC, datetime, timedelta

import pytest

from controller import config as cfg
from controller import engine
from tests.fakekube import FakeKube

T0 = datetime(2026, 11, 17, 3, 0, tzinfo=UTC)


def make_req(name="r1", pvc="data", ns="app", uid="0123456789abcdef", **spec):
    return {"metadata": {"name": name, "namespace": ns, "uid": uid},
            "spec": {"sourcePVC": pvc, "snapshotClass": "snap",
                     "repository": {"url": "s3:https://s3.example/bucket", "credentialsSecret": "creds"}, **spec},
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
    assert not kube.objs["snapshot"] and not kube.objs["job"]
    assert not kube.created["secret"] and not kube.created["configMap"]  # nothing is copied any more
    # only the source PVCs are left
    assert set(kube.objs["pvc"]) == {("app", "data"), ("app", "other")}
    assert kube.objs["backups"][("app", "r1")]["status"]["phase"] == "Succeeded"
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
    # the attempt has failed; the result waits for the API to come back so it can be recorded
    assert body["status"]["phase"] == "Recording" and body["status"]["outcome"] == "failed"
    kube.fail_next = 0
    run([body], kube, adm, now)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "ApiErrors"
    assert kube.objs["backups"][("app", "r1")]["status"]["reason"] == "ApiErrors"


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
    for _ in range(12):  # a fails, records and cleans up while b keeps running
        for body in (a, b):
            apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    assert a["status"]["phase"] == "Failed"
    b_names = engine.Req.from_body(b).names()
    for kind in ("job", "pvc", "snapshot"):
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


def test_cleanup_never_deletes_a_replacement_with_the_same_name(kube):
    req = engine.Req.from_body(make_req(uid="aaaaaaaa1"))
    name = req.names()["job"]
    kube.objs["job"][("app", name)] = {"metadata": {"name": name, "uid": "old", "labels": req.labels()}}
    listed = kube.list_owned("job", "app", req.selector())
    # between list and delete, the owned object is replaced by a foreign one with the same name
    kube.objs["job"][("app", name)] = {"metadata": {"name": name, "uid": "new", "labels": {}}}
    kube.delete("job", "app", name, listed[0]["metadata"]["uid"])
    assert kube.objs["job"][("app", name)]["metadata"]["uid"] == "new"


def test_cleanup_uses_recorded_source_namespace(kube):
    body = make_req(uid="aaaaaaaa1", sourceNamespace="a-ns")
    body["status"] = {"sourceNamespace": "a-ns", "attempt": 1}
    req = engine.Req.from_body(body)
    name = req.names()["job"]
    kube.objs["job"][("a-ns", name)] = {"metadata": {"name": name, "uid": "j", "labels": req.labels()}}
    body["spec"]["sourceNamespace"] = "b-ns"  # (rejected by the CRD; cleanup must not follow it anyway)
    assert engine.cleanup_all(engine.Req.from_body(body), kube) == [name]
    assert ("a-ns", name) not in kube.objs["job"]


# --- #11: outcomes recorded before cleanup ---

def _until(body, kube, adm, now, phase, limit=50):
    for _ in range(limit):
        if body["status"].get("phase") == phase:
            return now
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        kube.settle()
        now += timedelta(seconds=10)
    raise AssertionError(f"never reached {phase}: {body['status']}")


def test_success_is_recorded_before_any_cleanup(kube, adm):
    body = make_req()
    now = _until(body, kube, adm, T0, "Recording")
    while body["status"]["phase"] == "Recording":  # name, write, read back (no settle: nothing else moves)
        apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    assert body["status"]["phase"] == "CleaningUp"
    rec = kube.objs["backups"][("app", "r1")]
    assert rec["status"]["phase"] == "Succeeded"
    assert rec["status"]["artifact"] == {"snapshotId": "abc12345", "filesNew": 3}
    assert rec["status"]["result"]["exitCode"] == 0
    names = engine.Req.from_body(body).names()
    assert ("app", names["job"]) in kube.objs["job"]  # temporary objects still present when recorded
    run([body], kube, adm, now)
    assert body["status"]["phase"] == "Succeeded" and body["status"]["backupRef"] == "r1"


def test_record_failure_never_reruns_the_transfer(kube, adm, monkeypatch):
    monkeypatch.setattr(cfg, "API_ERROR_BUDGET", 1)
    body = make_req()
    now = _until(body, kube, adm, T0, "Recording")
    kube.fail_next = 40
    for _ in range(40):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        assert body["status"]["phase"] == "Recording" and body["status"]["outcome"] == "succeeded"
        now += timedelta(minutes=10)
        if not kube.fail_next:
            break
    run([body], kube, adm, now)
    assert body["status"]["phase"] == "Succeeded"
    assert kube.created["job"] == ["bo-01234567-a1-job"]
    assert kube.objs["backups"][("app", "r1")]["status"]["phase"] == "Succeeded"


def test_final_job_failure_is_recorded_with_history(kube, adm):
    kube.job_result = "Failed"
    body = make_req(maxAttempts=2)
    run([body], kube, adm, T0, cycles=2000)
    assert body["status"]["phase"] == "Failed"
    assert [a["attempt"] for a in body["status"]["attempts"]] == [1, 2]
    rec = kube.objs["backups"][("app", "r1")]["status"]
    assert rec["phase"] == "Failed" and rec["reason"] == "JobFailed" and rec["result"]["exitCode"] == 1
    assert rec["attempts"] == 2


def test_missing_source_is_recorded_as_failed(kube, adm):
    body = make_req(pvc="nope", maxAttempts=1)
    run([body], kube, adm, T0)
    assert kube.objs["backups"][("app", "r1")]["status"]["reason"] == "SourceNotFound"


@pytest.mark.parametrize("phase", ["Snapshotting", "Cloning", "Running", "Recording", "CleaningUp"])
def test_restart_in_any_phase_resumes_without_duplicates(kube, phase):
    adm = engine.Admission(ready=True)
    body = make_req()
    now = _until(body, kube, adm, T0, phase)
    fresh = engine.Admission()  # operator restart
    fresh.bootstrap([body])
    run([body], kube, fresh, now)
    assert body["status"]["phase"] == "Succeeded"
    assert len(kube.created["snapshot"]) == len(kube.created["job"]) == len(kube.created["backups"]) == 1


def test_metrics_rebuilt_from_backup_events():
    from controller import metrics
    from controller.handlers import observability
    for i, completed in enumerate(("2026-11-10T03:00:00Z", "2026-11-12T03:00:00Z", "2026-11-11T03:00:00Z")):
        observability.backup_event(event={"type": "ADDED"}, body={
            "metadata": {"name": f"x{i}", "namespace": "app"},
            "spec": {"source": "nightly", "sourceNamespace": "app", "requestRef": {"uid": f"u{i}"}},
            "status": {"phase": "Succeeded", "completedAt": completed}})
    val = metrics.LAST_SUCCESS.labels(namespace="app", source="nightly")._value.get()
    assert val == datetime(2026, 11, 12, 3, tzinfo=UTC).timestamp()


# --- review of #11 (Codex) ---

def test_reused_request_name_gets_its_own_record(kube, adm):
    kube.objs["backups"][("app", "r1")] = {"metadata": {"name": "r1"},
                                           "spec": {"requestRef": {"name": "r1", "uid": "old-uid"}},
                                           "status": {"phase": "Succeeded", "completedAt": "2026-01-01T00:00:00Z"}}
    body = make_req()
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Succeeded" and body["status"]["backupRef"] == "r1-01234567"
    assert kube.objs["backups"][("app", "r1")]["spec"]["requestRef"]["uid"] == "old-uid"  # untouched
    assert kube.objs["backups"][("app", "r1")]["status"]["completedAt"] == "2026-01-01T00:00:00Z"
    assert kube.objs["backups"][("app", "r1-01234567")]["status"]["phase"] == "Succeeded"


def test_record_deleted_before_status_write_is_retried_not_trusted(kube, adm):
    body = make_req()
    now = _until(body, kube, adm, T0, "Recording")
    apply(body, engine.reconcile(body, kube, adm, now))  # choose record name
    real_create = kube.create_cr

    def vanish(plural, ns, b):  # the record is created, then disappears before its status is written
        kube.create_cr = real_create
        real_create(plural, ns, b)
        kube.objs[plural].pop((ns, b["metadata"]["name"]))

    kube.create_cr = vanish
    now += timedelta(minutes=10)
    apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
    assert body["status"]["phase"] == "Recording"  # the vanished record is not trusted
    while body["status"]["phase"] == "Recording":
        assert ("app", engine.Req.from_body(body).names()["job"]) in kube.objs["job"]  # nothing cleaned up yet
        now += timedelta(minutes=10)
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
    assert kube.objs["backups"][("app", "r1")]["status"]["phase"] == "Succeeded"  # verified before cleanup
    run([body], kube, adm, now + timedelta(minutes=10))
    assert body["status"]["phase"] == "Succeeded"
    assert kube.objs["backups"][("app", "r1")]["status"]["phase"] == "Succeeded"


def test_recording_rejection_keeps_original_failure_reason(kube, adm):
    from kubernetes.client.exceptions import ApiException
    kube.job_result = "Failed"
    body = make_req(maxAttempts=1)
    now = _until(body, kube, adm, T0, "Recording")
    def forbid(*_):
        raise ApiException(status=403, reason="Forbidden")
    kube.hooks["create_cr"] = forbid
    for _ in range(4):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        now += timedelta(minutes=10)
    assert body["status"]["phase"] == "Recording" and body["status"]["reason"] == "JobFailed"
    assert "Forbidden" in body["status"]["lastError"]
    kube.hooks.clear()
    run([body], kube, adm, now)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == "JobFailed"
    assert kube.objs["backups"][("app", "r1")]["status"]["reason"] == "JobFailed"


def test_delayed_recording_keeps_transfer_completion_time(kube, adm):
    from controller import metrics
    body = make_req(uid="delayed01")
    now = _until(body, kube, adm, T0, "Recording")
    transfer_done = engine.parse_ts(body["status"]["completedAt"])
    kube.fail_next = 1000  # catalog unreachable for hours
    for _ in range(30):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        now += timedelta(minutes=10)
    kube.fail_next = 0
    run([body], kube, adm, now)
    rec = kube.objs["backups"][("app", "r1")]["status"]
    assert engine.parse_ts(rec["completedAt"]) == transfer_done
    assert engine.parse_ts(rec["recordedAt"]) > transfer_done + timedelta(hours=4)
    # the gauge comes from the record's watch event, which carries the transfer completion time
    from controller.handlers import observability
    observability.backup_event(event={"type": "ADDED"}, body=kube.objs["backups"][("app", "r1")])
    gauge = metrics.LAST_SUCCESS.labels(namespace="app", source="data")._value.get()
    assert gauge == transfer_done.timestamp()


@pytest.mark.parametrize("phase", ["Succeeded", "Failed"])
def test_record_without_uid_is_never_adopted(kube, adm, phase):
    kube.objs["backups"][("app", "r1")] = {"metadata": {"name": "r1", "resourceVersion": "1"}, "spec": {},
                                           "status": {"phase": phase, "completedAt": "2026-01-01T00:00:00Z"}}
    body = make_req()
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Succeeded" and body["status"]["backupRef"] == "r1-01234567"
    assert kube.objs["backups"][("app", "r1")]["status"] == {"phase": phase, "completedAt": "2026-01-01T00:00:00Z"}


def test_concurrently_created_foreign_record_is_not_overwritten(kube, adm):
    body = make_req()
    now = _until(body, kube, adm, T0, "Recording")
    apply(body, engine.reconcile(body, kube, adm, now))  # backupRef chosen: r1
    real_create = kube.create_cr

    def race(plural, ns, b):  # someone else creates r1 first; our create conflicts (409 = no-op)
        kube.create_cr = real_create
        kube.objs[plural][(ns, "r1")] = {"metadata": {"name": "r1", "resourceVersion": "900"},
                                         "spec": {"requestRef": {"uid": "someone-else"}}, "status": {}}
        real_create(plural, ns, b)

    kube.create_cr = race
    for _ in range(5):
        now += timedelta(minutes=10)
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        assert body["status"]["phase"] == "Recording"
    foreign = kube.objs["backups"][("app", "r1")]
    assert foreign["spec"]["requestRef"]["uid"] == "someone-else" and foreign["status"] == {}
    assert "not created by this request" in body["status"]["lastError"]


def test_status_write_is_conditional_on_verified_version(kube):
    kube.objs["backups"][("app", "r1")] = {"metadata": {"name": "r1", "resourceVersion": "5"}, "spec": {}}
    from controller.kube import TransientApiError
    with pytest.raises(TransientApiError):
        kube.patch_cr_status("backups", "app", "r1", {"phase": "Succeeded"}, resource_version="4")
    assert "status" not in kube.objs["backups"][("app", "r1")]


def test_record_replaced_between_read_and_write_is_not_overwritten(kube, adm):
    body = make_req()
    now = _until(body, kube, adm, T0, "Recording")
    for _ in range(2):  # choose name, create record
        apply(body, engine.reconcile(body, kube, adm, now))
        now += timedelta(seconds=10)
    real_patch = kube.patch_cr_status

    def replace_then_patch(plural, ns, name, status, resource_version=None):
        kube.patch_cr_status = real_patch
        kube.objs[plural][(ns, name)] = {"metadata": {"name": name, "resourceVersion": "777"},
                                         "spec": {"requestRef": {"uid": "intruder"}}, "status": {"phase": "Failed"}}
        real_patch(plural, ns, name, status, resource_version)

    kube.patch_cr_status = replace_then_patch
    for _ in range(4):
        apply(body, engine.reconcile(body, kube, adm, now, rnd=lambda: 0.5))
        now += timedelta(minutes=10)
        assert body["status"]["phase"] == "Recording"
    rec = kube.objs["backups"][("app", "r1")]
    assert rec["spec"]["requestRef"]["uid"] == "intruder" and rec["status"] == {"phase": "Failed"}
    assert ("app", engine.Req.from_body(body).names()["job"]) in kube.objs["job"]  # no cleanup


# --- #12: restic worker job, plugins, credentials, record retention ---

def _job(kube):
    return next(iter(kube.objs["job"].values()))


def _env(job):
    return {e["name"]: e["value"] for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}


def test_worker_job_runs_plugin_against_repository(kube, adm):
    kube.job_result = None
    body = make_req(plugin={"name": "archives", "config": {"dir": "backups", "newest": "3"}},
                    retention={"keepLast": 7, "keepWeekly": 4}, verify=True)
    run([body], kube, adm, T0, cycles=8)
    job = _job(kube)
    env = _env(job)
    assert env["PLUGIN"] == "archives" and env["PLUGIN_DIR"] == "backups" and env["PLUGIN_NEWEST"] == "3"
    assert env["RESTIC_REPOSITORY"] == "s3:https://s3.example/bucket" and env["KEEP_LAST"] == "7"
    assert env["KEEP_WEEKLY"] == "4" and env["VERIFY"] == "true" and env["BACKUP_SOURCE"] == "data"
    pod = job["spec"]["template"]["spec"]
    creds = next(v for v in pod["volumes"] if v["name"] == "creds")
    assert creds["secret"]["secretName"] == "creds"  # referenced in the source namespace, never copied
    assert not any("PASSWORD" in e or "SECRET" in e for e in env)  # credentials only as files
    sc = pod["containers"][0]["securityContext"]
    assert sc["capabilities"] == {"drop": ["ALL"], "add": ["DAC_READ_SEARCH"]}
    assert sc["readOnlyRootFilesystem"] and not sc["allowPrivilegeEscalation"]
    assert pod["automountServiceAccountToken"] is False


def test_custom_plugin_mounts_configmap_from_source_namespace(kube, adm):
    kube.job_result = None
    body = make_req(plugin={"name": "custom", "configMapRef": {"name": "my-plugin"}})
    run([body], kube, adm, T0, cycles=8)
    vols = {v["name"]: v for v in _job(kube)["spec"]["template"]["spec"]["volumes"]}
    assert vols["plugin"]["configMap"]["name"] == "my-plugin"


@pytest.mark.parametrize("spec,reason", [
    ({"repository": {"url": "s3:https://x/b", "credentialsSecret": "missing"}}, "CredentialsNotFound"),
    ({"plugin": {"name": "custom", "configMapRef": {"name": "nope"}}}, "PluginNotFound"),
    ({"plugin": {"name": "files", "config": {"bad-key": "x"}}}, "InvalidPluginConfig"),
])
def test_missing_dependencies_fail_terminally_before_the_job(kube, adm, spec, reason):
    body = make_req(maxAttempts=3, **spec)
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Failed" and body["status"]["reason"] == reason
    assert kube.created["job"] == [] and not kube.objs["snapshot"]


def test_records_follow_restic_retention(kube, adm):
    def record(name, short, repo="s3:https://s3.example/bucket", source="data", src_ns="app"):
        kube.objs["backups"][("app", name)] = {
            "metadata": {"name": name, "uid": f"u-{name}", "labels": {
                "app.kubernetes.io/managed-by": "backup-operator", "backup.techmonkeys.se/source": source}},
            "spec": {"source": source, "repository": repo, "sourceNamespace": src_ns},
            "status": {"phase": "Succeeded", "artifact": {"shortId": short}}}
    record("old1", "aaaa0001")
    record("old2", "aaaa0002")
    record("other-repo", "aaaa0003", repo="s3:https://elsewhere/b")
    record("other-ns", "aaaa0004", src_ns="other")
    kube.termination_message = ('{"shortId": "bbbb0001", "retention": {"kept": ["bbbb0001", "aaaa0002"], '
                                '"removed": 1}}')
    body = make_req()
    run([body], kube, adm, T0)
    names = sorted(n for (_, n) in kube.objs["backups"])
    # old1's snapshot was pruned; other repositories and other source namespaces are out of scope
    assert names == ["old2", "other-ns", "other-repo", "r1"]


def test_failed_job_message_names_the_worker_error_code(kube, adm):
    """kubectl get backuprequests shows why the worker failed, not only BackoffLimitExceeded."""
    kube.job_result = "Failed"
    kube.termination_message = '{"error": "repository-password"}'
    body = make_req(maxAttempts=1)
    run([body], kube, adm, T0)
    assert body["status"]["phase"] == "Failed"
    assert "(repository-password)" in body["status"]["message"]
    kube.termination_message = '{"error": "not a code; rm -rf"}'  # unsanitized text never reaches status
    body2 = make_req(name="r2", uid="fedcba9876543210", maxAttempts=1)
    run([body2], kube, adm, T0)
    assert "rm -rf" not in body2["status"]["message"] and "()" not in body2["status"]["message"]


def test_temporary_pvc_uses_the_source_storage_class(kube, adm, monkeypatch):
    """Clusters without a default StorageClass: the clone takes the source PVC's class unless the run or
    DEFAULT_STORAGE_CLASS names one."""
    monkeypatch.setattr(cfg, "DEFAULT_STORAGE", "")
    kube.objs["pvc"][("app", "data")]["spec"]["storageClassName"] = "fast-csi"
    seen = []
    orig = kube.create_pvc
    kube.create_pvc = lambda ns, body: (seen.append(body["spec"].get("storageClassName")), orig(ns, body))[1]
    run([make_req()], kube, adm, T0)
    assert seen and seen[0] == "fast-csi"
    seen.clear()
    monkeypatch.setattr(cfg, "DEFAULT_STORAGE", "default-sc")
    run([make_req(name="r2", uid="feedbeeffeedbeef")], kube, adm, T0)
    assert seen and seen[0] == "default-sc"
    seen.clear()
    run([make_req(name="r3", uid="abcdabcdabcdabcd", storageClass="explicit")], kube, adm, T0)
    assert seen and seen[0] == "explicit"
