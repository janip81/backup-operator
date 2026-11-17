"""BackupSchedule: one run per tick, catch-up and overlap policies, no churn (#9)."""
from datetime import UTC, datetime, timedelta

import pytest

from controller import scheduling
from controller.engine import ts
from controller.kube import TransientApiError
from tests.fakekube import FakeKube

CREATED = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)


def schedule(cron="0 3 * * *", **spec):
    return {"metadata": {"name": "nightly", "namespace": "app", "uid": "sched-uid-1",
                         "creationTimestamp": ts(CREATED)},
            "spec": {"schedule": cron, "template": {"sourcePVC": "data", "snapshotClass": "snap",
                                                       "repository": {"url": "s3:https://s3.example/b",
                                                                      "credentialsSecret": "creds"}},
                     **spec},
            "status": {}}


def apply(body, patch):
    for k, v in (patch or {}).items():
        if v is None:
            body["status"].pop(k, None)
        else:
            body["status"][k] = v


TAG = scheduling.request_name("nightly", "sched-uid-1", datetime(2026, 1, 1, tzinfo=UTC)).split("-")[1]


def rn(stamp):
    return f"nightly-{TAG}-{stamp}"


def requests(kube):
    return sorted(n.replace(f"-{TAG}-", "-") for (_, n) in kube.objs["backuprequests"])


@pytest.fixture
def kube():
    k = FakeKube()
    k.add_source_pvc("app", "data")
    return k


def test_no_run_at_creation_then_exactly_one_per_tick(kube):
    s = schedule()
    for minute in range(0, 16 * 60, 1):  # timer every minute from creation until after the 03:00 tick
        apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=minute)))
    assert requests(kube) == ["nightly-202611170300"]
    req = kube.objs["backuprequests"][("app", rn("202611170300"))]
    assert req["metadata"]["ownerReferences"][0]["uid"] == "sched-uid-1"
    assert req["metadata"]["labels"]["backup.techmonkeys.se/schedule"] == "nightly"
    assert req["spec"]["sourcePVC"] == "data"


def test_status_write_failures_and_restarts_never_duplicate(kube):
    s = schedule()
    tick = datetime(2026, 11, 17, 3, 0, tzinfo=UTC)
    for i in range(20):  # status patch "lost" every time: status never changes
        scheduling.reconcile_schedule(s, kube, tick + timedelta(seconds=30 * i))
    assert requests(kube) == ["nightly-202611170300"]


def test_idle_cycles_make_no_api_calls_or_status_writes(kube):
    s = schedule()
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    calls = kube.calls
    for m in range(2, 60):
        assert scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=m)) is None
    assert kube.calls == calls


def test_downtime_runs_only_latest_tick_within_deadline(kube):
    s = schedule(cron="0 * * * *", startingDeadlineSeconds=600)
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    # operator down for 5 hours, back 5 minutes after a tick
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=5, minutes=5)))
    assert requests(kube) == ["nightly-202611161700"]


def test_tick_beyond_deadline_is_skipped_not_run_late(kube):
    s = schedule(cron="0 * * * *", startingDeadlineSeconds=600)
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=5, minutes=30)))
    assert requests(kube) == []
    assert s["status"]["lastSkipped"]["reason"] == "MissedDeadline"
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=5, minutes=31)))
    assert requests(kube) == []  # the skipped tick is not re-evaluated


def test_forbid_skips_tick_while_previous_run_active(kube):
    s = schedule(cron="0 * * * *")
    t1 = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    apply(s, scheduling.reconcile_schedule(s, kube, t1))
    kube.objs["backuprequests"][("app", rn("202611161300"))]["status"] = {"phase": "Running"}
    apply(s, scheduling.reconcile_schedule(s, kube, t1 + timedelta(hours=1)))
    assert requests(kube) == ["nightly-202611161300"]
    assert s["status"]["lastSkipped"]["reason"] == "PreviousRunActive"
    kube.objs["backuprequests"][("app", rn("202611161300"))]["status"] = {"phase": "Succeeded"}
    apply(s, scheduling.reconcile_schedule(s, kube, t1 + timedelta(hours=2)))
    assert requests(kube) == ["nightly-202611161300", "nightly-202611161500"]
    assert "lastSkipped" not in s["status"]


def test_allow_policy_creates_overlapping_runs(kube):
    s = schedule(cron="0 * * * *", concurrencyPolicy="Allow")
    t1 = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    apply(s, scheduling.reconcile_schedule(s, kube, t1))
    kube.objs["backuprequests"][("app", rn("202611161300"))]["status"] = {"phase": "Running"}
    apply(s, scheduling.reconcile_schedule(s, kube, t1 + timedelta(hours=1)))
    assert len(requests(kube)) == 2  # the engine still serializes runs per source PVC


def test_suspend_creates_nothing(kube):
    s = schedule(cron="0 * * * *", suspend=True)
    for h in range(1, 5):
        apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=h)))
    assert requests(kube) == [] and s["status"]["phase"] == "Suspended"


def test_history_limits_keep_newest_and_never_touch_active(kube):
    s = schedule(cron="0 * * * *", successfulHistoryLimit=2, failedHistoryLimit=1)
    t = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    phases = ["Succeeded", "Failed", "Succeeded", "Failed", "Succeeded"]
    for i, phase in enumerate(phases):
        apply(s, scheduling.reconcile_schedule(s, kube, t + timedelta(hours=i)))
        name = scheduling.request_name("nightly", "sched-uid-1", t + timedelta(hours=i))
        kube.objs["backuprequests"][("app", name)]["status"] = {"phase": phase}
    apply(s, scheduling.reconcile_schedule(s, kube, t + timedelta(hours=5)))  # 18:00, still running
    # newest 2 succeeded (15:00, 17:00), newest 1 failed (16:00), and the new active run
    assert requests(kube) == ["nightly-202611161500", "nightly-202611161600", "nightly-202611161700",
                              "nightly-202611161800"]


def test_foreign_request_with_the_tick_name_is_not_adopted(kube):
    s = schedule(cron="0 * * * *")
    t1 = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    kube.objs["backuprequests"][("app", rn("202611161300"))] = {"metadata": {"name": rn("202611161300"),
                                                                              "labels": {}}}
    with pytest.raises(TransientApiError):
        scheduling.reconcile_schedule(s, kube, t1)


def test_timezone_is_respected(kube):
    s = schedule(cron="0 3 * * *", timeZone="Europe/Stockholm")
    apply(s, scheduling.reconcile_schedule(s, kube, datetime(2026, 11, 17, 2, 0, tzinfo=UTC)))  # 03:00 CET
    assert requests(kube) == ["nightly-202611170200"]


def test_long_names_stay_valid():
    name = scheduling.request_name("x" * 80, "u", datetime(2026, 11, 17, 3, 0, tzinfo=UTC))
    assert len(name) <= 63 and name.endswith("202611170300")


@pytest.mark.parametrize("limit", [0, 1])
def test_completion_between_lost_status_writes_never_duplicates(kube, limit):
    s = schedule(cron="0 * * * *", successfulHistoryLimit=limit, failedHistoryLimit=limit)
    t1 = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    for i in range(6):  # status write lost every time; the run completes in between
        scheduling.reconcile_schedule(s, kube, t1 + timedelta(seconds=20 * i))
        kube.objs["backuprequests"][("app", rn("202611161300"))]["status"] = {"phase": "Succeeded"}
    assert requests(kube) == ["nightly-202611161300"]
    assert len(kube.created["backuprequests"]) == 1


def test_recreated_schedule_gets_its_own_request_names(kube):
    old = schedule(cron="0 * * * *")
    t1 = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    scheduling.reconcile_schedule(old, kube, t1)
    new = schedule(cron="0 * * * *")
    new["metadata"]["uid"] = "sched-uid-2"
    patch = scheduling.reconcile_schedule(new, kube, t1 + timedelta(seconds=30))
    assert len(kube.objs["backuprequests"]) == 2 and patch["lastRequest"] != rn("202611161300")


def test_never_runs_before_the_tick(kube):
    s = schedule(cron="0 * * * *")
    just_before = datetime(2026, 11, 16, 12, 59, 59, 500000, tzinfo=UTC)
    assert scheduling.latest_tick(s["spec"], just_before) == datetime(2026, 11, 16, 12, 0, tzinfo=UTC)
    exact = datetime(2026, 11, 16, 13, 0, tzinfo=UTC)
    assert scheduling.latest_tick(s["spec"], exact) == exact
    apply(s, scheduling.reconcile_schedule(s, kube, just_before))
    assert requests(kube) == []


def test_interval_metric(kube):
    from controller import metrics
    s = schedule(cron="0 */6 * * *")
    scheduling.reconcile_schedule(s, kube, CREATED)
    assert metrics.SCHEDULE_INTERVAL.labels(namespace="app", source="nightly")._value.get() == 6 * 3600


def test_validation_flags_broken_setup_and_skips_ticks(kube):
    s = schedule(cron="0 * * * *")
    s["spec"]["template"]["sourcePVC"] = "missing"
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    cond = s["status"]["conditions"][0]
    assert cond["status"] == "False" and cond["reason"] == "SourcePVCNotFound"
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=1, minutes=1)))
    assert requests(kube) == [] and s["status"]["lastSkipped"]["reason"] == "SourcePVCNotFound"
    from controller import metrics
    assert metrics.SCHEDULE_READY.labels(namespace="app", source="nightly")._value.get() == 0
    kube.add_source_pvc("app", "missing")  # fixed: the next tick runs and Ready flips
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(hours=2, minutes=1)))
    assert len(requests(kube)) == 1 and s["status"]["conditions"][0]["status"] == "True"


@pytest.mark.parametrize("change,reason", [
    (lambda t: t["repository"].update(url="s3:https://k:s@h/b"), "InvalidRepository"),
    (lambda t: t["repository"].update(credentialsSecret="nope"), "CredentialsNotFound"),
    (lambda t: t.update(plugin={"name": "custom", "configMapRef": {"name": "nope"}}), "PluginNotFound"),
    (lambda t: t["repository"].update(url="s3:http://minio.local/b"), "InsecureRepository"),
    (lambda t: t["repository"].update(url="sftp:backup@nas.example:/srv/r"), "CredentialsNotFound"),
])
def test_validation_reasons(kube, change, reason):
    s = schedule()
    change(s["spec"]["template"])
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    assert s["status"]["conditions"][0]["reason"] == reason


def test_spec_change_revalidates_once(kube):
    s = schedule()
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    calls = kube.calls
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=2)))
    assert kube.calls == calls  # unchanged generation: no validation calls
    s["metadata"]["generation"] = 2
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=3)))
    assert kube.calls > calls and s["status"]["observedGeneration"] == 2


def test_insecure_allowed_and_complete_sftp_secret_are_ready(kube):
    s = schedule()
    s["spec"]["template"]["repository"].update(url="s3:http://minio.local/b", allowInsecure=True)
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    assert s["status"]["conditions"][0]["status"] == "True"
    kube.secrets[("app", "sftp")] = {"data": {"RESTIC_PASSWORD": "x", "ssh-privatekey": "x", "known_hosts": "x"}}
    s2 = schedule()
    s2["spec"]["template"]["repository"] = {"url": "sftp:backup@nas.example:/srv/r", "credentialsSecret": "sftp"}
    apply(s2, scheduling.reconcile_schedule(s2, kube, CREATED + timedelta(minutes=1)))
    assert s2["status"]["conditions"][0]["status"] == "True"


def test_missing_storage_class_makes_schedule_not_ready(kube):
    """A named StorageClass that doesn't exist would leave every temporary PVC Pending until timeout."""
    kube.missing_storage_classes.add("gone")
    s = schedule()
    s["spec"]["template"]["storageClass"] = "gone"
    apply(s, scheduling.reconcile_schedule(s, kube, CREATED + timedelta(minutes=1)))
    cond = s["status"]["conditions"][0]
    assert cond["status"] == "False" and cond["reason"] == "StorageClassNotFound"


# --- whenSourceUnused: Skip (#23) ---

def _app_pod(pvc="data", phase="Running"):
    return {"metadata": {"name": "app-0"}, "status": {"phase": phase},
            "spec": {"volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": pvc}}]}}


def _finish_requests(kube, phase="Succeeded", started=None):
    for (_, _), r in kube.objs["backuprequests"].items():
        st = r.setdefault("status", {})
        if not st.get("phase"):
            st["phase"] = phase
            st["startedAt"] = ts(started or parse(r["metadata"]["annotations"]))


def parse(annotations):
    from controller.engine import parse_ts
    return parse_ts(next(iter(annotations.values())))


def _tick(s, kube, day):
    """Reconcile shortly after the 03:00 tick of the given day."""
    apply(s, scheduling.reconcile_schedule(s, kube, datetime(2026, 11, day, 3, 1, tzinfo=UTC)))


def test_unused_source_backs_up_final_state_then_pauses_and_resumes(kube):
    from controller import metrics
    s = schedule(whenSourceUnused="Skip")
    kube.pods["app"] = [_app_pod()]
    _tick(s, kube, 17)                       # app running: normal run
    assert len(requests(kube)) == 1 and s["status"]["phase"] == "Active"
    assert "sourceUnusedSince" not in s["status"]
    _finish_requests(kube)

    kube.pods["app"] = []                    # app scaled to 0
    _tick(s, kube, 18)                       # one more run captures the final state
    assert len(requests(kube)) == 2 and s["status"]["phase"] == "Active"
    assert s["status"]["sourceUnusedSince"] == ts(datetime(2026, 11, 18, 3, tzinfo=UTC))
    _finish_requests(kube)

    _tick(s, kube, 19)                       # unused and backed up: paused
    assert len(requests(kube)) == 2
    assert s["status"]["phase"] == "Paused" and s["status"]["lastSkipped"]["reason"] == "SourceUnused"
    assert metrics.SCHEDULE_PAUSED.labels(namespace="app", source="nightly")._value.get() == 1
    _tick(s, kube, 20)
    assert len(requests(kube)) == 2 and s["status"]["phase"] == "Paused"

    kube.pods["app"] = [_app_pod(phase="Pending")]   # app starting again
    _tick(s, kube, 21)
    assert len(requests(kube)) == 3 and s["status"]["phase"] == "Active"
    assert "sourceUnusedSince" not in s["status"]
    assert metrics.SCHEDULE_PAUSED.labels(namespace="app", source="nightly")._value.get() == 0


def test_unused_source_keeps_trying_until_the_final_backup_succeeds(kube):
    s = schedule(whenSourceUnused="Skip")
    kube.pods["app"] = [_app_pod(phase="Succeeded"), _app_pod(pvc="other")]  # finished pod, other volume
    _tick(s, kube, 17)
    _finish_requests(kube, phase="Failed")
    _tick(s, kube, 18)                       # final backup failed: try again, don't pause
    assert len(requests(kube)) == 2 and s["status"]["phase"] == "Active"
    _finish_requests(kube)
    _tick(s, kube, 19)
    assert s["status"]["phase"] == "Paused"


def test_old_backup_from_before_the_app_stopped_does_not_pause(kube):
    """A success from while the app was running is not the final state."""
    s = schedule(whenSourceUnused="Skip")
    kube.pods["app"] = [_app_pod()]
    _tick(s, kube, 17)
    _finish_requests(kube)
    kube.pods["app"] = []
    _tick(s, kube, 18)
    assert len(requests(kube)) == 2 and s["status"]["phase"] == "Active"


def test_default_runs_without_looking_at_pods(kube):
    s = schedule()
    kube.pods["app"] = []
    calls = []
    kube.list_pods = lambda ns: calls.append(ns) or []
    _tick(s, kube, 17)
    assert len(requests(kube)) == 1 and calls == []


def _paused_schedule(kube):
    """A Skip schedule that has paused: ran while used, ran once more unused, then paused."""
    s = schedule(whenSourceUnused="Skip")
    kube.pods["app"] = [_app_pod()]
    _tick(s, kube, 17)
    _finish_requests(kube)
    kube.pods["app"] = []
    _tick(s, kube, 18)
    _finish_requests(kube)
    _tick(s, kube, 19)
    assert s["status"]["phase"] == "Paused"
    return s


def test_resume_survives_a_lost_status_write_and_a_later_stop_waits_for_a_new_backup(kube):
    s = _paused_schedule(kube)
    kube.pods["app"] = [_app_pod()]
    when = datetime(2026, 11, 20, 3, 1, tzinfo=UTC)
    scheduling.reconcile_schedule(s, kube, when)            # request created, status write lost
    apply(s, scheduling.reconcile_schedule(s, kube, when))  # retry of the same tick
    assert s["status"]["phase"] == "Active" and "sourceUnusedSince" not in s["status"]
    assert len(requests(kube)) == 3
    _finish_requests(kube)
    kube.pods["app"] = []                                    # stopped again: needs a new final backup
    _tick(s, kube, 21)
    assert len(requests(kube)) == 4 and s["status"]["phase"] == "Active"
    assert s["status"]["sourceUnusedSince"] == ts(datetime(2026, 11, 21, 3, tzinfo=UTC))


def test_switching_back_to_run_clears_pause_between_and_on_ticks(kube):
    from controller import metrics
    s = _paused_schedule(kube)
    s["spec"]["whenSourceUnused"] = "Run"
    apply(s, scheduling.reconcile_schedule(s, kube, datetime(2026, 11, 19, 12, 0, tzinfo=UTC)))  # between ticks
    assert s["status"]["phase"] == "Active" and "sourceUnusedSince" not in s["status"]
    assert metrics.SCHEDULE_PAUSED.labels(namespace="app", source="nightly")._value.get() == 0
    _tick(s, kube, 20)
    assert len(requests(kube)) == 3 and s["status"]["phase"] == "Active"


def test_app_resuming_with_broken_credentials_is_not_reported_paused(kube):
    from controller import metrics
    s = _paused_schedule(kube)
    kube.pods["app"] = [_app_pod()]
    kube.secrets.pop(("app", "creds"))
    _tick(s, kube, 20)
    assert s["status"]["phase"] == "Active" and s["status"]["lastSkipped"]["reason"] == "CredentialsNotFound"
    assert metrics.SCHEDULE_PAUSED.labels(namespace="app", source="nightly")._value.get() == 0


def test_paused_check_uses_the_source_namespace(kube):
    s = schedule(whenSourceUnused="Skip")
    s["spec"]["template"]["sourceNamespace"] = "data-ns"
    kube.add_source_pvc("data-ns", "data")
    kube.secrets[("data-ns", "creds")] = {"data": {"RESTIC_PASSWORD": "cHc="}}
    kube.pods["app"] = []                        # schedule's own namespace: irrelevant
    kube.pods["data-ns"] = [_app_pod()]
    _tick(s, kube, 17)
    assert s["status"]["phase"] == "Active" and "sourceUnusedSince" not in s["status"]
