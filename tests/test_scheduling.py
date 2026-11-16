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
