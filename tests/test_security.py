"""Security properties (#13): no secret material in status, records, logs or metrics; least privilege."""
import logging
from datetime import timedelta

import yaml

from controller import engine, jobspec
from tests.fakekube import FakeKube
from tests.test_engine import T0, apply, make_req, run

CANARY = "Q2FuYXJ5LXNlY3JldC12YWx1ZS0xMjM0"  # base64 of a synthetic secret value


def test_secret_values_never_leave_the_secret(caplog):
    caplog.set_level(logging.DEBUG)
    kube = FakeKube()
    kube.add_source_pvc("app", "data")
    kube.secrets[("app", "creds")] = {"data": {"RESTIC_PASSWORD": CANARY, "AWS_SECRET_ACCESS_KEY": CANARY}}
    kube.termination_message = '{"shortId": "abcd1234", "password": "' + CANARY + '", "error": null}'
    adm = engine.Admission(ready=True)
    ok, failing = make_req(), make_req("r2", pvc="nope", uid="fedcba9876543210", maxAttempts=1)
    run([ok, failing], kube, adm, T0)
    blobs = [repr(ok["status"]), repr(failing["status"]), repr(dict(kube.objs["backups"])),
             repr(dict(kube.objs["job"])), caplog.text]
    from prometheus_client import generate_latest
    blobs.append(generate_latest().decode())
    for blob in blobs:
        assert CANARY not in blob and "Canary-secret" not in blob
    assert "password" not in kube.objs["backups"][("app", "r1")]["status"]["artifact"]  # unknown fields dropped


def test_job_never_carries_credentials_in_env_or_args():
    req = engine.Req.from_body(make_req())
    job = jobspec.worker_job(req, req.names())
    c = job["spec"]["template"]["spec"]["containers"][0]
    assert all("valueFrom" not in e for e in c["env"])  # no secretKeyRef env vars either
    assert not any(k in e["name"] for e in c["env"] for k in ("PASSWORD", "SECRET", "KEY_ID", "TOKEN"))
    assert c["args"] == ["backup"]


def test_non_root_worker_mode_has_no_capabilities():
    req = engine.Req.from_body(make_req(worker={"runAsUser": 1000}))
    sc = jobspec.worker_job(req, req.names())["spec"]["template"]["spec"]["containers"][0]["securityContext"]
    assert sc["runAsNonRoot"] and sc["runAsUser"] == 1000 and sc["capabilities"] == {"drop": ["ALL"]}


def test_operator_rbac_is_least_privilege():
    docs = list(yaml.safe_load_all(open("k8s/base/rbac.yaml")))
    rules = next(d for d in docs if d["kind"] == "ClusterRole")["rules"]
    by_resource = {}
    for r in rules:
        for res in r["resources"]:
            by_resource.setdefault(res, set()).update(r["verbs"])
    assert by_resource["secrets"] == {"get"} and by_resource["configmaps"] == {"get"}
    assert by_resource["pods"] == {"get", "list"} and "pods/log" not in by_resource
    for forbidden in ("roles", "rolebindings", "serviceaccounts", "volumesnapshotcontents", "*"):
        assert forbidden not in by_resource
    assert not any("*" in r["verbs"] or "update" in r["verbs"] for r in rules)


def test_failure_messages_name_secrets_but_never_show_values(caplog):
    kube = FakeKube()
    kube.add_source_pvc("app", "data")
    kube.secrets[("app", "creds")] = {"data": {"AWS_SECRET_ACCESS_KEY": CANARY}}  # no RESTIC_PASSWORD
    body = make_req(maxAttempts=1)
    run([body], kube, engine.Admission(ready=True), T0 + timedelta(minutes=1))
    assert body["status"]["reason"] == "CredentialsNotFound" and "app/creds" in body["status"]["message"]
    assert CANARY not in repr(body["status"]) + caplog.text
    apply(body, None)


def test_artifact_is_sanitized():
    raw = {"snapshotId": "ab" * 32, "shortId": "abababab", "filesNew": 3, "bytesAdded": -1, "totalFiles": True,
           "plugin": "files", "verified": "yes", "error": "Fatal: open /creds: key=" + CANARY,
           "retention": {"kept": ["abababab", "not hex!", CANARY], "removed": 1}, "extra": CANARY}
    out = engine.sanitize_artifact(raw)
    assert out == {"snapshotId": "ab" * 32, "shortId": "abababab", "filesNew": 3, "plugin": "files",
                   "retention": {"kept": ["abababab"], "removed": 1}}
    assert engine.sanitize_artifact({"error": "repository-unavailable"}) == {"error": "repository-unavailable"}


import pytest  # noqa: E402


@pytest.mark.parametrize("url,ok", [
    ("s3:https://s3.example.com/backups/app", True),
    ("s3:https://s3.example.com:9000/b", True),
    ("rest:https://rest.example/repo", True),
    ("sftp:backup@nas.example:/srv/restic/app", True),
    ("s3:https://key:secret@s3.example.com/b", False),
    ("rest:https://user:" + CANARY + "@rest.example/repo", False),
    ("sftp:backup:pw@nas.example:/srv", False),
    ("s3:https://s3.example.com/b?access_key=x", False),
    ("local:/tmp/repo", False),
])
def test_repository_urls_never_carry_credentials(url, ok):
    req = engine.Req.from_body(make_req(repository={"url": url, "credentialsSecret": "creds"}))
    if ok:
        jobspec.validate_repository(req)
    else:
        with pytest.raises(engine.AttemptFailed):
            jobspec.validate_repository(req)


def test_non_root_mode_can_read_credentials():
    req = engine.Req.from_body(make_req(worker={"runAsUser": 1000, "runAsGroup": 2000}))
    pod = jobspec.worker_job(req, req.names())["spec"]["template"]["spec"]
    assert pod["securityContext"]["fsGroup"] == 2000
    creds = next(v for v in pod["volumes"] if v["name"] == "creds")
    assert creds["secret"]["defaultMode"] == 0o440


@pytest.mark.parametrize("retention", [{"kept": 5, "removed": 1}, {"kept": "abababab", "removed": 1},
                                       {"kept": None, "removed": "1"}, "x", [1]])
def test_malformed_artifact_never_raises(retention):
    out = engine.sanitize_artifact({"retention": retention, "shortId": "abababab"})
    assert out["shortId"] == "abababab"
    assert out.get("retention", {"kept": []})["kept"] == []


@pytest.mark.parametrize("pvc", ["data", "nope"])  # validation failure, and a failure before validation
def test_failure_records_never_store_an_invalid_url(pvc):
    kube = FakeKube()
    kube.add_source_pvc("app", "data")
    bad = "s3:https://key:" + CANARY + "@s3.example.com/b"
    body = make_req(pvc=pvc, maxAttempts=1, repository={"url": bad, "credentialsSecret": "creds"})
    run([body], kube, engine.Admission(ready=True), T0)
    rec = kube.objs["backups"][("app", "r1")]
    assert rec["status"]["phase"] == "Failed" and rec["spec"].get("repository") is None
    assert CANARY not in repr(rec) + repr(body["status"])
