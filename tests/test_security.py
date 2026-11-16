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
    docs = list(yaml.safe_load_all(open("k8s/manifests/02-rbac.yaml")))
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
