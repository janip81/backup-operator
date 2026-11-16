"""In-memory stand-in for controller.kube.Kube, with simple controller behaviour and fault injection."""
from __future__ import annotations

import copy
from collections import defaultdict

from controller.kube import TransientApiError


class FakeKube:
    def __init__(self):
        self.objs: dict[str, dict] = defaultdict(dict)  # kind -> {(ns,name): obj}
        self.calls = 0
        self.fail_next = 0  # raise TransientApiError for the next N calls
        self.created = defaultdict(list)  # kind -> names created (in order)
        self.max_live_snapshots = 0
        # behaviour knobs
        self.snapshot_error: str | None = None
        self.job_result = "Complete"  # Complete | Failed | None (keeps running)
        self.wffc = False
        self.hooks = {}  # method name -> callable(*args) that may raise
        self.termination_message = '{"snapshotId": "abc123", "filesNew": 3}'
        self.cms = {("app", "my-plugin"): {"data": {"plugin.sh": "echo hi"}}}
        self.secrets = {("app", "creds"): {"data": {"RESTIC_PASSWORD": "cHc="}}}

    # --- helpers ---
    def _tick(self):
        self.calls += 1
        if self.fail_next:
            self.fail_next -= 1
            raise TransientApiError("simulated API outage")

    def _get(self, kind, ns, name):
        self._tick()
        o = self.objs[kind].get((ns, name))
        return copy.deepcopy(o) if o else None

    def _create(self, kind, ns, body):
        self._tick()
        key = (ns, body["metadata"]["name"])
        if key not in self.objs[kind]:
            self.uid_seq = getattr(self, "uid_seq", 0) + 1
            self.objs[kind][key] = copy.deepcopy(body)
            self.objs[kind][key]["metadata"]["uid"] = f"uid-{self.uid_seq}"
            self.objs[kind][key]["metadata"]["resourceVersion"] = str(self.uid_seq)
            self.created[kind].append(key[1])
        live = len(self.objs["snapshot"])
        self.max_live_snapshots = max(self.max_live_snapshots, live)

    def _delete(self, kind, ns, name, uid=None):
        self._tick()
        o = self.objs[kind].get((ns, name))
        if o is not None and uid and (o.get("metadata") or {}).get("uid") != uid:
            return  # UID precondition failed (409): a different object now has this name
        self.objs[kind].pop((ns, name), None)

    def add_source_pvc(self, ns, name, phase="Bound"):
        self.objs["pvc"][(ns, name)] = {"metadata": {"name": name, "namespace": ns},
                                        "spec": {"resources": {"requests": {"storage": "1Gi"}}},
                                        "status": {"phase": phase}}

    def settle(self):
        """Simulate the snapshot, PV and Job controllers making progress."""
        for o in self.objs["snapshot"].values():
            if self.snapshot_error:
                o["status"] = {"error": {"message": self.snapshot_error}, "readyToUse": False}
            else:
                o["status"] = {"readyToUse": True, "restoreSize": "1Gi"}
        for o in self.objs["pvc"].values():
            if not self.wffc:
                o.setdefault("status", {})["phase"] = "Bound"
        for o in self.objs["job"].values():
            if self.job_result and not (o.get("status") or {}).get("conditions"):
                o["status"] = {"conditions": [{"type": self.job_result, "status": "True", "reason": "x"}],
                               "completionTime": "2026-11-17T03:01:00Z" if self.job_result == "Complete" else None}

    def list_job_pods(self, ns, job):
        self._tick()
        j = self.objs["job"].get((ns, job))
        if not j or not (j.get("status") or {}).get("conditions"):
            return []
        code = 0 if j["status"]["conditions"][0]["type"] == "Complete" else 1
        return [{"status": {"containerStatuses": [{"state": {"terminated": {
            "exitCode": code, "reason": "Completed" if code == 0 else "Error", "message": self.termination_message}}}]}}]

    # --- Kube API surface ---
    def get_pvc(self, ns, name):
        return self._get("pvc", ns, name)

    def create_pvc(self, ns, body):
        self._create("pvc", ns, body)

    def delete_pvc(self, ns, name, uid=None):
        self._delete("pvc", ns, name, uid)

    def get_storage_class(self, name):
        self._tick()
        return {"volumeBindingMode": "WaitForFirstConsumer" if self.wffc else "Immediate"}

    def get_snapshot(self, ns, name):
        return self._get("snapshot", ns, name)

    def create_snapshot(self, ns, body):
        self._create("snapshot", ns, body)

    def delete_snapshot(self, ns, name, uid=None):
        self._delete("snapshot", ns, name, uid)

    def list_snapshots(self, ns, selector):
        self._tick()
        want = dict(kv.split("=", 1) for kv in selector.split(","))
        return [copy.deepcopy(o) for (n, _), o in self.objs["snapshot"].items()
                if n == ns and all(o["metadata"].get("labels", {}).get(k) == v for k, v in want.items())]

    def get_job(self, ns, name):
        return self._get("job", ns, name)

    def create_job(self, ns, body):
        self._create("job", ns, body)

    def delete_job(self, ns, name, uid=None):
        self._delete("job", ns, name, uid)

    def get_secret(self, ns, name):
        self._tick()
        if (ns, name) in self.secrets:
            return copy.deepcopy(self.secrets[(ns, name)])
        return copy.deepcopy(self.objs["secret"].get((ns, name)))

    def create_secret(self, ns, body):
        self._create("secret", ns, body)

    def delete_secret(self, ns, name, uid=None):
        self._delete("secret", ns, name, uid)

    def get_configmap(self, ns, name):
        self._tick()
        if (ns, name) in self.cms:
            return copy.deepcopy(self.cms[(ns, name)])
        return copy.deepcopy(self.objs["configMap"].get((ns, name)))

    def create_configmap(self, ns, body):
        self._create("configMap", ns, body)

    def delete_configmap(self, ns, name, uid=None):
        self._delete("configMap", ns, name, uid)

    def list_owned(self, kind, ns, selector):
        self._tick()
        want = dict(kv.split("=", 1) for kv in selector.split(","))
        return [copy.deepcopy(o) for (n, _), o in self.objs[kind].items()
                if n == ns and all(((o.get("metadata") or {}).get("labels") or {}).get(k) == v
                                   for k, v in want.items())]

    def delete(self, kind, ns, name, uid=None):
        self._delete(kind, ns, name, uid)

    def create_cr(self, plural, ns, body):
        if "create_cr" in self.hooks:
            self.hooks["create_cr"](plural, ns, body)
        self._create(plural, ns, body)

    def list_crs_ns(self, plural, ns, selector):
        return self.list_owned(plural, ns, selector)

    def delete_cr(self, plural, ns, name, uid=None):
        self._delete(plural, ns, name, uid)

    def get_cr(self, plural, ns, name):
        return self._get(plural, ns, name)

    def patch_cr_status(self, plural, ns, name, status, resource_version=None):
        self._tick()
        o = self.objs[plural].get((ns, name))
        if o is None:
            raise TransientApiError("HTTP 404 Not Found")
        if resource_version and o["metadata"].get("resourceVersion") != resource_version:
            raise TransientApiError("HTTP 409 Conflict")
        o.setdefault("status", {}).update(status)
        self.uid_seq = getattr(self, "uid_seq", 0) + 1
        o["metadata"]["resourceVersion"] = str(self.uid_seq)
