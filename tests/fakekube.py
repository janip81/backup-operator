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
        self.cms = {("backup-operator", "rsync-snapshot-default-backup-script"): {"data": {"backup.sh": "echo hi"}}}
        self.secrets = {("backup-operator", "backup-ssh-key"): {"data": {"ssh_key": "a2V5"}}}

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
            self.objs[kind][key] = copy.deepcopy(body)
            self.created[kind].append(key[1])
        live = len(self.objs["snapshot"])
        self.max_live_snapshots = max(self.max_live_snapshots, live)

    def _delete(self, kind, ns, name):
        self._tick()
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
            if self.job_result:
                o["status"] = {"conditions": [{"type": self.job_result, "status": "True", "reason": "x"}]}

    # --- Kube API surface ---
    def get_pvc(self, ns, name):
        return self._get("pvc", ns, name)

    def create_pvc(self, ns, body):
        self._create("pvc", ns, body)

    def delete_pvc(self, ns, name):
        self._delete("pvc", ns, name)

    def get_storage_class(self, name):
        self._tick()
        return {"volumeBindingMode": "WaitForFirstConsumer" if self.wffc else "Immediate"}

    def get_snapshot(self, ns, name):
        return self._get("snapshot", ns, name)

    def create_snapshot(self, ns, body):
        self._create("snapshot", ns, body)

    def delete_snapshot(self, ns, name):
        self._delete("snapshot", ns, name)

    def list_snapshots(self, ns, selector):
        self._tick()
        want = dict(kv.split("=", 1) for kv in selector.split(","))
        return [copy.deepcopy(o) for (n, _), o in self.objs["snapshot"].items()
                if n == ns and all(o["metadata"].get("labels", {}).get(k) == v for k, v in want.items())]

    def get_job(self, ns, name):
        return self._get("job", ns, name)

    def create_job(self, ns, body):
        self._create("job", ns, body)

    def delete_job(self, ns, name):
        self._delete("job", ns, name)

    def get_secret(self, ns, name):
        self._tick()
        if (ns, name) in self.secrets:
            return copy.deepcopy(self.secrets[(ns, name)])
        return copy.deepcopy(self.objs["secret"].get((ns, name)))

    def create_secret(self, ns, body):
        self._create("secret", ns, body)

    def delete_secret(self, ns, name):
        self._delete("secret", ns, name)

    def get_configmap(self, ns, name):
        self._tick()
        if (ns, name) in self.cms:
            return copy.deepcopy(self.cms[(ns, name)])
        return copy.deepcopy(self.objs["configMap"].get((ns, name)))

    def create_configmap(self, ns, body):
        self._create("configMap", ns, body)

    def delete_configmap(self, ns, name):
        self._delete("configMap", ns, name)

    def list_owned(self, kind, ns, selector):
        self._tick()
        want = dict(kv.split("=", 1) for kv in selector.split(","))
        return [copy.deepcopy(o) for (n, _), o in self.objs[kind].items()
                if n == ns and all(((o.get("metadata") or {}).get("labels") or {}).get(k) == v
                                   for k, v in want.items())]

    def delete(self, kind, ns, name):
        self._delete(kind, ns, name)

    def create_cr(self, plural, ns, body):
        self._create(plural, ns, body)

    def get_cr(self, plural, ns, name):
        return self._get(plural, ns, name)

    def patch_cr_status(self, plural, ns, name, status):
        self._tick()
        o = self.objs[plural].get((ns, name))
        if o is not None:
            o.setdefault("status", {}).update(status)
