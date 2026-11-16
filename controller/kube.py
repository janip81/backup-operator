"""Thin Kubernetes API wrapper with request timeouts and normalized errors.

Every call has a connect/read timeout, so a hung API server can't block a handler indefinitely (#8).
404 on reads and deletes and 409 on creates are treated as normal outcomes; all other API failures
raise TransientApiError so the reconciler can back off instead of restarting the pipeline.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from . import config as cfg

SNAP_GROUP, SNAP_VERSION, SNAP_PLURAL = "snapshot.storage.k8s.io", "v1", "volumesnapshots"


class TransientApiError(Exception):
    """API call failed in a way that may succeed later (timeouts, 5xx, 429, network)."""


class RateLimiter:
    """Process-wide token bucket shared by all reconcile cycles (#8): caps aggregate API load,
    including the burst when many backoffs expire together after an outage."""

    def __init__(self, qps: float, burst: int, clock=time.monotonic, sleep=time.sleep):
        self.qps, self.burst, self.clock, self.sleep = qps, burst, clock, sleep
        self.tokens, self.last = float(burst), clock()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self.clock()
                self.tokens = min(self.burst, self.tokens + (now - self.last) * self.qps)
                self.last = now
                if self.tokens >= 1 - 1e-9:  # tolerance: float rounding must not stall the loop
                    self.tokens = max(self.tokens - 1, 0.0)
                    return
                wait = max((1 - self.tokens) / self.qps, 0.001)
            self.sleep(wait)


class Kube:
    def __init__(self) -> None:
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        api = client.ApiClient()
        self.core = client.CoreV1Api(api)
        self.batch = client.BatchV1Api(api)
        self.custom = client.CustomObjectsApi(api)
        self.storage = client.StorageV1Api(api)
        self.timeout = (cfg.API_CONNECT_TIMEOUT, cfg.API_READ_TIMEOUT)
        self.limiter = RateLimiter(cfg.API_QPS, cfg.API_BURST)

    # --- error handling ---
    def _call(self, fn, *args, ok_status: tuple[int, ...] = (), **kwargs) -> Any:
        self.limiter.acquire()
        try:
            return fn(*args, _request_timeout=self.timeout, **kwargs)
        except ApiException as e:
            if e.status in ok_status:
                return None
            if e.status in (400, 403, 422):
                raise  # permanent: caller decides
            raise TransientApiError(f"{fn.__name__}: HTTP {e.status} {e.reason}") from e
        except Exception as e:  # urllib3 timeouts, connection errors
            raise TransientApiError(f"{fn.__name__}: {type(e).__name__}: {e}") from e

    @staticmethod
    def _dict(obj: Any) -> dict | None:
        if obj is None:
            return None
        return obj if isinstance(obj, dict) else client.ApiClient().sanitize_for_serialization(obj)

    # --- generic helpers ---
    def get_pvc(self, ns: str, name: str) -> dict | None:
        return self._dict(self._call(self.core.read_namespaced_persistent_volume_claim, name, ns, ok_status=(404,)))

    def create_pvc(self, ns: str, body: dict) -> None:
        self._call(self.core.create_namespaced_persistent_volume_claim, ns, body, ok_status=(409,))

    @staticmethod
    def _opts(uid: str | None, **kw) -> dict:
        """Delete options; with a UID precondition the API refuses to delete a different object
        that has since taken the same name (409), so cleanup can never remove a replacement (#10)."""
        body = client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=uid) if uid else None, **kw)
        return {"body": body}

    def delete_pvc(self, ns: str, name: str, uid: str | None = None) -> None:
        self._call(self.core.delete_namespaced_persistent_volume_claim, name, ns, ok_status=(404, 409),
                   **self._opts(uid))

    def get_storage_class(self, name: str) -> dict | None:
        return self._dict(self._call(self.storage.read_storage_class, name, ok_status=(404,)))

    def get_snapshot(self, ns: str, name: str) -> dict | None:
        return self._call(self.custom.get_namespaced_custom_object, SNAP_GROUP, SNAP_VERSION, ns, SNAP_PLURAL, name,
                          ok_status=(404,))

    def create_snapshot(self, ns: str, body: dict) -> None:
        self._call(self.custom.create_namespaced_custom_object, SNAP_GROUP, SNAP_VERSION, ns, SNAP_PLURAL, body,
                   ok_status=(409,))

    def delete_snapshot(self, ns: str, name: str, uid: str | None = None) -> None:
        # Normal delete only. Finalizers are never removed: that would leave the snapshot behind in the backend.
        self._call(self.custom.delete_namespaced_custom_object, SNAP_GROUP, SNAP_VERSION, ns, SNAP_PLURAL, name,
                   ok_status=(404, 409), **self._opts(uid))

    def list_snapshots(self, ns: str, selector: str) -> list[dict]:
        res = self._call(self.custom.list_namespaced_custom_object, SNAP_GROUP, SNAP_VERSION, ns, SNAP_PLURAL,
                         label_selector=selector)
        return (res or {}).get("items", [])

    def get_job(self, ns: str, name: str) -> dict | None:
        return self._dict(self._call(self.batch.read_namespaced_job, name, ns, ok_status=(404,)))

    def create_job(self, ns: str, body: dict) -> None:
        self._call(self.batch.create_namespaced_job, ns, body, ok_status=(409,))

    def delete_job(self, ns: str, name: str, uid: str | None = None) -> None:
        self._call(self.batch.delete_namespaced_job, name, ns, ok_status=(404, 409),
                   **self._opts(uid, propagation_policy="Background"))

    def list_job_pods(self, ns: str, job: str) -> list[dict]:
        res = self._call(self.core.list_namespaced_pod, ns, label_selector=f"job-name={job}")
        return self._dict(res).get("items", []) if res else []

    def get_secret(self, ns: str, name: str) -> dict | None:
        return self._dict(self._call(self.core.read_namespaced_secret, name, ns, ok_status=(404,)))

    def create_secret(self, ns: str, body: dict) -> None:
        self._call(self.core.create_namespaced_secret, ns, body, ok_status=(409,))

    def delete_secret(self, ns: str, name: str, uid: str | None = None) -> None:
        self._call(self.core.delete_namespaced_secret, name, ns, ok_status=(404, 409), **self._opts(uid))

    def get_configmap(self, ns: str, name: str) -> dict | None:
        return self._dict(self._call(self.core.read_namespaced_config_map, name, ns, ok_status=(404,)))

    def create_configmap(self, ns: str, body: dict) -> None:
        self._call(self.core.create_namespaced_config_map, ns, body, ok_status=(409,))

    def delete_configmap(self, ns: str, name: str, uid: str | None = None) -> None:
        self._call(self.core.delete_namespaced_config_map, name, ns, ok_status=(404, 409), **self._opts(uid))

    # --- generic by kind (used by cleanup) ---
    def list_owned(self, kind: str, ns: str, selector: str) -> list[dict]:
        if kind == "snapshot":
            return self.list_snapshots(ns, selector)
        fn = {"job": self.batch.list_namespaced_job, "pvc": self.core.list_namespaced_persistent_volume_claim,
              "secret": self.core.list_namespaced_secret, "configMap": self.core.list_namespaced_config_map}[kind]
        res = self._dict(self._call(fn, ns, label_selector=selector))
        return (res or {}).get("items", [])

    def delete(self, kind: str, ns: str, name: str, uid: str | None = None) -> None:
        {"job": self.delete_job, "pvc": self.delete_pvc, "snapshot": self.delete_snapshot,
         "secret": self.delete_secret, "configMap": self.delete_configmap}[kind](ns, name, uid)

    # --- operator CRs ---
    def create_cr(self, plural: str, ns: str, body: dict) -> None:
        self._call(self.custom.create_namespaced_custom_object, cfg.GROUP, cfg.VERSION, ns, plural, body,
                   ok_status=(409,))

    def get_cr(self, plural: str, ns: str, name: str) -> dict | None:
        return self._call(self.custom.get_namespaced_custom_object, cfg.GROUP, cfg.VERSION, ns, plural, name,
                          ok_status=(404,))

    def list_crs_ns(self, plural: str, ns: str, selector: str) -> list[dict]:
        res = self._call(self.custom.list_namespaced_custom_object, cfg.GROUP, cfg.VERSION, ns, plural,
                         label_selector=selector)
        return (res or {}).get("items", [])

    def delete_cr(self, plural: str, ns: str, name: str, uid: str | None = None) -> None:
        self._call(self.custom.delete_namespaced_custom_object, cfg.GROUP, cfg.VERSION, ns, plural, name,
                   ok_status=(404, 409), **self._opts(uid))

    def list_crs(self, plural: str) -> list[dict]:
        res = self._call(self.custom.list_cluster_custom_object, cfg.GROUP, cfg.VERSION, plural)
        return (res or {}).get("items", [])

    def patch_cr_status(self, plural: str, ns: str, name: str, status: dict,
                        resource_version: str | None = None) -> None:
        # 404 is not success here: a missing record must be retried, never treated as persisted (#11).
        # With resource_version the write is conditional (409 if the object changed or was replaced).
        body: dict = {"status": status}
        if resource_version:
            body["metadata"] = {"resourceVersion": resource_version}
        self._call(self.custom.patch_namespaced_custom_object_status, cfg.GROUP, cfg.VERSION, ns, plural, name, body)
