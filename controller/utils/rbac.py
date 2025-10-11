from datetime import datetime
import yaml
from .kubectl import run
from .status import labels_yaml
from ..config import SERVICE_ACCOUNT, IMAGE_PULL_SECRET

def ensure_rbac(ns, job, req, ts=None):
    ts = ts or datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    manifest = f"""
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {SERVICE_ACCOUNT}
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {SERVICE_ACCOUNT}-role
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
rules:
  - apiGroups: [""]
    resources: ["pods","pods/log","persistentvolumeclaims","configmaps","secrets"]
    verbs: ["get","list","watch","create","patch","update","delete"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get","list","create","delete","watch"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshots"]
    verbs: ["get","list","create","delete","watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {SERVICE_ACCOUNT}-binding
  namespace: {ns}
  labels:
{labels_yaml('rbac-' + SERVICE_ACCOUNT, req, ts, indent=4)}
subjects:
- kind: ServiceAccount
  name: {SERVICE_ACCOUNT}
  namespace: {ns}
roleRef:
  kind: Role
  name: {SERVICE_ACCOUNT}-role
  apiGroup: rbac.authorization.k8s.io
"""
    run(["kubectl", "apply", "-f", "-"], input=manifest)
    print(f"[INFO] RBAC ensured in {ns}")

def copy_secret(name, src_ns, dst_ns):
    import subprocess
    try:
        src_yaml = run(["kubectl", "-n", src_ns, "get", "secret", name, "-o", "yaml"])
        data = yaml.safe_load(src_yaml)
        data["metadata"]["namespace"] = dst_ns
        for f in ["resourceVersion","uid","creationTimestamp","managedFields"]:
            data["metadata"].pop(f, None)
        run(["kubectl", "apply", "-f", "-"], input=yaml.safe_dump(data, sort_keys=False))
        print(f"[INFO] Secret {name} copied {src_ns} → {dst_ns}")
    except subprocess.CalledProcessError:
        print(f"[WARN] Secret {name} not found in {src_ns}")

def copy_configmap(cm, key, src_ns, dst_ns):
    src_yaml = run(["kubectl", "-n", src_ns, "get", "configmap", cm, "-o", "yaml"])
    data = yaml.safe_load(src_yaml)
    data["metadata"]["namespace"] = dst_ns
    data["metadata"]["name"] = cm
    for f in ["resourceVersion","uid","creationTimestamp","managedFields"]:
        data["metadata"].pop(f, None)
    run(["kubectl", "apply", "-f", "-"], input=yaml.safe_dump(data, sort_keys=False))
    print(f"[INFO] ConfigMap {cm}:{key} copied {src_ns} → {dst_ns}")
    return cm
