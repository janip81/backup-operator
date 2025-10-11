import random, string, json
from .kubectl import run

def rand_suffix(n=5):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def get_pvc_size(ns, pvc):
    out = run(["kubectl", "-n", ns, "get", "pvc", pvc, "-o", "json"])
    return json.loads(out)["spec"]["resources"]["requests"]["storage"]

def get_job_logs(ns, job, tail=100):
    try:
        pods_json = run(["kubectl", "-n", ns, "get", "pods", "-l", f"job-name={job}", "-o", "json"])
        pods = json.loads(pods_json).get("items", [])
        if not pods:
            return "[WARN] No pods found for job."
        pod = pods[0]["metadata"]["name"]
        print(f"[INFO] Fetching logs from {pod}...")
        logs = run(["kubectl", "-n", ns, "logs", pod, "--tail", str(tail)], capture=True, check=False)
        return logs.strip() or "[INFO] (no logs)"
    except Exception as e:
        return f"[WARN] Could not get logs: {e}"
