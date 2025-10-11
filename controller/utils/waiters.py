import time, json
from .kubectl import run

def wait_for_snapshot(ns, name, timeout=600):
    print(f"[INFO] Waiting for snapshot {ns}/{name}...")
    start = time.time()
    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "volumesnapshot", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"Snapshot {name} deleted during wait")
        if json.loads(out).get("status", {}).get("readyToUse"):
            print(f"[INFO] Snapshot {name} ready.")
            return True
        time.sleep(5)
    raise TimeoutError(f"Snapshot {name} not ready after {timeout}s")

def wait_for_pvc_bound(ns, name, timeout=120):
    print(f"[INFO] Waiting for PVC {ns}/{name} to Bind...")
    start = time.time()
    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "pvc", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"PVC {name} deleted during wait")
        pvc = json.loads(out)
        phase = pvc.get("status", {}).get("phase")
        if phase == "Bound":
            print(f"[INFO] PVC {name} Bound.")
            return True
        time.sleep(3)
    print(f"[WARN] PVC {name} not bound after {timeout}s — continuing anyway.")
    return False

def wait_for_job(ns, name, timeout=7200):
    print(f"[INFO] Waiting for Job {ns}/{name}...")
    start = time.time()
    while time.time() - start < timeout:
        out = run(["kubectl", "-n", ns, "get", "job", name, "-o", "json"], ignore_not_found=True)
        if not out:
            raise FileNotFoundError(f"Job {name} deleted during wait")
        job = json.loads(out)
        for c in job.get("status", {}).get("conditions", []):
            if c.get("type") == "Complete" and c.get("status") == "True":
                print(f"[INFO] Job {name} succeeded.")
                return "Succeeded"
            if c.get("type") == "Failed" and c.get("status") == "True":
                print(f"[ERROR] Job {name} failed.")
                return "Failed"
        time.sleep(5)
    raise TimeoutError(f"Job {name} timeout after {timeout}s")
