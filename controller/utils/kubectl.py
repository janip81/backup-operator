import subprocess, sys, json

def run(cmd, input=None, capture=True, check=True, ignore_not_found=False):
    """Run kubectl and return stdout; handle 'NotFound' gracefully when requested."""
    if cmd and cmd[0] == "kubectl":
        print("[CMD]", " ".join(cmd))
    try:
        result = subprocess.run(cmd, text=True, input=input, capture_output=capture, check=check)
        if capture:
            if result.stdout.strip():
                print(result.stdout.strip())
            if result.stderr.strip():
                print(result.stderr.strip(), file=sys.stderr)
            return result.stdout.strip()
        return ""
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip() if e.stderr else ""
        if ignore_not_found and "NotFound" in stderr:
            print(f"[WARN] Resource not found for command: {' '.join(cmd)}")
            return None
        print(f"[ERROR] Command failed: {' '.join(cmd)}")
        if e.stdout:
            print("[STDOUT]", e.stdout.strip())
        if e.stderr:
            print("[STDERR]", e.stderr.strip(), file=sys.stderr)
        raise

def cleanup(ns, *resources):
    for r in resources:
        run(["kubectl", "-n", ns, "delete", r, "--ignore-not-found=true"])
