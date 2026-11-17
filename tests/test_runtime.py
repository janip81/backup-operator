"""Admission bootstrap retries are throttled and stop once successful (#8)."""
from controller import engine, runtime
from controller.kube import TransientApiError


class FlakyKube:
    def __init__(self, failures):
        self.failures, self.lists = failures, 0

    def list_crs(self, plural):
        self.lists += 1
        if self.failures:
            self.failures -= 1
            raise TransientApiError("down")
        return [{"metadata": {"name": "a", "namespace": "app", "uid": "u1"},
                 "spec": {"sourcePVC": "data"}, "status": {"phase": "Running"}}]


def test_bootstrap_retry_is_throttled_and_stops_after_success(monkeypatch):
    clock = [1000.0]
    fake = FlakyKube(failures=2)
    monkeypatch.setattr(runtime, "ADMISSION", engine.Admission())
    monkeypatch.setattr(runtime, "kube", lambda: fake)
    monkeypatch.setattr(runtime, "_last_boot", 0.0)
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])

    assert runtime.ensure_bootstrapped() is False and fake.lists == 1
    clock[0] += 5
    assert runtime.ensure_bootstrapped() is False and fake.lists == 1  # throttled
    clock[0] += 30
    assert runtime.ensure_bootstrapped() is False and fake.lists == 2
    clock[0] += 30
    assert runtime.ensure_bootstrapped() is True and fake.lists == 3
    assert runtime.ADMISSION.active_count() == 1
    clock[0] += 300
    assert runtime.ensure_bootstrapped() is True and fake.lists == 3  # no further lists
