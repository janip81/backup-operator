"""Smoke test: the controller package and its handlers import cleanly."""
import importlib


def test_controller_imports():
    for module in (
        "controller.handlers.backup_request",
        "controller.handlers.backup_schedule",
        "controller.metrics",
    ):
        importlib.import_module(module)
