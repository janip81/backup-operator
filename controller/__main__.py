#!/usr/bin/env python3
import kopf
from prometheus_client import start_http_server
import importlib


print("[DEBUG] __main__.py executing import loop …")

for module in (
    "controller.handlers.backup_request",
    "controller.handlers.backup_schedule",
):
    print(f"[DEBUG] importing {module}")
    importlib.import_module(module)

@kopf.on.create("backup.techmonkeys.se", "v1", "backuprequests")
def debug_test(**_):
    print("[DEBUG] debug_test handler fired")

@kopf.on.startup()
def startup(logger, **_):
    try:
        start_http_server(8080)
        logger.info("Metrics endpoint started on :8080/metrics")
    except Exception as e:
        logger.warning(f"Could not start metrics server: {e}")
    logger.info("Backup Operator is running and handlers are registered.")
