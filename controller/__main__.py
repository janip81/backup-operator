#!/usr/bin/env python3
"""kopf entrypoint: importing the handler modules registers them with kopf."""
import importlib

import kopf
from prometheus_client import start_http_server

for module in (
    "controller.handlers.backup_request",
    "controller.handlers.backup_schedule",
):
    importlib.import_module(module)


@kopf.on.startup()
def startup(logger, **_):
    try:
        start_http_server(8080)
        logger.info("Metrics endpoint started on :8080/metrics")
    except Exception as e:  # noqa: BLE001 - metrics must not block startup
        logger.warning(f"Could not start metrics server: {e}")
    logger.info("Backup Operator is running and handlers are registered.")
