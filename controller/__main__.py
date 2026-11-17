#!/usr/bin/env python3
"""kopf entrypoint: importing the handler modules registers them with kopf."""
import importlib
import logging

import kopf
from prometheus_client import start_http_server

from controller import config as cfg

for module in (
    "controller.handlers.backup_request",
    "controller.handlers.backup_schedule",
    "controller.handlers.observability",
):
    importlib.import_module(module)


@kopf.on.startup()
def startup(settings: kopf.OperatorSettings, logger, **_):
    # Bound every kopf API call and watch (#8); keep kopf's own events quiet.
    settings.networking.connect_timeout = 10
    settings.networking.request_timeout = 60
    settings.watching.server_timeout = 300
    settings.watching.client_timeout = 330
    settings.posting.level = logging.WARNING
    settings.persistence.finalizer = cfg.FINALIZER
    # kopf logs every successful timer run at INFO; the reconciler logs what matters itself.
    logging.getLogger("kopf.objects").setLevel(logging.WARNING)
    try:
        start_http_server(8080)
        logger.info("Metrics endpoint started on :8080/metrics")
    except Exception as e:  # noqa: BLE001 - metrics must not block startup
        logger.warning(f"Could not start metrics server: {e}")
    logger.info("Backup Operator is running and handlers are registered.")
