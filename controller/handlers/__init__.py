# Importing sub-handlers ensures Kopf registers them
from . import backup_request, backup_schedule  # noqa: F401 - registers kopf handlers
