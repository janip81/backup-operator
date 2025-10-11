#!/usr/bin/env python3
import os
import sys
from kopf.cli import run

# Ensure that the controller/ folder is importable
sys.path.insert(0, os.path.dirname(__file__))

if __name__ == "__main__":
    # Equivalent of: PYTHONPATH=. kopf run -m controller -A
    run(module="controller", all_namespaces=True)
