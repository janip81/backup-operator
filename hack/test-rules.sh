#!/usr/bin/env bash
# Unit-test the PrometheusRule with promtool (runs in a container; needs docker).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf -- "$tmp"' EXIT
python3 - "$root/k8s/manifests/09-prometheusrule.yaml" "$tmp/rules.yaml" <<'PY'
import sys, yaml
cr = yaml.safe_load(open(sys.argv[1]))
yaml.safe_dump({"groups": cr["spec"]["groups"]}, open(sys.argv[2], "w"), sort_keys=False)
PY
cp "$root/tests/prometheus/rules_test.yaml" "$tmp/"
docker run --rm --user "$(id -u):$(id -g)" -v "$tmp:/w:ro" -w /w --entrypoint promtool \
  prom/prometheus:v3.5.0 test rules rules_test.yaml
