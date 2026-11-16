"""All YAML manifests parse, and every CRD declares a structural schema."""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _docs():
    for path in sorted((ROOT / "k8s").rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc:
                yield path, doc


def test_manifests_parse():
    assert list(_docs())


def test_crds_have_schema():
    crds = [(p, d) for p, d in _docs() if d.get("kind") == "CustomResourceDefinition"]
    assert crds
    for path, crd in crds:
        for version in crd["spec"]["versions"]:
            assert "openAPIV3Schema" in version.get("schema", {}), f"{path}: {version['name']} has no schema"
