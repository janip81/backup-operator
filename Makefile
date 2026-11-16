# ============================================
# Backup Operator Makefile
# Local builds and checks only. Published images are built by CI from committed code
# (see .github/workflows/build-release.yaml). Pushing CI-owned tags by hand is refused.
# ============================================

REGISTRY        := ghcr.io
IMAGE_BASE      := $(REGISTRY)/janip81/backup-operator
TAG             ?= local
CONTROLLER_IMG  := $(IMAGE_BASE)-controller
WORKER_IMG      := $(IMAGE_BASE)-worker
PY_IMAGE        := python:3.13-slim

.PHONY: all build build-controller build-worker push guard-tag lint test check clean run-operator

all: build

build: build-controller build-worker

build-controller:
	docker build -t $(CONTROLLER_IMG):$(TAG) -f controller/Dockerfile .

build-worker:
	docker build -t $(WORKER_IMG):$(TAG) -f worker/Dockerfile .

# Only throwaway tags (local*, dev-*, test-*) may be pushed by hand.
guard-tag:
	@case "$(TAG)" in local*|dev-*|test-*) ;; *) echo "Refusing to push tag '$(TAG)': CI owns release tags (see #14)"; exit 1;; esac

push: guard-tag
	docker push $(CONTROLLER_IMG):$(TAG)
	docker push $(WORKER_IMG):$(TAG)

# Lint and tests run in a container so the host Python version doesn't matter.
check:
	docker run --rm --user "$(shell id -u):$(shell id -g)" -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 \
	  -v "$(CURDIR):/src" -w /src $(PY_IMAGE) sh -c \
	  "pip install -q --user -r requirements-dev.txt && ~/.local/bin/ruff check --no-cache . && python -m pytest -q -p no:cacheprovider"

lint test: check

clean:
	-docker rmi $(CONTROLLER_IMG):$(TAG) $(WORKER_IMG):$(TAG)

# Run against the current kube context. Use a disposable cluster only.
run-operator:
	PYTHONPATH=. kopf run -m controller --all-namespaces --verbose
