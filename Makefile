# ============================================
# Backup Operator Makefile
# Builds and pushes both controller and worker images
# ============================================

REPO            := backup-operator
ORG             := janip81
REGISTRY        := ghcr.io
IMAGE_BASE      := $(REGISTRY)/$(ORG)/$(REPO)
TAG             ?= latest

# Derived image names
CONTROLLER_IMG  := $(IMAGE_BASE)-controller
WORKER_IMG      := $(IMAGE_BASE)-worker

.PHONY: all build push clean build-controller build-worker push-controller push-worker tag

# Default target
all: build

# ============================================
# Build Targets
# ============================================

build: build-controller build-worker

build-controller:
	docker build -t $(CONTROLLER_IMG):$(TAG) -f controller/Dockerfile .

build-worker:
	docker build -t $(WORKER_IMG):$(TAG) -f worker/Dockerfile .

# ============================================
# Push Targets
# ============================================

push: push-controller push-worker

push-controller:
	docker push $(CONTROLLER_IMG):$(TAG)

push-worker:
	docker push $(WORKER_IMG):$(TAG)

# ============================================
# Tag & Push Versioned Images
# ============================================

# Usage: make tag TAG=v0.1
tag:
	@if [ -z "$(TAG)" ]; then echo "❌ TAG not set! Use make tag TAG=v0.1"; exit 1; fi
	@echo "🏷️  Tagging controller and worker images with $(TAG)..."
	docker tag $(CONTROLLER_IMG):latest $(CONTROLLER_IMG):$(TAG)
	docker tag $(WORKER_IMG):latest $(WORKER_IMG):$(TAG)
	@echo "📤 Pushing both $(TAG) and latest tags to GHCR..."
	docker push $(CONTROLLER_IMG):$(TAG)
	docker push $(WORKER_IMG):$(TAG)
	docker push $(CONTROLLER_IMG):latest
	docker push $(WORKER_IMG):latest
	@echo "✅ Done — pushed controller and worker as $(TAG) and latest."

# ============================================
# Clean Targets
# ============================================

clean:
	-docker rmi $(CONTROLLER_IMG):$(TAG) || true
	-docker rmi $(WORKER_IMG):$(TAG) || true

# ============================================
# Local test runs
# ============================================

run-worker:
	docker run --rm -it \
		-e SOURCE_NAMESPACE=home-assistant \
		-e SOURCE_PVC=home-assistant-home-assistant \
		-e SNAPSHOT_CLASS=vsphere-csi-snapclass \
		-e BACKUP_NAMESPACE=backup \
		-e REMOTE_PATH=backup@starbase:/s3/test-backup/ \
		-e SSH_PRIVATE_KEY="$$(cat ~/.ssh/id_ed25519_backup)" \
		$(WORKER_IMG):$(TAG)

run-controller:
	docker run --rm -it \
		-v ~/.kube/config:/root/.kube/config:ro \
		-e WORKER_IMAGE=$(WORKER_IMG):$(TAG) \
		$(CONTROLLER_IMG):$(TAG)

run-operator:
	PYTHONPATH=. kopf run -m controller -A