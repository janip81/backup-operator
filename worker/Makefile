# ============================================
# Backup Operator Makefile (v0.2.0)
# Builds and pushes controller + worker images
# ============================================

REPO            := backup-operator
ORG             := janip81
REGISTRY        := ghcr.io
IMAGE_BASE      := $(REGISTRY)/$(ORG)/$(REPO)
TAG             ?= 0.2.0

CONTROLLER_IMG  := $(IMAGE_BASE)-controller
WORKER_IMG      := $(IMAGE_BASE)-worker

.PHONY: all build push clean build-controller build-worker push-controller push-worker

all: build

build: build-controller build-worker

build-controller:
	docker build -t $(CONTROLLER_IMG):$(TAG) -f controller/Dockerfile controller

build-worker:
	docker build -t $(WORKER_IMG):$(TAG) -f worker/Dockerfile worker

push: push-controller push-worker

push-controller:
	docker push $(CONTROLLER_IMG):$(TAG)

push-worker:
	docker push $(WORKER_IMG):$(TAG)

clean:
	-docker rmi $(CONTROLLER_IMG):$(TAG) || true
	-docker rmi $(WORKER_IMG):$(TAG) || true
