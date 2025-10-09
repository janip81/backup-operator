REPO        := backup-operator-controller
IMAGE_NAME  := ghcr.io/janip81/$(REPO)
TAG         ?= latest

.PHONY: all build push clean

all: build

build:
	docker build -t $(IMAGE_NAME):$(TAG) controller/

push:
	docker push $(IMAGE_NAME):$(TAG)

clean:
	docker rmi $(IMAGE_NAME):$(TAG) || true
