# Controller image. Base image pinned by digest; bump deliberately.
FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

# kubectl: pinned version, verified checksum.
ARG KUBECTL_VERSION=v1.35.10
ARG KUBECTL_SHA256=c49a28f1f4efc4d475ed05f47f2ce8e563bf7bef69ed77fe1b924ea6d83b6327
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && echo "${KUBECTL_SHA256}  /usr/local/bin/kubectl" | sha256sum -c - \
 && chmod +x /usr/local/bin/kubectl \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY controller/requirements.txt controller/requirements.txt
RUN pip install -r controller/requirements.txt
COPY controller controller

RUN useradd -u 10001 -M -s /usr/sbin/nologin appuser
USER 10001

ENTRYPOINT ["kopf", "run", "-m", "controller", "--all-namespaces"]
