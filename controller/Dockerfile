# Controller image. Base image pinned by digest; bump deliberately.
FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

# No kubectl: the controller talks to the API through the Python client with request timeouts.

WORKDIR /app
COPY controller/requirements.txt controller/requirements.txt
RUN pip install -r controller/requirements.txt
COPY controller controller

RUN useradd -u 10001 -M -s /usr/sbin/nologin appuser
USER 10001

ENTRYPOINT ["kopf", "run", "-m", "controller", "--all-namespaces"]
