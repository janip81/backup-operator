FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    KOPF_LOGLEVEL=DEBUG

# Install kubectl and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git tzdata \
    && curl -fsSL -o /usr/local/bin/kubectl \
         https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for caching
COPY controller/requirements.txt /app/controller/requirements.txt
RUN pip install --no-cache-dir -r /app/controller/requirements.txt

# Copy source code
COPY controller /app/controller
ENV PYTHONPATH=/app

# Create non-root user
RUN useradd -u 10001 -m appuser
USER appuser
WORKDIR /app/controller

# Run Kopf directly on handler file
ENTRYPOINT ["kopf", "run", "--standalone", "--all-namespaces", "/app/controller/__main__.py"]
