FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    KOPF_LOGLEVEL=DEBUG \
    PYTHONPATH=/app

# -------------------------------------------------------------------
# Install kubectl + core utilities
# -------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git tzdata \
 && curl -fsSL -o /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
 && chmod +x /usr/local/bin/kubectl \
 && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Copy app and install requirements
# -------------------------------------------------------------------
WORKDIR /app

# Copy only requirements first for caching
COPY controller/requirements.txt controller/requirements.txt
RUN pip install --no-cache-dir -r controller/requirements.txt

# Copy full source
COPY controller controller

# -------------------------------------------------------------------
# Security: non-root user
# -------------------------------------------------------------------
RUN useradd -u 10001 -m appuser
USER appuser
WORKDIR /app

# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
# Use -m to run as a module, avoids import issues
ENTRYPOINT ["kopf", "run", "-m", "controller", "--all-namespaces"]
