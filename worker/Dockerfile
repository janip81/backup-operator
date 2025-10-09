# Minimal worker for transfer + custom scripts
FROM alpine:3.19

RUN apk add --no-cache bash coreutils ca-certificates openssh rsync tzdata curl \
    && update-ca-certificates

# Optional: kubectl for custom scripts that want to interact with K8s
ARG KUBECTL_VERSION=v1.30.4
RUN curl -fsSL -o /usr/local/bin/kubectl https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl

# Non-root
RUN adduser -D -u 10002 worker
USER worker

WORKDIR /work
ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["echo Worker ready"]
