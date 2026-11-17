# Worker image: restic plus the built-in plugins. No kubectl and no Kubernetes API access by design.
FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f

RUN apk add --no-cache bash coreutils findutils ca-certificates openssh-client tzdata \
      'restic~0.18' 'sqlite~3.49' 'jq~1.8'

COPY worker/bo-worker /usr/local/bin/bo-worker
COPY worker/plugins/ /plugins/
RUN chmod 0755 /usr/local/bin/bo-worker /plugins/*.sh

# The controller runs the pod as root with every capability dropped except DAC_READ_SEARCH
# (read any file on the snapshot clone), a read-only root filesystem and no service account token.
USER 10002
WORKDIR /tmp
ENTRYPOINT ["/usr/local/bin/bo-worker"]
CMD ["backup"]
