# Worker image: transfer tools only. No kubectl and no Kubernetes API access by design.
FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f

RUN apk add --no-cache bash coreutils ca-certificates openssh-client rsync tzdata

RUN adduser -D -u 10002 worker
USER 10002

WORKDIR /work
ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["echo Worker ready"]
