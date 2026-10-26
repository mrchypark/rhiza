FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 rhiza
ARG BINARY
COPY --chmod=0755 ${BINARY} /usr/local/bin/rhiza-entrypoint
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rhiza-entrypoint"]
