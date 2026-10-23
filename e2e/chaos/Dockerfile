FROM registry.k8s.io/kubectl@sha256:9cd9070def4060229de73314aef753caff194eebe2ca1f04d21f22320932c0c0 AS kubectl

FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 rhiza
ARG BINARY
COPY --chmod=0755 ${BINARY} /usr/local/bin/rhiza-entrypoint
COPY --from=kubectl /bin/kubectl /usr/local/bin/kubectl
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rhiza-entrypoint"]
