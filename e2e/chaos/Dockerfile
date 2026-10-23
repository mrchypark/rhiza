FROM registry.k8s.io/kubectl@sha256:9cd9070def4060229de73314aef753caff194eebe2ca1f04d21f22320932c0c0 AS kubectl

FROM mirror.gcr.io/library/alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 rhiza
ARG BINARY
COPY --chmod=0755 ${BINARY} /usr/local/bin/rhiza-entrypoint
COPY --from=kubectl /bin/kubectl /usr/local/bin/kubectl
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rhiza-entrypoint"]
