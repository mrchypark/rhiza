FROM public.ecr.aws/docker/library/alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 rhiza
ARG BINARY
COPY --chmod=0755 ${BINARY} /usr/local/bin/rhiza-entrypoint
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rhiza-entrypoint"]
