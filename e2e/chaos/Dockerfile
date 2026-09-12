FROM alpine:3.19
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 rhiza
ARG BINARY
COPY ${BINARY} /usr/local/bin/rhiza-entrypoint
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rhiza-entrypoint"]
