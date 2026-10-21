FROM mirror.gcr.io/library/golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -o rhiza ./cmd/rhiza && \
    CGO_ENABLED=0 GOOS=linux go build -o rhiza-fencer ./cmd/rhiza-fencer

FROM mirror.gcr.io/library/alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1

RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza \
    && mkdir -p /data && chown 65532:65532 /data

WORKDIR /data
ENV RHIZA_DATA_DIR=/data

COPY --from=builder /app/rhiza /usr/local/bin/rhiza
COPY --from=builder /app/rhiza-fencer /usr/local/bin/rhiza-fencer
VOLUME ["/data"]

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["rhiza"]
