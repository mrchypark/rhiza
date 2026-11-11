FROM mirror.gcr.io/library/golang:1.27.4-alpine@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -o rhiza ./cmd/rhiza

FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza \
    && mkdir -p /data && chown 65532:65532 /data

WORKDIR /data
ENV RHIZA_DATA_DIR=/data

COPY --from=builder /app/rhiza /usr/local/bin/rhiza
VOLUME ["/data"]

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["rhiza"]
