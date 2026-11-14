FROM --platform=$BUILDPLATFORM mirror.gcr.io/library/golang:1.27.4-alpine@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o embedded-example ./pkg/managedbootstrap/example
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o rhiza-ha-init ./cmd/rhiza-ha-init

FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza
COPY --from=builder /app/embedded-example /usr/local/bin/embedded-example
COPY --from=builder /app/rhiza-ha-init /usr/local/bin/rhiza-ha-init
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/embedded-example"]
