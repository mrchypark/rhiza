FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -o rhiza ./cmd/rhiza

FROM alpine:3.19

RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza \
    && mkdir -p /data && chown 65532:65532 /data

WORKDIR /data
ENV RHIZA_DATA_DIR=/data

COPY --from=builder /app/rhiza /usr/local/bin/rhiza
VOLUME ["/data"]

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["rhiza"]
