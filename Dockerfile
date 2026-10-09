FROM public.ecr.aws/docker/library/golang:1.27-alpine@sha256:738d1cf061836894ff6bb8c33881080ac66de8cf0586615012a0c8f592649cfa AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -o rhiza ./cmd/rhiza

FROM public.ecr.aws/docker/library/alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1

RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza \
    && mkdir -p /data && chown 65532:65532 /data

WORKDIR /data
ENV RHIZA_DATA_DIR=/data

COPY --from=builder /app/rhiza /usr/local/bin/rhiza
VOLUME ["/data"]

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["rhiza"]
