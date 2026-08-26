FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOEXPERIMENT=arenas,greenteagc go build -o rhiza ./cmd/rhiza

FROM alpine:3.19

RUN apk --no-cache add ca-certificates && adduser -D -u 65532 rhiza

WORKDIR /app

COPY --from=builder /app/rhiza .

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["./rhiza"]
