# syntax=docker/dockerfile:1
# Multi-stage Dockerfile for StreamForge Go Core & Analytics Services

FROM golang:1.24-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both core and analytics binaries statically
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/streamforge-core ./cmd/core
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/streamforge-analytics ./cmd/analytics

# Runtime minimal image
FROM alpine:3.21

RUN addgroup -g 10001 -S streamforge && \
    adduser -u 10001 -S streamforge -G streamforge

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/streamforge-core /usr/local/bin/streamforge-core
COPY --from=builder /out/streamforge-analytics /usr/local/bin/streamforge-analytics

# Security: run as non-root user 10001
USER 10001:10001

EXPOSE 8080 50051

ENTRYPOINT ["/usr/local/bin/streamforge-core"]
