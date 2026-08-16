# Stage 1: Build the Go binary
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install standard build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go.mod and go.sum and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o dbbridge cmd/dbbridge/main.go

# Stage 2: Final minimal execution image
FROM alpine:3.21

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

# Run as an unprivileged user. Without this the process is root, which turns any
# file-handling defect into "write anywhere in the container".
RUN adduser -D -u 10001 dbbridge

# Copy binary from builder
COPY --from=builder /app/dbbridge .

# Create results directory owned by the runtime user
RUN mkdir -p /app/results && chown -R dbbridge:dbbridge /app/results

USER 10001:10001

EXPOSE 8080 8081 9090

ENTRYPOINT ["/app/dbbridge"]
CMD ["-config", "configs/dbbridge.yaml"]
