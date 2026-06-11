# Local Development with Docker Compose

The `deploy/` directory contains everything needed to run dbbridge locally: a multi-node Docker Compose stack (two dbbridge instances + Redis + MinIO + Prometheus), Kubernetes manifests, and per-instance configs.

## Prerequisites

- Docker + Docker Compose v2
- `make` (or run the `docker compose` commands directly)
- Go 1.26+ (only for the binary-only mode below)

## Quick start — full stack (recommended)

From the **repo root**:

```bash
make up
```

This builds the dbbridge image and starts all services in the background:

| Container         | What it is                     | Host ports           |
|-------------------|--------------------------------|----------------------|
| `dbbridge-blue`   | dbbridge instance #1           | REST :8081, gRPC :9091 |
| `dbbridge-green`  | dbbridge instance #2           | REST :8082, gRPC :9092 |
| `dbbridge-redis`  | Redis 7 (shared MetaStore)     | :6379                |
| `dbbridge-minio`  | MinIO S3-compatible storage    | API :9000, UI :9001  |
| `dbbridge-prometheus` | Prometheus (scrapes both instances) | :9090        |

Both instances share Redis for coordination and MinIO for result storage, so queries submitted to one instance are visible from the other.

> **WebSocket** (`GET /v1/ws`) is served on the same REST port — no separate port is needed. Connect to `ws://localhost:8081/v1/ws` (blue) or `ws://localhost:8082/v1/ws` (green).

### Verify it's running

```bash
# Check service health
curl http://localhost:8081/v1/admin/can-stop
curl http://localhost:8082/v1/admin/can-stop

# Submit a test query (replace with a real DSN from your config)
curl -X POST http://localhost:8081/v1/queries \
  -H "Content-Type: application/json" \
  -d '{"database_id": "pg_test", "sql": "SELECT 1"}'
```

MinIO console: http://localhost:9001 (login: `minioadmin` / `minioadmin`)  
Prometheus: http://localhost:9090

### Useful make targets

```bash
make logs       # tail logs from all containers
make down       # stop and remove containers
make restart    # rebuild and restart only the dbbridge containers

make reload-config   # POST /v1/admin/reload to dbbridge-blue
make can-stop        # GET  /v1/admin/can-stop from dbbridge-blue
```

## Single-node mode (binary only, no Docker)

Useful for fast iteration without Docker overhead. Uses the in-memory MetaStore and local filesystem for results — no Redis or MinIO needed.

```bash
# 1. Build
make build

# 2. Run with the local config (metastore: memory, storage: fs)
./bin/dbbridge -config configs/dbbridge.yaml
```

The server listens on `:8080` (REST + WebSocket at `/v1/ws`) and `:9090` (gRPC).

To add target databases, edit `configs/dbbridge.yaml` under the `databases:` key before starting.

## Configuration

Each instance loads a single YAML config file passed via `-config`. Key options:

| Field | Values | Description |
|---|---|---|
| `instance.metastore` | `memory` / `redis` | `memory` = single-node; `redis` = multi-node |
| `instance.redis_addr` | host:port | Required when metastore is `redis` |
| `instance.default_storage` | `fs` / `s3` | Where query results are written |
| `server.rest_addr` | `:8080` | REST + WebSocket listen address |
| `server.grpc_addr` | `:9090` | gRPC-Connect listen address |
| `storage.fs.root` | path | Local directory for result files |
| `storage.s3.*` | — | S3/MinIO credentials and bucket |
| `databases[]` | — | List of target DB connections |

Example configs:
- `configs/dbbridge.yaml` — single-node local dev (memory + fs)
- `deploy/configs/dbbridge-blue.yaml` — multi-node (redis + s3/minio)
- `deploy/configs/dbbridge-green.yaml` — same, different instance ID

Config can be reloaded at runtime without restart:

```bash
# Send SIGHUP
kill -HUP <pid>

# Or via HTTP
curl -X POST http://localhost:8080/v1/admin/reload
```

## Directory layout

```
deploy/
├── Dockerfile                  # Multi-stage build (golang:1.26 → alpine:3.21)
├── docker-compose.yaml         # Full local dev stack
├── prometheus.yml              # Prometheus scrape config
├── configs/
│   ├── dbbridge-blue.yaml      # Config for blue instance
│   └── dbbridge-green.yaml     # Config for green instance
└── k8s/
    ├── configmap.yaml
    ├── deployment.yaml
    └── service.yaml
```

## Kubernetes

```bash
make k8s-apply    # kubectl apply -f deploy/k8s/
make k8s-delete   # kubectl delete -f deploy/k8s/
```

Before applying, review `deploy/k8s/configmap.yaml` and update Redis/S3 addresses for your cluster.
