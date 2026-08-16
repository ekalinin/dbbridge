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
| `dbbridge-blue`   | dbbridge instance #1           | REST :8081, gRPC :9091, admin :8181 |
| `dbbridge-green`  | dbbridge instance #2           | REST :8082, gRPC :9092, admin :8182 |
| `dbbridge-redis`  | Redis 7 (shared MetaStore)     | not published        |
| `dbbridge-minio`  | MinIO S3-compatible storage    | API :9000, UI :9001  |
| `dbbridge-prometheus` | Prometheus (scrapes both instances) | :9090        |

Both instances share Redis for coordination and MinIO for result storage, so queries submitted to one instance are visible from the other.

> **WebSocket** (`GET /v1/ws`) is served on the same REST port — no separate port is needed. Connect to `ws://localhost:8081/v1/ws` (blue) or `ws://localhost:8082/v1/ws` (green).

The stack requires bearer tokens. The compose file supplies development
defaults; override them with `DBBRIDGE_TOKEN_REPORTING` and
`DBBRIDGE_TOKEN_ADMIN` (and `REDIS_PASSWORD`) in the environment before
`make up`.

`/metrics` and `/v1/admin/*` are served on a separate listener, so they are not
reachable through the REST port that a load balancer would publish.

### Verify it's running

```bash
# Check service health (admin listener, admin scope)
curl -H "Authorization: Bearer dev-admin-token" http://localhost:8181/v1/admin/can-stop
curl -H "Authorization: Bearer dev-admin-token" http://localhost:8182/v1/admin/can-stop

# Submit a test query (write scope)
curl -X POST http://localhost:8081/v1/queries \
  -H "Authorization: Bearer dev-reporting-token" \
  -H "Content-Type: application/json" \
  -d '{"database_id": "dvdrental", "sql": "SELECT 1"}'
```

Only read-only statements are accepted; DML and DDL are rejected with 400
unless `defaults.allow_writes` is turned on.

MinIO console: http://localhost:9001 (login: `minioadmin` / `minioadmin`)  
Prometheus: http://localhost:9090

### Useful make targets

```bash
make logs       # tail logs from all containers
make down       # stop and remove containers
make restart    # rebuild and restart only the dbbridge containers

make reload-config   # POST /v1/admin/reload to dbbridge-blue (admin listener)
make can-stop        # GET  /v1/admin/can-stop from dbbridge-blue (admin listener)
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
| `server.admin_addr` | unset | Moves `/metrics` and `/v1/admin/*` to their own listener |
| `server.max_request_bytes` | `1048576` | Cap on a JSON request body |
| `server.request_timeout` | `60s` | Bounds ordinary routes; streams are exempt |
| `server.ws_allowed_origins` | `[]` | Browser origins allowed to open a WebSocket; empty = same-origin |
| `server.trusted_proxy_count` | `0` | Proxy hops in front of the service, for `X-Forwarded-For` |
| `server.tls.cert_file` / `key_file` | unset | Serve REST, gRPC and admin over TLS |
| `server.tls.allow_h2c` | `false` | Acknowledge cleartext HTTP/2 for gRPC when TLS is off |
| `auth.tokens[]` | — | Static bearer tokens with `read` / `write` / `admin` scopes |
| `defaults.max_concurrent_queries` | `0` | Cap on concurrent executions; 0 = unlimited |
| `defaults.allow_writes` | `false` | Allow DML and DDL |
| `storage.fs.root` | path | Local directory for result files |
| `storage.s3.*` | — | S3/MinIO credentials and bucket |
| `storage.clickhouse.*` | — | DSN and table for the ClickHouse result backend |
| `databases[]` | — | List of target DB connections |

Any `${VAR}` in the file is substituted from the environment when the config is
loaded, and an unset variable is a startup error. That is how credentials stay
out of the config file and how each Kubernetes replica gets its own
`instance.id`. A bare `$VAR` is left alone, so DSNs and passwords may contain a
dollar sign.

Settings that are fixed at startup - `instance.*`, `server.*`, `storage.*` and
`defaults.max_concurrent_queries` - are reported in the reload response under
`ignored` rather than silently kept at their old values.

Example configs:
- `configs/dbbridge.yaml` — single-node local dev (memory + fs)
- `deploy/configs/dbbridge-blue.yaml` — multi-node (redis + s3/minio)
- `deploy/configs/dbbridge-green.yaml` — same, different instance ID

Config can be reloaded at runtime without restart:

```bash
# Send SIGHUP
kill -HUP <pid>

# Or via HTTP (admin scope, admin listener when admin_addr is set)
curl -X POST http://localhost:8080/v1/admin/reload \
  -H "Authorization: Bearer $DBBRIDGE_TOKEN_ADMIN"
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
    ├── secret.example.yaml     # Copy to secret.yaml and fill in
    ├── deployment.yaml
    └── service.yaml
```

## Kubernetes

```bash
make k8s-apply    # kubectl apply -f deploy/k8s/
make k8s-delete   # kubectl delete -f deploy/k8s/
```

Before applying, review `deploy/k8s/configmap.yaml` for Redis and S3 addresses,
then copy `deploy/k8s/secret.example.yaml` to `deploy/k8s/secret.yaml` and fill
in every field: the Redis password, the S3 credentials and the API tokens, all
referenced from the ConfigMap as `${VAR}`. The tokens have to differ from each
other; a duplicate value is refused at startup.

`trusted_proxy_count` in the ConfigMap is 0, which is right for the ClusterIP
Service shipped here. Raise it to the number of hops only once something in
front of the pod actually appends `X-Forwarded-For`, otherwise a caller can
choose its own client address and with it its own rate-limit bucket.

`/metrics` needs the `admin` scope wherever it is mounted, so a scraper needs a
token of its own:

```yaml
scrape_configs:
  - job_name: dbbridge
    authorization:
      credentials_file: /etc/prometheus/dbbridge-token
    static_configs:
      - targets: ["dbbridge:8081"]
```

### TLS

`server.tls.cert_file` / `key_file` turn TLS on for all three listeners at once.
The manifests here do not use it: probes are plain `httpGet`, the Prometheus
annotation has no `scheme`, and no volume carries a certificate, so switching it
on in the ConfigMap alone makes the liveness probe fail and the pod restart in a
loop while the process itself is healthy. Either terminate TLS at an ingress and
leave `server.tls` unset, or set `scheme: HTTPS` on both probes, add
`prometheus.io/scheme: "https"` and mount the certificate.

The pair is validated at startup, so a wrong path fails the process with a clear
message instead of killing one listener after the rest are up. It is re-read
when either file's modification time changes, so a certificate renewed by
cert-manager is picked up without a restart; if the new pair does not load, the
previous one keeps serving.

The container runs as UID 10001 with a read-only root filesystem and all
capabilities dropped, so `/data/results` and `/tmp` are mounted as `emptyDir`.
Swap the results volume for a PVC if results have to outlive the pod; with
`default_storage: s3` they do not.
