# dbbridge

An asynchronous SQL proxy that accepts a query, executes it in the background against the target database, streams the result to shared storage, and serves downloads on demand. Consumers get a `query_id` immediately and poll or subscribe for completion — disconnecting the consumer never cancels the query.

## Key properties

- **Async-first** — execution context is decoupled from the HTTP/gRPC connection (invariant I1)
- **Multi-node** — any instance can serve reads; Redis coordinates ownership and cross-instance cancellation
- **Idempotent** — duplicate submissions with the same `Idempotency-Key` within the result TTL return the same `query_id`
- **Hot-reload** — config reloads at runtime via `SIGHUP` or `POST /v1/admin/reload` without dropping in-flight queries
- **Graceful drain** — SIGTERM switches to draining mode; `GET /v1/admin/can-stop` reports `SERVING` / `DRAINING` / `STOPPABLE` and zero in-flight to the orchestrator
- **Authenticated** — static bearer tokens with `read` / `write` / `admin` scopes; a query is only readable by the subject that submitted it
- **Read-only by default** — DML and DDL are rejected before they reach the database

## Supported backends

| Category | Options |
|---|---|
| Databases | PostgreSQL, MySQL, ClickHouse, Oracle |
| Result storage | Local filesystem, S3 / MinIO, ClickHouse |
| MetaStore | Redis (multi-node), in-memory (single-node) |
| Result formats | JSONL (default), CSV, Parquet |

## Installation

Install the binary with Go:

```bash
go install github.com/ekalinin/dbbridge/cmd/dbbridge@latest
```

This installs `dbbridge` into `$(go env GOPATH)/bin`. Run it against a config file:

```bash
dbbridge -config configs/dbbridge.yaml
```

To build from source instead, see [Development](#development).

## Quick start

```bash
# Start the full dev stack (two dbbridge instances + Redis + MinIO + Prometheus)
make up

# Or run a single binary without Docker (in-memory metastore, local FS storage)
make run
```

See [`deploy/README.md`](deploy/README.md) for detailed setup, port map, and configuration reference.

## API

Three transports, all backed by the same `QueryService`:

### REST (`:8080`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/queries` | Submit a query → `202 Accepted` + `query_id` |
| `GET` | `/v1/queries/{id}` | Poll status and options |
| `POST` | `/v1/queries/{id}:stop` | Cancel a query |
| `GET` | `/v1/queries/{id}/stats` | Execution stats |
| `GET` | `/v1/queries/{id}/result` | Download result (supports `Range`) |
| `GET` | `/v1/databases` | List configured databases |
| `GET` | `/v1/ws` | WebSocket — subscribe to query state events |
| `POST` | `/v1/admin/reload` | Hot-reload config |
| `GET` | `/v1/admin/can-stop` | Drain signal for orchestrator |
| `GET` | `/healthz`, `/readyz` | Liveness and readiness, unauthenticated |
| `GET` | `/metrics` | Prometheus metrics; `admin` scope; moves to `server.admin_addr` when set |

`/v1` routes and `/metrics` require a bearer token when `auth.tokens` is
configured: `read` for status, stats, download, list and watch; `write` for
submit and stop; `admin` for the two `/v1/admin` routes and for `/metrics`,
whose labels enumerate every configured `db_id`. `write` implies `read` and
`admin` implies both. `/v1/admin/*` and `/metrics` move to a separate listener
when `server.admin_addr` is set, which is network isolation rather than
authorization: the scope is still required there.

A browser cannot set the `Authorization` header on a WebSocket handshake, so
`/v1/ws` also accepts the credential as a subprotocol pair:
`new WebSocket(url, ["dbbridge.bearer", token])`.

Revoking a token needs a restart: `auth` is listed under `report.ignored` by a
reload rather than applied.

Submit a query:

```bash
curl -X POST http://localhost:8080/v1/queries \
  -H "Authorization: Bearer $DBBRIDGE_TOKEN" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-key-001" \
  -d '{"database_id": "pg_main", "sql": "SELECT count(*) FROM orders"}'
# → {"id": "...", "state": "PENDING", ...}
```

Poll until done, then download:

```bash
curl -H "Authorization: Bearer $DBBRIDGE_TOKEN" http://localhost:8080/v1/queries/{id}
curl -H "Authorization: Bearer $DBBRIDGE_TOKEN" http://localhost:8080/v1/queries/{id}/result
```

Watch via WebSocket:

```bash
websocat "ws://localhost:8080/v1/ws?query_id={id}"
```

### gRPC / Connect (`:9090`)

Same operations over gRPC-Connect (HTTP/1.1 + HTTP/2). Without `server.tls` it runs as cleartext HTTP/2, which has to be acknowledged with `server.tls.allow_h2c`. Proto definition: [`api/proto/dbbridge/v1/dbbridge.proto`](api/proto/dbbridge/v1/dbbridge.proto). OpenAPI spec: [`api/openapi/dbbridge.yaml`](api/openapi/dbbridge.yaml).

## Query lifecycle

```
PENDING → RUNNING → SUCCEEDED
                  → FAILED
                  → CANCELED
   * → EXPIRED  (after result_ttl)
```

`sync` mode blocks until a terminal state; `async` (default) returns immediately.

## Development

```bash
make build          # compile → bin/dbbridge
make test-unit      # go test ./internal/... -short
make test-integration  # requires live DBs and Redis (make up first)
make test-containers   # real Redis/PostgreSQL/MySQL/MinIO via testcontainers
make vulncheck      # govulncheck
make lint           # golangci-lint
make check          # vet + lint
make proto          # regenerate internal/gen/ from proto
```

## Project layout

```
cmd/dbbridge/       entry point — driver/storage blank imports
api/
  proto/            protobuf definitions
  openapi/          OpenAPI 3 spec
internal/
  authn/            bearer tokens, scopes, request identity
  sqlguard/         read-only statement guard
  core/
    domain/         QueryRecord, state machine, QueryOptions, ResultRef
    manager/        async execution, heartbeat/GC workers
    service/        lifecycle gate, transport-agnostic facade
  db/               Pool/Driver interfaces + driver registry
  storage/          ResultStore interface + backend registry
  state/            MetaStore — redis and memory implementations
  transport/
    rest/           chi HTTP server
    grpcconnect/    connectrpc handler
    ws/             WebSocket hub
  config/           hot-reloadable YAML config
  lifecycle/        drain state machine
  telemetry/        Prometheus metrics + OpenTelemetry traces
configs/            local dev config (memory metastore + fs storage)
deploy/             Dockerfile, docker-compose, k8s manifests
test/
  e2e/              REST end-to-end with fakes
  integration/      real backends under testcontainers (build tag: integration)
```

## Stack

- Go 1.26
- [connectrpc](https://connectrpc.com/)
- [chi](https://github.com/go-chi/chi)
- [coder/websocket](https://github.com/coder/websocket)
- [go-redis](https://github.com/redis/go-redis)
- pgx
- go-sql-driver/mysql
- clickhouse-go
- go-ora
- aws-sdk-go-v2
- OpenTelemetry

## License

[MIT](LICENSE) © Eugene Kalinin
