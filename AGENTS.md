# AGENTS.md

See [docs/spec.md](docs/spec.md) for the full specification and implementation plan.

## Code Style

Before writing or reviewing Go code, run `/use-modern-go` to apply modern Go syntax guidelines for this project's Go version.

## Commands

```bash
# Build & run
make build                    # compile to bin/dbbridge
make run                      # build + run with configs/dbbridge-blue.yaml
./bin/dbbridge -config <path> # run with a specific config

# Tests
make test-unit                # go test ./internal/... -short (unit tests only)
make test-integration         # go test ./internal/... -timeout 120s (requires live DBs/Redis)
make test-containers          # real Redis/PostgreSQL/MySQL/MinIO via testcontainers (needs Docker)
make test-e2e                 # go test ./test/e2e/... -timeout 300s
go test ./internal/config/... # run a single package's tests

# Quality
make lint                     # golangci-lint run ./...
make vet                      # go vet ./...
make fmt                      # gofmt -l -w .
make check                    # vet + lint
make ci                       # the fast CI job (fmt-check, vet, tests, race, lint, buf lint, govulncheck)
make test-containers          # the other CI job: real backends via testcontainers, needs Docker

# Protobuf
make proto                    # buf generate (regenerates internal/gen/)
make proto-lint               # buf lint

# Docker / dev stack
make up                       # docker compose up -d (postgres, mysql, redis, minio, prometheus)
make down
make logs

# Admin (against running instance)
make reload-config            # POST /v1/admin/reload
make can-stop                 # GET  /v1/admin/can-stop
```

## Architecture

dbbridge is a **stateless SQL proxy** that accepts queries over REST (including WebSocket) or gRPC-Connect, executes them asynchronously against target databases, streams and persists the results to a storage backend, then serves downloads on demand. Multiple instances coordinate via Redis.

### Request flow

1. Client POSTs to `POST /v1/queries` (REST) or calls `StartQuery` (gRPC-Connect).
2. `QueryService` (`internal/core/service`) checks lifecycle state and delegates to `QueryManager`.
3. `QueryManager` (`internal/core/manager`) creates a `QueryRecord`, persists it to the `MetaStore`, then runs the query in a goroutine.
4. The goroutine: executes SQL via a `db.Pool` → streams rows through `storage.EncodeStream` → writes encoded results to a `ResultStore`.
5. On completion, `QueryRecord` is updated in `MetaStore` with a `ResultRef` pointing to the stored file.
6. Clients poll `GET /v1/queries/{id}` or subscribe via WebSocket / gRPC server-stream (`WatchQuery`) for state transitions.
7. Results are downloaded via `GET /v1/queries/{id}/result` (supports HTTP Range) or the gRPC `DownloadResult` streaming RPC.

### Layer map

| Layer | Package | Role |
|---|---|---|
| Domain | `internal/core/domain` | `QueryRecord`, `QueryState` state machine, `QueryOptions`, `ResultRef` |
| Manager | `internal/core/manager` | Async execution, in-flight registry, heartbeat/GC/control workers |
| Service | `internal/core/service` | Lifecycle gate, result download helpers; thin wrapper over Manager |
| Transport | `internal/transport/rest`, `grpcconnect`, `ws` | HTTP+JSON (chi), gRPC-Connect (connectrpc), WebSocket |
| State | `internal/state` | `MetaStore` interface — `memory` (single-node) or `redis` (multi-node) |
| Storage | `internal/storage` | `ResultStore` interface — `fs`, `s3`, `clickhouse` backends |
| DB | `internal/db` | `Pool`/`Driver` interfaces; drivers registered via blank imports in `main.go` |
| Config | `internal/config` | Hot-reloadable via `atomic.Pointer[Config]`; triggered by SIGHUP or `POST /v1/admin/reload` |
| Telemetry | `internal/telemetry` | Prometheus metrics + OpenTelemetry traces/metrics |

### Key design points

- **Driver registration**: DB drivers (`postgres`, `mysql`, `clickhouse`, `oracle`) are registered at startup via blank imports in `cmd/dbbridge/main.go`. Storage backends (`fs`, `s3`, `clickhouse`) are built explicitly there from the sections the configuration asks for, so a backend that cannot be built is a startup failure rather than a 400 on the first query that needs it.
- **MetaStore duality**: `memory` metastore is for single-node dev; `redis` enables multi-node deployment with cross-instance query cancellation via Pub/Sub and lease heartbeats.
- **Idempotency**: Pass `Idempotency-Key` HTTP header (or `options.idempotency_key` in gRPC) to deduplicate submissions within a result TTL window.
- **Query modes**: `mode: "sync"` blocks until terminal state; `mode: "async"` (default) returns `202 Accepted` immediately.
- **Result formats**: `jsonl` (default), `csv`, `parquet` — controlled per-query via `result_format`, validated against a whitelist before any storage writer is opened.
- **Authentication**: static bearer tokens from `auth.tokens` with `read` / `write` / `admin` scopes; `write` implies `read`, `admin` implies both. `internal/authn` holds only the identity, the scopes and the token comparison - the chi middleware lives in `internal/transport/rest` and the Connect interceptor in `internal/transport/grpcconnect`, so the core does not link a transport. A query is only readable by the subject that submitted it.
- **Read-only guard**: `internal/sqlguard` rejects DML and DDL before execution unless `defaults.allow_writes` is set.
- **Graceful shutdown**: SIGTERM/SIGINT triggers draining state (new queries rejected, admission closed in the manager under the same lock that registers a query), then waits up to 30 s for in-flight queries to finish before closing HTTP servers.
- **Config hot-reload**: `config.Manager` uses `atomic.Pointer` for lock-free reads; `QueryManager.Reload()` diffs DB pools and closes removed ones without restarting.

### Proto / generated code

Proto definitions live under `api/proto/`. Generated Go code is at `internal/gen/` — never edit files there manually. Regenerate with `make proto`. The gRPC transport uses [connectrpc](https://connectrpc.com/) (HTTP/1.1 + HTTP/2, no TLS required for local dev).

### Configuration

YAML config file path is passed via `-config` flag. Key fields:

- `instance.metastore`: `"memory"` or `"redis"` — determines cluster mode
- `instance.gc_interval`: how often expired results are swept; default `1m`
- `instance.default_storage`: `"fs"`, `"s3"` or `"clickhouse"` — default result backend, captured at startup and not reloadable
- `databases[].engine`: `postgres` | `mysql` | `clickhouse` | `oracle`
- `storage.fs.root`: local directory for result files
- `storage.s3.*`: S3-compatible (MinIO supported via `endpoint` override)

Example configs: `configs/dbbridge.yaml`, `deploy/configs/dbbridge-blue.yaml`, `deploy/configs/dbbridge-green.yaml`.
