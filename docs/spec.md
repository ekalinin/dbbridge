# dbbridge — Specification

## 1. Purpose and Key Invariants

`dbbridge` is an SQL proxy that accepts a query, executes it on the target database in the background, and survives a restart of the **consumer**. The consumer receives a `query_id` and subsequently polls the status or downloads the result using it.

Invariants (mandatory to adhere to across all layers):
- **I1. The query lifecycle is NOT bound to the incoming connection.** The execution context is derived from `context.Background()` + timeout options, NOT from the `ctx` of the HTTP/gRPC request. Disconnecting the consumer does not cancel the query.
- **I2. Any instance can handle read requests** (status/stats/download/list) — metadata is stored in Redis, and results are stored in shared storage. Write/execution belongs to a single owner instance (`owner_instance_id`).
- **I3. Idempotency:** repeating `StartQuery` with the same key within the retention window returns the same `query_id` and does not trigger a second execution.
- **I4. Results are materialized exactly once** (stream+persist during execution); download always reads from storage, and the database is not queried again.
- **I5. Graceful drain:** an instance returns `can_be_stopped=true` only when it has 0 in-flight owned queries.

Out of scope for v1 (design interfaces, implement in v2): restarting the proxy itself without stopping its queries (split gateway/executor + FD-handoff variant).

## 2. Architecture (v1: single binary, layer by layer)

```mermaid
flowchart TB
  consumer[Consumer App] --> lb[LB / Orchestrator]
  lb --> inst[dbbridge instance]
  subgraph inst [dbbridge instance single binary]
    rest[REST adapter chi] --> svc
    grpc[gRPC/Connect adapter] --> svc
    ws[WebSocket hub] --> svc
    svc[QueryService core] --> qm[QueryManager]
    qm --> drv[DB Driver Registry]
    qm --> store[ResultStore Registry]
    qm --> meta[MetaStore]
    qm --> tel[Telemetry]
    cfg[Config + reload] --> svc
    life[Lifecycle/Drain] --> svc
  end
  drv --> dbs[(Oracle/PG/MySQL/CH)]
  store --> backends[(FS / S3 / ClickHouse)]
  meta --> redis[(Redis / in-memory)]
  tel --> prom[Prometheus/OTLP]
```

Layers are strictly separated by interfaces, so that in v2 `QueryManager` can be moved to a separate durable daemon without rewriting the transports.

## 3. Query State Machine

```mermaid
stateDiagram-v2
  [*] --> PENDING
  PENDING --> RUNNING
  RUNNING --> SUCCEEDED
  RUNNING --> FAILED
  RUNNING --> CANCELED
  PENDING --> CANCELED
  SUCCEEDED --> EXPIRED
  FAILED --> EXPIRED
  CANCELED --> EXPIRED
```
- `RUNNING` includes streaming→persisting sub-phases (visible in stats).
- `EXPIRED` — after `result_ttl` expires, a background GC cleans up storage + metadata.
- If the owner dies (lease expires) and the query is in `RUNNING` — transition to `FAILED` (reason=`owner_lost`) in v1.

## 4. Domain Data Structures (`internal/core/domain`)

- `QueryOptions`: `Timeout time.Duration` (0 = no limit, default), `Mode` (`async` default | `sync`), `ResultTTL time.Duration` (default 24h), `IdempotencyKey string`, `ResultFormat` (`jsonl` default | `csv` | `parquet`), `StorageBackend string` (default override).
- `QueryRecord`: `ID`, `DatabaseID`, `SQL`, `Options`, `State`, `OwnerInstanceID`, `CreatedAt/StartedAt/FinishedAt`, `Error *QueryError`, `Stats QueryStats`, `Result *ResultRef`, `IdempotencyKey`, `LeaseDeadline`.
- `QueryStats`: `RowsRead`, `BytesWritten`, `DBExecDuration`, `StorageWriteDuration`, `TotalDuration`, `Retries`.
- `ResultRef`: `Backend string`, `Locator string` (path/key/table), `SizeBytes`, `RowCount`, `Format`, `Checksum`.
- `DatabaseInfo`: `ID`, `Engine` (oracle|postgres|mysql|clickhouse), `DisplayName`, `Healthy bool`.
- `QueryError`: `Code` (enum), `Message`, `Retryable bool`.

## 5. Module Contracts (Interfaces)

### 5.1 QueryService (`internal/core/service`) — transport-agnostic facade
A single interface called by ALL three transports:
- `StartQuery(ctx, dbID, sql, opts) (QueryRecord, error)` — I1: decouples context internally.
- `GetStatus(ctx, id) (QueryRecord, error)` — returns record with all options.
- `StopQuery(ctx, id) error` — cancels locally or forwards to the owner (5.5).
- `GetStats(ctx, id) (QueryStats, error)`.
- `OpenResult(ctx, id) (io.ReadCloser, ResultRef, error)` — stream from storage.
- `ListDatabases(ctx) ([]DatabaseInfo, error)`.
- `ReloadConfig(ctx) (ReloadReport, error)`.
- `Watch(ctx, id) (<-chan QueryEvent, error)` — for WS/streams.

### 5.2 DB Driver plugin (`internal/db`)
Registry based on compile-time registration (NOT Go `plugin`):
- `type Driver interface { Open(ctx, DSNConfig) (Pool, error) }`
- `type Pool interface { Exec(ctx, sql) (RowStream, error); Ping(ctx) error; Close() error; Stats() PoolStats }`
- `type RowStream interface { Columns() []ColumnMeta; Next() bool; Scan(dest...) / Row() []any; Err() error; Close() error }`
- `func Register(engine string, d Driver)` + `init()` in each driver.
- Drivers: `postgres` (jackc/pgx), `mysql` (go-sql-driver/mysql), `clickhouse` (clickhouse-go/v2), `oracle` (godror or sijms/go-ora). Each returns a stream so that large results are not kept in memory.

### 5.3 ResultStore plugin (`internal/storage`)
- `type ResultStore interface { Writer(ctx, ResultRef) (io.WriteCloser, error); Reader(ctx, ResultRef) (io.ReadCloser, error); Stat(ctx, ResultRef) (ResultRef, error); Delete(ctx, ResultRef) error }`
- `func Register(name string, factory Factory)` + `init()`.
- Backends: `fs` (local FS/NFS), `s3` (aws-sdk-go-v2, multipart upload for large files), `clickhouse` (writing results to a system table). Backend selection is from config (default) with override in `QueryOptions`.

### 5.4 MetaStore (`internal/state`)
- `type MetaStore interface { PutQuery / GetQuery / UpdateState / ListByInstance / ListDatabasesSeen; AcquireIdempotency(ctx, dbID, key, queryID) (existing string, acquired bool, err error); Heartbeat(ctx, instanceID, queries []string, ttl) error; PublishControl(ctx, ControlMsg) error; SubscribeControl(ctx) (<-chan ControlMsg, error); CountInFlight(ctx, instanceID) (int, error) }`
- Implementations: `redis` (go-redis/v9; idempotency via `SET NX`, lease via TTL keys + heartbeat, control via Pub/Sub) and `memory` (in-process, for single-node installations — without cross-instance capabilities).

### 5.5 Cross-instance control
`StopQuery`/reload-config for a query owned by another instance: `QueryService` checks `OwnerInstanceID`; if not local — `MetaStore.PublishControl({type: stop, query_id})`; the owner cancels the local `context.CancelFunc` from the active query registry via `SubscribeControl`.

## 6. QueryManager (`internal/core/manager`) — Execution Core

`StartQuery` Algorithm:
1. Validate `dbID` against the current config snapshot.
2. If idempotency key is set → `AcquireIdempotency`; if occupied — return the existing `QueryRecord`.
3. Create `QueryRecord{State: PENDING, OwnerInstanceID: self}`, `PutQuery`.
4. Register `cancel` in the local `activeRegistry[id]`.
5. **I1:** `execCtx := context.WithTimeout(context.Background(), opts.Timeout)` (or without timeout).
6. Launch goroutine `run(execCtx, record)`: `Pool.Exec` → `RowStream` → format encoder → `ResultStore.Writer` (stream+persist, I4), increment stats, update state in MetaStore.
7. `async`: immediately return record with `query_id`. `sync`: wait for completion/timeout and return the final record (but execution is still decoupled — continues if connection drops).
8. Heartbeat ticker updates owner lease and in-flight list.

GC Worker: periodically searches for `EXPIRED`/outdated queries by `ResultTTL`, cleans up storage + metadata.

## 7. Transports (`internal/transport`)

The API contract is defined spec-first: a single proto file `api/proto/dbbridge/v1/dbbridge.proto` + OpenAPI for REST.

- **gRPC + Connect** (`connectrpc.com/connect`, buf generation) — methods: `StartQuery`, `GetQueryStatus`, `StopQuery`, `GetQueryStats`, `DownloadResult` (server-stream), `ListDatabases`, `ReloadConfig`, `CanIBeStopped`, `WatchQuery` (server-stream). Provides gRPC + gRPC-Web + Connect/JSON from a single service.
- **REST** (`go-chi/chi`, thin adapter to the same `QueryService`):
  - `POST /v1/queries` (body: db_id, sql, options; header `Idempotency-Key`) → 202 + query_id.
  - `GET /v1/queries/{id}` → status + all options.
  - `POST /v1/queries/{id}:stop`.
  - `GET /v1/queries/{id}/stats`.
  - `GET /v1/queries/{id}/result` → result stream (chunked, `Range` optional).
  - `GET /v1/databases`.
  - `POST /v1/admin/reload`.
  - `GET /v1/admin/can-stop` → `{can_be_stopped, in_flight}` for orchestrator.
  - `GET /healthz`, `GET /readyz`, `GET /metrics`.
- **WebSocket** (`coder/websocket`, `/v1/ws`): subscription to `QueryEvent` (state changes, progress) via `QueryService.Watch`.

All adapters are without business logic, only mapping DTO↔domain.

## 8. Configuration (`internal/config`)

YAML, hot-reload via endpoint `/v1/admin/reload` + `SIGHUP` (+ optional fsnotify). Atomic snapshot swap via `atomic.Pointer[Config]`; on reload: add new DB pools, drain removed ones, update modified ones. Draft:

```yaml
instance:
  id: dbbridge-blue
  metastore: redis   # redis | memory
  redis: { addr: "redis:6379" }
  default_storage: s3
server:
  rest_addr: ":8080"
  grpc_addr: ":9090"
defaults:
  result_ttl: 24h
  query_timeout: 0
storage:
  s3: { bucket: dbbridge, region: eu-central-1 }
  fs: { root: /var/lib/dbbridge/results }
  clickhouse: { dsn: "...", table: dbbridge_results }
databases:
  - id: pg_main
    engine: postgres
    dsn: "postgres://..."
    pool: { max_conns: 20 }
  - id: ora_billing
    engine: oracle
    dsn: "oracle://..."
```

## 9. Lifecycle and Blue/Green (`internal/lifecycle`)

- Instance states: `SERVING` → `DRAINING` → `STOPPABLE`. On SIGTERM → `DRAINING`: new `StartQuery` requests are rejected (503), `CanIBeStopped` starts depending on `CountInFlight`.
- `CanIBeStopped` (REST + gRPC): `true` ⟺ in-flight owned queries = 0 (I5). Orchestrator polls before stopping the blue instance.
- Two instances (`dbbridge-blue`/`dbbridge-green`) behind LB, shared Redis + storage; reads are served by any instance, drain is done one by one.

## 10. Telemetry (`internal/telemetry`)

OpenTelemetry SDK (metrics+traces), Prometheus exporter on `/metrics`, OTLP export. Go metrics via `runtime/metrics` (`otel` runtime instrumentation). Domain metrics: `dbbridge_queries_total{engine,state}`, `dbbridge_query_duration_seconds`, `dbbridge_inflight_queries`, `dbbridge_result_bytes_total{backend}`, `dbbridge_db_pool_*`, `dbbridge_idempotency_hits_total`. End-to-end tracing: transport → service → manager → driver/store.

## 11. Repository Structure

```
dbbridge/
  cmd/dbbridge/main.go
  api/proto/dbbridge/v1/dbbridge.proto
  api/openapi/dbbridge.yaml
  internal/config/
  internal/core/{domain,service,manager}/
  internal/core/idempotency/
  internal/db/{registry.go, drivers/{postgres,mysql,oracle,clickhouse}}/
  internal/storage/{registry.go, backends/{fs,s3,clickhouse}}/
  internal/state/{redis,memory}/
  internal/transport/{rest,grpcconnect,ws}/
  internal/telemetry/
  internal/lifecycle/
  configs/dbbridge.yaml
  deploy/{docker-compose.yaml, Dockerfile, k8s/}
  go.mod (go 1.26)
```

## 12. Stack

Go 1.26; `connectrpc.com/connect` + buf; `go-chi/chi`; `coder/websocket`; `redis/go-redis/v9`; `jackc/pgx/v5`, `go-sql-driver/mysql`, `ClickHouse/clickhouse-go/v2`, `godror` (or `sijms/go-ora`); `aws/aws-sdk-go-v2`; `go.opentelemetry.io/otel` + prometheus exporter; `spf13/viper` or `knadh/koanf` for config; `testcontainers-go` for integration tests.

## 13. Implementation Order (Phases)

Phases are ordered by dependencies; see todos. Each phase ends with compilable code and tests. Driver/storage tests use testcontainers; core uses unit tests with fake registries.

## 14. Code Conventions

No trailing spaces; each file ends with an empty line; `go vet`/`golangci-lint` in CI; domain errors via `errors.AsType` (Go 1.26).
