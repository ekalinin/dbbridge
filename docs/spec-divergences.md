# Spec vs. implementation — divergence checklist

A living checklist of where the code differs from [`spec.md`](spec.md). Tick an
item once the divergence is resolved (spec and code agree — either the code was
brought to the spec, or the spec text was updated to match the intended code).

Two kinds of divergence:

- **Behavior / feature gaps** — the code does less (or something else) than the
  spec promises. Close in code, or consciously defer to v2.
- **Interface sketch drift** — `spec.md` gives interface *sketches*; the code
  refined names/signatures during implementation. Usually reconciled by updating
  the spec text, not the code.

## Open divergences

### Behavior / feature gaps

- [ ] **§4 / §5.3 — `parquet` not implemented.** Spec lists `parquet` as a
  result format, but `internal/storage/encoder.go` falls back to JSONL
  (`case "jsonl", "parquet"`). Implement a real parquet encoder or drop it from
  the advertised formats.
- [ ] **§12 / §13 — no `testcontainers-go`.** Spec requires integration tests
  for drivers/storage via testcontainers; not in `go.mod`, tests use in-memory
  fakes only (`internal/testutil`).
- [ ] **§9 — no `STOPPABLE` state.** Spec: `SERVING → DRAINING → STOPPABLE`;
  `internal/lifecycle/manager.go` has only `SERVING` / `DRAINING` (stoppability
  is derived from `can-stop`).
- [ ] **§12 — config library.** Spec: `spf13/viper` or `knadh/koanf`; code uses
  plain `gopkg.in/yaml.v3` (`internal/config`). Spec says "or", so this is a soft
  requirement.
- [ ] **§14 — no CI.** Spec: `go vet` / `golangci-lint` in CI. `.golangci.yml`
  exists, but there is no CI workflow (`.github/workflows` absent).

### Interface sketch drift (reconcile by updating spec text)

- [ ] **§5.1 — service method names.** Spec: `GetStatus`, `GetStats`,
  `OpenResult`, `Watch`. Code: `GetQueryStatus`, `GetQueryStats`,
  `DownloadResult(+offset,limit)`, `WatchQuery`. Code matches the gRPC names in
  §7, so §5.1 is internally inconsistent with §7 — fix the spec.
- [ ] **§5.3 — `Writer` / `Register` signatures.** Spec: `Writer(ctx, ResultRef)`
  and `Register(name, factory Factory)`. Code: `Writer(ctx, queryID, format)` and
  `Register(name, store ResultStore)` (registers an instance, not a factory).
- [ ] **§5.4 — `UpdateState` vs `UpdateQuery`.** Code updates the whole record
  (`UpdateQuery`), and `AcquireIdempotency` takes an extra `ttl` param (code also
  adds `ListExpiredQueries` / `ListStaleQueries` / `DeleteQuery` / `Close`).
- [ ] **§5.2 — driver interfaces.** Spec: `Open(ctx, DSNConfig)`,
  `Stats() PoolStats`, `Columns() []ColumnMeta`. Code: `Open(ctx, dsn, maxConns)`,
  `Stat() PoolStat`, `Columns() ([]string, error)`.

### Structural / minor

- [ ] **§11 — no `internal/core/idempotency/` package.** Idempotency logic lives
  in `manager` + `state`.
- [ ] **§11 — state layout.** Spec: `internal/state/{redis,memory}/` (subdirs);
  code: flat files `internal/state/redis.go`, `memory.go`.
- [ ] **§4 — `QueryError.Code` "enum".** Code uses a free-form `string`.

## Already reconciled

- [x] **§9 — drain rejects new queries with `503` / `Unavailable`** (typed
  `domain.DrainingError`).
- [x] **§14 — domain errors matched via `errors.AsType`** (Go 1.26).
- [x] **§10 — Go `runtime/metrics` exported** via the full `GoCollector` ruleset.
- [x] **§5.3 — `ResultStore.Stat`** added and implemented for fs/s3/clickhouse.
- [x] **§5.4 — `MetaStore.ListByInstance` / `ListDatabasesSeen`** added.
- [x] **§5.1 — `ReloadConfig` returns `ReloadReport`.**
- [x] **§9 — `/readyz` readiness gating during drain** (returns `503` while
  draining; documented in the spec).
