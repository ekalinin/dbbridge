package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ekalinin/dbbridge/internal/authn"
	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/sqlguard"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/telemetry"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	// gcInterval is the default period between garbage-collection passes,
	// used when instance.gc_interval is not set.
	gcInterval = time.Minute
	gcLockName = "gc"

	// reapBudget bounds a single owner-reaper pass.
	reapBudget = 15 * time.Second

	// terminalPersistAttempts is how many times a terminal state is retried
	// before the query is left to the owner reaper.
	terminalPersistAttempts = 5

	// poolDrainTimeout bounds how long a replaced or removed pool waits for the
	// queries still using it before it is closed anyway.
	poolDrainTimeout = 2 * time.Minute

	// queryCloseTimeout bounds how long Close waits for query goroutines that
	// have already been canceled.
	queryCloseTimeout = 10 * time.Second

	// watcherBuffer is the per-subscription event buffer.
	watcherBuffer = 20

	// eventPublishBuffer bounds the queue of events waiting to be published to
	// the other instances. Publishing must never stall query execution, so the
	// queue is drained by its own goroutine and overflow is dropped.
	eventPublishBuffer = 256

	// eventPublishTimeout bounds one publish. eventPublishTerminalWait is how
	// long a terminal event waits for room in the queue instead of being
	// dropped, and eventDrainTimeout how long shutdown spends emptying it.
	eventPublishTimeout      = 5 * time.Second
	eventPublishTerminalWait = 5 * time.Second
	eventDrainTimeout        = 5 * time.Second

	// progressPersistInterval throttles how often streaming progress is written
	// to the MetaStore and announced to watchers, and progressPersistTimeout
	// bounds one such write. The timeout is of the same order as the interval:
	// a progress figure that is already stale is not worth waiting for.
	progressPersistInterval = 2 * time.Second
	progressPersistTimeout  = 2 * time.Second
)

type QueryEvent struct {
	QueryID string
	State   domain.QueryState
	Stats   domain.QueryStats
	Error   *domain.QueryError
}

// managedPool remembers the configuration a pool was opened with so a reload
// can tell "unchanged" from "same ID, different DSN".
type managedPool struct {
	pool db.Pool
	cfg  config.DatabaseConfig
}

// activeQuery is an entry of the local in-flight registry.
type activeQuery struct {
	cancel context.CancelFunc
	dbID   string
}

type QueryManager struct {
	cfgManager *config.Manager
	metaStore  state.MetaStore
	instanceID string
	// defaultStorage is captured at construction, like instanceID. The store
	// registry is built once in main and never grows, so honouring a reloaded
	// instance.default_storage would point every new query at a backend that
	// was never built - a permanent 400 no reload could undo. Reporting the
	// section as ignored is only honest if it really is ignored (spec §8).
	defaultStorage string

	dbPools   map[string]*managedPool
	dbPoolsMu sync.RWMutex
	reloadMu  sync.Mutex // serializes reloads; pools are opened under it, not under dbPoolsMu

	// activeRegMu guards both the registry and the draining flag, so admission
	// and the drain decision are one atomic step (I5).
	activeReg   map[string]*activeQuery
	activeRegMu sync.RWMutex
	draining    bool

	// slots caps concurrent executions; nil means unlimited.
	slots chan struct{}

	watchers   map[string]map[chan QueryEvent]struct{}
	watchersMu sync.Mutex

	// events carries locally produced events to the publisher goroutine, which
	// is stopped through publisherStop rather than through ctx: it has to
	// outlive the query goroutines whose terminal events it publishes.
	events        chan QueryEvent
	publisherStop chan struct{}
	publisherDone chan struct{}

	wg      sync.WaitGroup // background workers
	queryWG sync.WaitGroup // in-flight query goroutines
	poolWG  sync.WaitGroup // deferred pool drains

	ctx    context.Context
	cancel context.CancelFunc
}

func NewQueryManager(cfgManager *config.Manager, metaStore state.MetaStore) (*QueryManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := cfgManager.Get()

	qm := &QueryManager{
		cfgManager:     cfgManager,
		metaStore:      metaStore,
		dbPools:        make(map[string]*managedPool),
		instanceID:     cfg.Instance.ID,
		defaultStorage: cfg.Instance.DefaultStorage,
		activeReg:      make(map[string]*activeQuery),
		watchers:       make(map[string]map[chan QueryEvent]struct{}),
		events:         make(chan QueryEvent, eventPublishBuffer),
		publisherStop:  make(chan struct{}),
		publisherDone:  make(chan struct{}),
		ctx:            ctx,
		cancel:         cancel,
	}
	if n := cfg.Defaults.MaxConcurrentQueries; n > 0 {
		qm.slots = make(chan struct{}, n)
	}

	// Initialize pools from configuration
	if err := qm.syncPools(); err != nil {
		log.Printf("ERROR: some database pools failed to open at startup: %v", err)
	}

	qm.recoverOrphans()

	// Start background workers
	qm.wg.Go(qm.heartbeatWorker)
	qm.wg.Go(qm.gcWorker)
	qm.wg.Go(qm.controlWorker)
	qm.wg.Go(qm.ownerReaper)
	go qm.eventPublisher()

	return qm, nil
}

func (qm *QueryManager) heartbeatTTL() time.Duration {
	return qm.cfgManager.Get().Instance.HeartbeatTTL
}

// gcPeriod is how often this instance offers to run garbage collection.
func (qm *QueryManager) gcPeriod() time.Duration {
	if d := qm.cfgManager.Get().Instance.GCInterval; d > 0 {
		return d
	}
	return gcInterval
}

// gcBudget bounds a single GC pass and gcLockTTL keeps it exclusive across the
// cluster. Both are derived from the period so the lock always expires before
// the next tick and exactly one instance runs a pass; at the one-minute default
// they are the 45s and 50s the constants used to spell out.
func (qm *QueryManager) gcBudget() time.Duration  { return qm.gcPeriod() * 3 / 4 }
func (qm *QueryManager) gcLockTTL() time.Duration { return qm.gcPeriod() * 5 / 6 }

// samePoolConfig reports whether a pool opened for old can keep serving new.
func samePoolConfig(a, b config.DatabaseConfig) bool {
	return a.Engine == b.Engine && a.DSN == b.DSN && a.MaxConns == b.MaxConns
}

// syncPools brings the live pools in line with the current configuration.
// Pools are opened outside dbPoolsMu: db.OpenPool pings the database, and
// holding the write lock across that call stalls every SubmitQuery and every
// heartbeat for as long as an unreachable database takes to time out.
func (qm *QueryManager) syncPools() error {
	qm.reloadMu.Lock()
	defer qm.reloadMu.Unlock()

	cfg := qm.cfgManager.Get()

	qm.dbPoolsMu.RLock()
	current := maps.Clone(qm.dbPools)
	qm.dbPoolsMu.RUnlock()

	next := make(map[string]*managedPool, len(cfg.Databases))
	type retired struct {
		id   string
		pool db.Pool
	}
	var stale []retired
	var errs []error

	for _, dbCfg := range cfg.Databases {
		existing, hadExisting := current[dbCfg.ID]
		if hadExisting {
			delete(current, dbCfg.ID)
			if samePoolConfig(existing.cfg, dbCfg) {
				next[dbCfg.ID] = existing
				continue
			}
		}

		pool, err := db.OpenPool(qm.ctx, dbCfg.Engine, dbCfg.DSN, dbCfg.MaxConns)
		if err != nil {
			errs = append(errs, fmt.Errorf("database %s: %w", dbCfg.ID, err))
			// Keep serving from the previous pool rather than dropping the
			// database entirely because its replacement could not be opened.
			if hadExisting {
				next[dbCfg.ID] = existing
			}
			continue
		}
		next[dbCfg.ID] = &managedPool{pool: pool, cfg: dbCfg}
		if hadExisting {
			stale = append(stale, retired{dbCfg.ID, existing.pool})
		}
	}

	// Whatever is left in current was removed from the configuration.
	for id, mp := range current {
		stale = append(stale, retired{id, mp.pool})
	}

	qm.dbPoolsMu.Lock()
	qm.dbPools = next
	qm.dbPoolsMu.Unlock()

	for _, r := range stale {
		qm.closePoolWhenIdle(r.id, r.pool)
	}

	return errors.Join(errs...)
}

// closePoolWhenIdle drains a pool that a reload replaced or removed: it waits
// for the queries still using it to finish before closing (spec §8), and gives
// up after poolDrainTimeout so one stuck query cannot leak a pool for ever.
func (qm *QueryManager) closePoolWhenIdle(dbID string, pool db.Pool) {
	qm.poolWG.Go(func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.Now().Add(poolDrainTimeout)

	drain:
		for qm.activeForDB(dbID) > 0 && time.Now().Before(deadline) {
			select {
			case <-qm.ctx.Done():
				break drain
			case <-ticker.C:
			}
		}

		log.Printf("Closing database pool %s", dbID)
		if err := pool.Close(); err != nil {
			log.Printf("ERROR: failed to close database pool %s: %v", dbID, err)
		}
	})
}

func (qm *QueryManager) activeForDB(dbID string) int {
	qm.activeRegMu.RLock()
	defer qm.activeRegMu.RUnlock()
	n := 0
	for _, aq := range qm.activeReg {
		if aq.dbID == dbID {
			n++
		}
	}
	return n
}

// Reload reloads the configuration and updates DB connection pools dynamically.
// It returns a ReloadReport summarizing which databases were added/removed/updated,
// which sections were ignored, and which pools failed to open.
func (qm *QueryManager) Reload() (domain.ReloadReport, error) {
	oldCfg := qm.cfgManager.Get()
	if err := qm.cfgManager.Reload(); err != nil {
		return domain.ReloadReport{}, err
	}
	newCfg := qm.cfgManager.Get()

	diff := config.DiffDatabases(oldCfg, newCfg)
	report := domain.ReloadReport{
		Added:   dbIDs(diff.Added),
		Removed: dbIDs(diff.Removed),
		Updated: dbIDs(diff.Updated),
		Ignored: config.NonReloadableChanges(oldCfg, newCfg),
	}

	if err := qm.syncPools(); err != nil {
		report.Failures = strings.Split(err.Error(), "\n")
		return report, err
	}
	return report, nil
}

func dbIDs(dbs []config.DatabaseConfig) []string {
	ids := make([]string, len(dbs))
	for i, d := range dbs {
		ids[i] = d.ID
	}
	return ids
}

func (qm *QueryManager) lookupPool(dbID string) (db.Pool, bool) {
	qm.dbPoolsMu.RLock()
	defer qm.dbPoolsMu.RUnlock()
	mp, ok := qm.dbPools[dbID]
	if !ok {
		return nil, false
	}
	return mp.pool, true
}

// applyDefaults fills in the server-side defaults for options the client left
// unset. It runs before validation so a missing mode is stored as "async"
// rather than as an empty string.
func (qm *QueryManager) applyDefaults(opts domain.QueryOptions) domain.QueryOptions {
	cfg := qm.cfgManager.Get()
	if opts.Mode == "" {
		opts.Mode = "async"
	}
	if opts.ResultTTL == 0 {
		opts.ResultTTL = cfg.Defaults.ResultTTL
	}
	if opts.Timeout == 0 {
		opts.Timeout = cfg.Defaults.QueryTimeout
	}
	if opts.ResultFormat == "" {
		opts.ResultFormat = "jsonl"
	}
	if opts.StorageBackend == "" {
		opts.StorageBackend = qm.defaultStorage
	}
	return opts
}

// idempotencySubmitTTL covers execution plus the retention window. The key is
// re-armed against FinishedAt once the query finishes (see complete).
func idempotencySubmitTTL(opts domain.QueryOptions) time.Duration {
	return opts.ResultTTL + 24*time.Hour
}

// scopedIdempotencyKey namespaces a client-chosen key by the subject that chose
// it. Without the namespace the key is global per database, so a caller sending
// somebody else's key gets their record back - SQL text, stats and result
// locator - and can also squat on the key for its whole TTL. I3 only has to
// hold within a subject.
func scopedIdempotencyKey(subject, key string) string {
	return subject + "\x00" + key
}

func (qm *QueryManager) SubmitQuery(ctx context.Context, dbID string, sql string, opts domain.QueryOptions) (*domain.QueryRecord, error) {
	pool, exists := qm.lookupPool(dbID)
	if !exists {
		return nil, domain.NotFoundError{Resource: "database", ID: dbID}
	}

	// Everything that can reject the request runs before the first side effect:
	// no idempotency key is burned and no storage file is created for a request
	// that was never going to be accepted.
	opts = qm.applyDefaults(opts)
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	store, err := storage.GetStore(opts.StorageBackend)
	if err != nil {
		return nil, domain.ValidationError{Field: "storage_backend", Reason: "unknown storage backend"}
	}
	if !storage.SupportsFormat(store, opts.ResultFormat) {
		return nil, domain.ValidationError{
			Field:  "result_format",
			Reason: fmt.Sprintf("storage backend %q cannot store %s results unchanged", opts.StorageBackend, opts.ResultFormat),
		}
	}
	if !qm.cfgManager.Get().Defaults.AllowWrites {
		if err := sqlguard.ReadOnly(sql); err != nil {
			return nil, domain.ValidationError{Field: "sql", Reason: err.Error()}
		}
	}

	queryID := uuid.New().String()
	// Stamped from the authenticated caller so the read paths can tell whose
	// query this is, and so the idempotency key stays inside one subject.
	// Empty when authentication is disabled.
	subject := authn.SubjectFromContext(ctx)
	idemKey := scopedIdempotencyKey(subject, opts.IdempotencyKey)

	// Idempotency is resolved before Ping: a repeated submission has to return
	// the same query ID even while the database is temporarily unreachable (I3).
	if opts.IdempotencyKey != "" {
		existing, err := qm.resolveIdempotency(ctx, dbID, idemKey, opts, queryID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			telemetry.RecordIdempotencyHit()
			return existing, nil
		}
	}

	// From here on every failure has to undo the idempotency claim, otherwise
	// the key outlives the query it points at and blocks every retry.
	release := func() {
		if opts.IdempotencyKey == "" {
			return
		}
		if err := qm.metaStore.ReleaseIdempotency(context.WithoutCancel(ctx), dbID, idemKey, queryID); err != nil {
			log.Printf("ERROR: failed to release idempotency key for query %s: %v", queryID, err)
		}
	}

	if err := pool.Ping(ctx); err != nil {
		release()
		// The driver error carries the DSN (host, user, parameters); it belongs
		// in the log, never in the response.
		log.Printf("ERROR: database %s is unreachable: %v", dbID, err)
		return nil, domain.UnavailableError{Resource: "database " + dbID}
	}

	if !qm.acquireSlot() {
		release()
		return nil, domain.ResourceExhaustedError{Reason: "too many concurrent queries on this instance"}
	}

	// Setup execution context (I1: decoupled from the caller's context).
	var execCtx context.Context
	var cancelExec context.CancelFunc
	if opts.Timeout > 0 {
		execCtx, cancelExec = context.WithTimeout(context.Background(), opts.Timeout)
	} else {
		execCtx, cancelExec = context.WithCancel(context.Background())
	}

	// Registration is the admission gate: it rejects the query if the instance
	// went into drain, in the same critical section that Drain() uses. Checking
	// draining separately from registering let a query slip in after the drain
	// loop had already observed zero in-flight queries (I5).
	if err := qm.admit(queryID, dbID, cancelExec); err != nil {
		cancelExec()
		qm.releaseSlot()
		release()
		return nil, err
	}

	record := &domain.QueryRecord{
		ID:              queryID,
		DatabaseID:      dbID,
		SQL:             sql,
		Options:         opts,
		State:           domain.StatePending,
		OwnerInstanceID: qm.instanceID,
		CreatedAt:       time.Now(),
		IdempotencyKey:  opts.IdempotencyKey,
		Subject:         subject,
	}

	if err := qm.metaStore.PutQuery(ctx, record); err != nil {
		qm.deregister(queryID)
		cancelExec()
		qm.releaseSlot()
		release()
		return nil, fmt.Errorf("failed to persist query state: %w", err)
	}

	// Claim the lease straight away so another instance's reaper cannot see a
	// brand-new query as ownerless before the first heartbeat tick.
	if err := qm.metaStore.Heartbeat(ctx, qm.instanceID, []string{queryID}, qm.heartbeatTTL()); err != nil {
		log.Printf("ERROR: failed to claim lease for query %s: %v", queryID, err)
	}

	qm.notifyWatchers(QueryEvent{
		QueryID: queryID,
		State:   domain.StatePending,
	})

	// Snapshot the record for the async response BEFORE run() starts mutating it
	// in its own goroutine, otherwise the caller (e.g. a transport JSON-encoding
	// the response) races with run() over the shared *record.
	asyncSnapshot := *record

	// For sync mode, subscribe to the watcher BEFORE launching run() so a fast
	// query cannot emit its terminal event before we are listening (avoids a hang).
	var syncCh <-chan QueryEvent
	if opts.Mode == "sync" {
		ch, werr := qm.Watch(ctx, queryID)
		if werr != nil {
			log.Printf("ERROR: failed to watch query %s in sync mode: %v", queryID, werr)
		}
		syncCh = ch
	}

	// I1 detaches the execution context from the request, which also detaches
	// the span. The submitting span is carried over as a link so a trace can
	// still be followed from transport to execution.
	submitter := trace.SpanContextFromContext(ctx)

	// Async execution. The goroutine is tracked so shutdown can wait for it.
	qm.queryWG.Go(func() { qm.run(execCtx, record, pool, cancelExec, submitter) })

	if opts.Mode == "sync" {
		// Wait for execution to finish
		if syncCh != nil {
			for ev := range syncCh {
				if ev.State.IsTerminal() {
					break
				}
			}
		}
		// Return updated record
		return qm.GetQuery(ctx, queryID)
	}

	return &asyncSnapshot, nil
}

// resolveIdempotency claims the subject-scoped key for queryID. It returns a
// non-nil record when the key is already held by a live query, which the caller
// returns as is.
func (qm *QueryManager) resolveIdempotency(ctx context.Context, dbID, key string, opts domain.QueryOptions, queryID string) (*domain.QueryRecord, error) {
	ttl := idempotencySubmitTTL(opts)

	existingID, acquired, err := qm.metaStore.AcquireIdempotency(ctx, dbID, key, queryID, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to process idempotency: %w", err)
	}
	if acquired {
		return nil, nil
	}

	rec, err := qm.metaStore.GetQuery(ctx, existingID)
	if err == nil {
		// The key is scoped to the subject, so a match is the caller's own
		// query. The check costs nothing and keeps StartQuery - the one read
		// path that does not go through service.authorized - from ever being
		// the way a record escapes its subject.
		if aerr := authn.AuthorizeSubject(ctx, rec.Subject); aerr != nil {
			return nil, domain.NotFoundError{Resource: "query", ID: existingID}
		}
		log.Printf("Idempotency match! Returning existing query ID %s", existingID)
		return rec, nil
	}
	if !errors.Is(err, state.ErrNotFound) {
		return nil, err
	}

	// The key outlived the record it pointed at (GC removed it, or an older
	// build failed to roll the key back). Reclaim it instead of refusing the
	// query for the rest of the TTL.
	log.Printf("Idempotency key for database %s points at missing query %s; reclaiming", dbID, existingID)
	if err := qm.metaStore.ReleaseIdempotency(ctx, dbID, key, existingID); err != nil {
		return nil, fmt.Errorf("failed to reclaim idempotency key: %w", err)
	}
	existingID, acquired, err = qm.metaStore.AcquireIdempotency(ctx, dbID, key, queryID, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to process idempotency: %w", err)
	}
	if acquired {
		return nil, nil
	}
	return qm.metaStore.GetQuery(ctx, existingID)
}

// admit registers a query in the local in-flight registry unless the instance
// is draining.
func (qm *QueryManager) admit(queryID, dbID string, cancel context.CancelFunc) error {
	qm.activeRegMu.Lock()
	defer qm.activeRegMu.Unlock()
	if qm.draining {
		return domain.DrainingError{}
	}
	qm.activeReg[queryID] = &activeQuery{cancel: cancel, dbID: dbID}
	return nil
}

func (qm *QueryManager) deregister(queryID string) {
	qm.activeRegMu.Lock()
	defer qm.activeRegMu.Unlock()
	delete(qm.activeReg, queryID)
}

func (qm *QueryManager) acquireSlot() bool {
	if qm.slots == nil {
		return true
	}
	select {
	case qm.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (qm *QueryManager) releaseSlot() {
	if qm.slots == nil {
		return
	}
	select {
	case <-qm.slots:
	default:
	}
}

// Drain closes admission. Once it returns, no further query can enter the local
// registry, so a subsequent CountInFlight of zero really means quiesced (I5).
func (qm *QueryManager) Drain() {
	qm.activeRegMu.Lock()
	defer qm.activeRegMu.Unlock()
	qm.draining = true
}

func (qm *QueryManager) run(ctx context.Context, record *domain.QueryRecord, pool db.Pool, cancelExec context.CancelFunc, submitter trace.SpanContext) {
	// LIFO: the registry entry is dropped only after the terminal state has been
	// persisted, which keeps the lease alive across the retries and keeps the
	// owner reaper away from a query that is merely slow to record its result.
	defer cancelExec()
	defer qm.releaseSlot()
	defer qm.deregister(record.ID)

	spanOpts := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("query.id", record.ID),
			attribute.String("query.database_id", record.DatabaseID),
		),
	}
	if submitter.IsValid() {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: submitter}))
	}
	ctx, span := otel.Tracer("dbbridge").Start(ctx, "query.run", spanOpts...)
	defer span.End()

	startTime := time.Now()

	// Update to RUNNING
	if !record.State.CanTransitionTo(domain.StateRunning) {
		log.Printf("ERROR: refusing transition %s -> RUNNING for query %s", record.State, record.ID)
		return
	}
	record.State = domain.StateRunning
	record.StartedAt = startTime
	if err := qm.persist(record, terminalPersistAttempts); err != nil {
		// Not fatal on its own: the terminal write below is the one that must
		// land, and it is retried separately.
		log.Printf("ERROR: failed to persist RUNNING state for query %s: %v", record.ID, err)
	}
	qm.notifyWatchers(QueryEvent{QueryID: record.ID, State: domain.StateRunning})
	telemetry.RecordQueryStarted()

	// Execute Query on Database
	dbStart := time.Now()
	rowsStream, err := pool.Exec(ctx, record.SQL)
	dbDuration := time.Since(dbStart)

	if err != nil {
		nextState, qErr := terminalFor(ctx, err, domain.ErrCodeDBExecFailed)
		qm.finishRun(record, nextState, qErr, dbDuration, 0)
		return
	}
	defer func() {
		if cerr := rowsStream.Close(); cerr != nil {
			log.Printf("ERROR: failed to close row stream for query %s: %v", record.ID, cerr)
		}
	}()

	// Initialize Result Store Writer
	store, err := storage.GetStore(record.Options.StorageBackend)
	if err != nil {
		qm.finishRun(record, domain.StateFailed,
			domain.NewQueryError(domain.ErrCodeStorageInitFailed, err.Error(), false), dbDuration, 0)
		return
	}

	// The execution context is handed to the writer so a stopped or timed-out
	// query also aborts its upload. A private context.Background() here left S3
	// uploads running for queries that were already terminal.
	writer, ref, err := store.Writer(ctx, record.ID, record.Options.ResultFormat)
	if err != nil {
		qm.finishRun(record, domain.StateFailed,
			domain.NewQueryError(domain.ErrCodeStorageWriterFailed, err.Error(), false), dbDuration, 0)
		return
	}

	// Stream and Persist Rows. The checksum is computed from the same bytes on
	// their way to storage, so it costs one pass and never needs the result to
	// be read back.
	storeStart := time.Now()
	hasher := sha256.New()
	report, stopReporting := qm.progressReporter(ctx, record)
	rowsCount, bytesWritten, encErr := storage.EncodeStream(ctx, rowsStream, record.Options.ResultFormat,
		io.MultiWriter(writer, hasher), report)
	// No progress write may still be in flight when the terminal write lands,
	// or the two race for the record.
	stopReporting()
	// Close is where S3 and ClickHouse wait for the upload/commit and surface
	// its error, so its result decides whether the query really succeeded (I4).
	closeErr := writer.Close()
	storeDuration := time.Since(storeStart)

	if encErr != nil {
		deletePartial(store, ref, record.ID)
		nextState, qErr := terminalFor(ctx, encErr, domain.ErrCodeStreamEncodeFailed)
		qm.finishRun(record, nextState, qErr, dbDuration, storeDuration)
		return
	}

	if closeErr != nil {
		deletePartial(store, ref, record.ID)
		nextState, qErr := terminalFor(ctx, closeErr, domain.ErrCodeStorageFinalizeFailed)
		qm.finishRun(record, nextState, qErr, dbDuration, storeDuration)
		return
	}

	// Update stats & ref
	ref.SizeBytes = bytesWritten
	ref.RowCount = rowsCount
	ref.Checksum = "sha256:" + hex.EncodeToString(hasher.Sum(nil))

	record.Result = &ref
	record.Stats.RowsRead = rowsCount
	record.Stats.BytesWritten = bytesWritten
	record.Stats.TotalDuration = time.Since(startTime)

	qm.finishRun(record, domain.StateSucceeded, nil, dbDuration, storeDuration)
	telemetry.RecordResultBytes(ref.Backend, bytesWritten)
}

// progressReporter returns the callback that publishes streaming progress and
// the function that shuts down the writer behind it. Stats used to be written
// once, at completion, so a query that ran for an hour reported nothing until
// it was already over. Writes are throttled: the point is a live figure, not a
// record of every row.
func (qm *QueryManager) progressReporter(ctx context.Context, record *domain.QueryRecord) (func(rows, bytes int64), func()) {
	// A single-slot mailbox. The persist used to run inline, so every report
	// parked the row loop for as long as the MetaStore took to answer - up to
	// the full persist timeout on a degraded Redis, six times per minute, on a
	// context that could not be canceled.
	updates := make(chan domain.QueryRecord, 1)
	done := make(chan struct{})
	var lost atomic.Bool

	go func() {
		defer close(done)
		for snapshot := range updates {
			// Conditional write. An unconditional PutQuery here walked straight
			// through the fence failOwnerLost puts up: a record another
			// instance's reaper had already failed went back to RUNNING, with
			// its FinishedAt and error cleared, and rejoined this instance's
			// in-flight set with no lease behind it - so the reaper failed it
			// again on the next tick, announcing a false terminal state to the
			// cluster every couple of seconds.
			writeCtx, cancel := context.WithTimeout(ctx, progressPersistTimeout)
			written, err := qm.metaStore.UpdateQueryIfState(writeCtx, &snapshot, domain.StateRunning)
			cancel()
			switch {
			case err != nil:
				log.Printf("ERROR: failed to persist progress for query %s: %v", snapshot.ID, err)
			case !written:
				// The record is no longer ours: somebody finalized it, or GC
				// removed it. Stop reporting rather than fight for it.
				log.Printf("Query %s is no longer RUNNING in the MetaStore, dropping its progress reports", snapshot.ID)
				lost.Store(true)
				return
			}
		}
	}()

	// Zero, not time.Now(): the first batch of rows is reported straight away so
	// a client sees the query moving instead of waiting out the first interval.
	var last time.Time
	report := func(rows, bytes int64) {
		if lost.Load() || time.Since(last) < progressPersistInterval {
			return
		}
		last = time.Now()

		record.Stats.RowsRead = rows
		record.Stats.BytesWritten = bytes
		// Without this a client watching a running query sees the row count
		// climb while the elapsed time stays at zero.
		record.Stats.TotalDuration = time.Since(record.CreatedAt)

		select {
		case updates <- *record:
		default:
			// The writer is still busy. Dropping is right: the next report
			// carries a newer figure, and the query must not wait for it.
		}
		qm.notifyWatchers(QueryEvent{
			QueryID: record.ID,
			State:   record.State,
			Stats:   record.Stats,
		})
	}

	stop := func() {
		close(updates)
		<-done
	}
	return report, stop
}

// terminalFor maps an execution failure to the terminal state the state machine
// asks for: an explicit stop is CANCELED, an expired deadline is a FAILED with
// its own code, anything else keeps the caller's code. The context is consulted
// as well because drivers wrap or replace the cancellation error.
func terminalFor(ctx context.Context, err error, fallback domain.QueryErrorCode) (domain.QueryState, *domain.QueryError) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(ctx.Err(), context.Canceled):
		return domain.StateCanceled, nil
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return domain.StateFailed, domain.NewQueryError(domain.ErrCodeQueryTimeout, "query exceeded its timeout", true)
	default:
		return domain.StateFailed, domain.NewQueryError(fallback, err.Error(), false)
	}
}

// finishRun applies a terminal transition from inside run() and records the
// telemetry that pairs with RecordQueryStarted.
func (qm *QueryManager) finishRun(rec *domain.QueryRecord, next domain.QueryState, qErr *domain.QueryError, dbDur, storeDur time.Duration) {
	rec.Error = qErr
	rec.Stats.DBExecDuration = dbDur
	rec.Stats.StorageWriteDuration = storeDur
	if rec.Stats.TotalDuration == 0 {
		rec.Stats.TotalDuration = time.Since(rec.CreatedAt)
	}

	if !qm.complete(rec, next) {
		return
	}

	telemetry.RecordQueryFinished()
	telemetry.RecordQueryCompleted(qm.getEngine(rec.DatabaseID), string(next), rec.Stats.TotalDuration)
}

// complete performs a terminal transition on a record: it enforces the state
// machine, persists with retries, re-arms the idempotency key against the
// retention window and notifies watchers. It reports whether the transition was
// applied.
func (qm *QueryManager) complete(rec *domain.QueryRecord, next domain.QueryState) bool {
	if !rec.State.CanTransitionTo(next) {
		log.Printf("ERROR: refusing transition %s -> %s for query %s", rec.State, next, rec.ID)
		return false
	}

	rec.State = next
	rec.FinishedAt = time.Now()

	if err := qm.persist(rec, terminalPersistAttempts); err != nil {
		// The lease stops being refreshed once run() deregisters the query, so
		// the owner reaper on this or another instance eventually fails it. That
		// is the honest outcome: better a FAILED record than one stuck in
		// RUNNING for ever.
		log.Printf("ERROR: gave up persisting %s for query %s, leaving it to the owner reaper: %v", next, rec.ID, err)
	}

	// Retention is measured from FinishedAt, so the key acquired at submission
	// time is re-armed here to expire together with the result (I3).
	if rec.IdempotencyKey != "" && rec.Options.ResultTTL > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := scopedIdempotencyKey(rec.Subject, rec.IdempotencyKey)
		if err := qm.metaStore.RefreshIdempotency(ctx, rec.DatabaseID, key, rec.ID, rec.Options.ResultTTL); err != nil {
			log.Printf("ERROR: failed to align idempotency TTL for query %s: %v", rec.ID, err)
		}
		cancel()
	}

	qm.notifyWatchers(QueryEvent{
		QueryID: rec.ID,
		State:   next,
		Stats:   rec.Stats,
		Error:   rec.Error,
	})
	return true
}

// persist writes the record to the MetaStore, retrying with backoff. A dropped
// terminal write leaves readers on other instances looking at RUNNING for a
// query that already produced its result (I2, I4), so it is worth retrying
// instead of being logged once and forgotten.
func (qm *QueryManager) persist(rec *domain.QueryRecord, attempts int) error {
	backoff := 200 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := qm.metaStore.PutQuery(ctx, rec)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("ERROR: failed to persist %s state for query %s (attempt %d/%d): %v",
			rec.State, rec.ID, attempt, attempts, err)

		if attempt == attempts {
			break
		}
		select {
		case <-qm.ctx.Done():
			return lastErr
		case <-time.After(backoff):
		}
		backoff *= 2
	}

	return lastErr
}

func (qm *QueryManager) GetQuery(ctx context.Context, queryID string) (*domain.QueryRecord, error) {
	return qm.metaStore.GetQuery(ctx, queryID)
}

func (qm *QueryManager) StopQuery(ctx context.Context, queryID string) error {
	record, err := qm.metaStore.GetQuery(ctx, queryID)
	if err != nil {
		return err
	}

	if record.State.IsTerminal() {
		return nil // already finished
	}

	if record.OwnerInstanceID == qm.instanceID {
		qm.activeRegMu.RLock()
		aq, exists := qm.activeReg[queryID]
		qm.activeRegMu.RUnlock()
		if exists {
			aq.cancel()
			return nil
		}
	}

	// Publish control message to let the owner know to stop it
	return qm.metaStore.PublishControl(ctx, state.ControlMsg{
		Type:     state.ControlStopQuery,
		QueryID:  queryID,
		SenderID: qm.instanceID,
	})
}

// CountInFlight reports the owned queries still in flight. It takes the larger
// of the local registry and the MetaStore's view: after a restart under the
// same instance ID the registry is empty while records owned by this ID are
// still marked active, and reporting zero there would let an orchestrator stop
// a node that has not quiesced (I5, spec §10).
func (qm *QueryManager) CountInFlight(ctx context.Context) int {
	qm.activeRegMu.RLock()
	local := len(qm.activeReg)
	qm.activeRegMu.RUnlock()

	remote, err := qm.metaStore.CountInFlight(ctx, qm.instanceID)
	if err != nil {
		log.Printf("ERROR: failed to count in-flight queries in the MetaStore: %v", err)
		return local
	}
	return max(local, remote)
}

func (qm *QueryManager) Watch(ctx context.Context, queryID string) (<-chan QueryEvent, error) {
	// A watch on an unknown ID used to block for ever. Resolving the record
	// first also gives the subscriber the current state, so a subscription that
	// arrives after the query finished terminates instead of waiting.
	rec, err := qm.metaStore.GetQuery(ctx, queryID)
	if err != nil {
		return nil, err
	}

	ch := make(chan QueryEvent, watcherBuffer)
	current := QueryEvent{
		QueryID: queryID,
		State:   rec.State,
		Stats:   rec.Stats,
		Error:   rec.Error,
	}

	qm.watchersMu.Lock()
	subs, ok := qm.watchers[queryID]
	if !ok {
		subs = make(map[chan QueryEvent]struct{})
		qm.watchers[queryID] = subs
	}
	subs[ch] = struct{}{}
	ch <- current
	terminal := rec.State.IsTerminal()
	if terminal {
		qm.dropWatcherLocked(queryID, ch)
	}
	qm.watchersMu.Unlock()

	if terminal {
		return ch, nil
	}

	go func() {
		<-ctx.Done()
		qm.unwatch(queryID, ch)
	}()

	return ch, nil
}

// dropWatcherLocked removes a subscription and closes its channel. Callers must
// hold watchersMu, which notifyWatchers also holds while it sends, so a channel
// can never be closed with a send in flight.
func (qm *QueryManager) dropWatcherLocked(queryID string, ch chan QueryEvent) {
	subs, ok := qm.watchers[queryID]
	if !ok {
		return
	}
	if _, ok := subs[ch]; !ok {
		return
	}

	delete(subs, ch)
	if len(subs) == 0 {
		delete(qm.watchers, queryID)
	}
	close(ch)
}

func (qm *QueryManager) unwatch(queryID string, ch chan QueryEvent) {
	qm.watchersMu.Lock()
	defer qm.watchersMu.Unlock()
	qm.dropWatcherLocked(queryID, ch)
}

// notifyWatchers delivers an event locally and hands it to the publisher so
// subscriptions held by other instances see it too (I2).
func (qm *QueryManager) notifyWatchers(ev QueryEvent) {
	qm.deliverEvent(ev)

	if ev.State.IsTerminal() {
		// A terminal event is the one that closes a remote subscription, and
		// no further event for that query will ever follow it. Dropped along
		// with the progress events it queues behind, it left every WebSocket
		// connection and Connect stream held by another instance open for
		// good. The query is finished by now, so waiting here costs nothing it
		// was still doing.
		select {
		case qm.events <- ev:
		case <-time.After(eventPublishTerminalWait):
			log.Printf("ERROR: could not queue the terminal announcement for query %s, remote watchers will have to time out", ev.QueryID)
		}
		return
	}

	select {
	case qm.events <- ev:
	default:
		// The publisher is behind. Dropping a progress announcement is
		// preferable to stalling the query that produced it; readers can always
		// poll the record, and the next report carries a newer figure.
		log.Printf("WARNING: dropping the cross-instance announcement for query %s, publish queue is full", ev.QueryID)
	}
}

// deliverEvent fans an event out to the subscriptions held by this process,
// without republishing it.
func (qm *QueryManager) deliverEvent(ev QueryEvent) {
	qm.watchersMu.Lock()
	defer qm.watchersMu.Unlock()

	for ch := range qm.watchers[ev.QueryID] {
		select {
		case ch <- ev:
		default:
			// Non-blocking write to avoid blocking during notifications
		}
		// A terminal state is the last event a query can produce; closing the
		// subscription here is what lets a WebSocket or gRPC stream end instead
		// of waiting for a client timeout.
		if ev.State.IsTerminal() {
			qm.dropWatcherLocked(ev.QueryID, ch)
		}
	}
}

// Background workers

func (qm *QueryManager) heartbeatWorker() {
	ticker := time.NewTicker(qm.heartbeatTTL() / 2)
	defer ticker.Stop()

	for {
		select {
		case <-qm.ctx.Done():
			return
		case <-ticker.C:
			qm.activeRegMu.RLock()
			activeIDs := slices.Collect(maps.Keys(qm.activeReg))
			qm.activeRegMu.RUnlock()

			ttl := qm.heartbeatTTL()
			if err := qm.metaStore.Heartbeat(qm.ctx, qm.instanceID, activeIDs, ttl); err != nil {
				log.Printf("ERROR: MetaStore Heartbeat failed: %v", err)
			}

			qm.dbPoolsMu.RLock()
			for dbID, mp := range qm.dbPools {
				s := mp.pool.Stat()
				telemetry.RecordPoolStat(dbID, s.Open, s.Idle, s.InUse)
			}
			qm.dbPoolsMu.RUnlock()
		}
	}
}

// eventPublisher forwards locally produced events to the other instances.
func (qm *QueryManager) eventPublisher() {
	defer close(qm.publisherDone)
	for {
		select {
		case ev := <-qm.events:
			qm.publishEvent(ev)
		case <-qm.publisherStop:
			// Drain what the last query goroutines queued. Their terminal
			// events are what closes the subscriptions the other instances
			// hold; without this pass those connections hang until the client
			// gives up.
			deadline := time.Now().Add(eventDrainTimeout)
			for time.Now().Before(deadline) {
				select {
				case ev := <-qm.events:
					qm.publishEvent(ev)
				default:
					return
				}
			}
			return
		}
	}
}

func (qm *QueryManager) publishEvent(ev QueryEvent) {
	ctx, cancel := context.WithTimeout(qm.ctx, eventPublishTimeout)
	defer cancel()
	err := qm.metaStore.PublishControl(ctx, state.ControlMsg{
		Type:     state.ControlQueryEvent,
		QueryID:  ev.QueryID,
		SenderID: qm.instanceID,
		Event: &state.QueryEventPayload{
			State: string(ev.State),
			Stats: ev.Stats,
			Error: ev.Error,
		},
	})
	if err != nil && qm.ctx.Err() == nil {
		log.Printf("ERROR: failed to publish the event for query %s: %v", ev.QueryID, err)
	}
}

func (qm *QueryManager) controlWorker() {
	ch, err := qm.metaStore.SubscribeControl(qm.ctx)
	if err != nil {
		log.Printf("ERROR: Failed to subscribe to control messages: %v", err)
		return
	}

	for {
		select {
		case <-qm.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Our own announcements come back on the same channel; delivering
			// them again would double every event.
			if msg.SenderID == qm.instanceID {
				continue
			}

			switch msg.Type {
			case state.ControlStopQuery:
				qm.activeRegMu.RLock()
				aq, ok := qm.activeReg[msg.QueryID]
				qm.activeRegMu.RUnlock()
				if ok {
					log.Printf("Received remote cancellation request for query %s", msg.QueryID)
					aq.cancel()
				}
			case state.ControlQueryEvent:
				if msg.Event == nil {
					continue
				}
				// A query this instance is executing reports on itself. Handing
				// it a remote verdict - a reaper that failed it after one missed
				// heartbeat, say - closed the local subscriptions and made a
				// sync submission return FAILED for a query that was still
				// running and went on to succeed.
				qm.activeRegMu.RLock()
				_, mine := qm.activeReg[msg.QueryID]
				qm.activeRegMu.RUnlock()
				if mine {
					continue
				}
				qm.deliverEvent(QueryEvent{
					QueryID: msg.QueryID,
					State:   domain.QueryState(msg.Event.State),
					Stats:   msg.Event.Stats,
					Error:   msg.Event.Error,
				})
			}
		}
	}
}

func (qm *QueryManager) gcWorker() {
	// The period is read once: the ticker outlives a reload, like the reaper's.
	ticker := time.NewTicker(qm.gcPeriod())
	defer ticker.Stop()

	for {
		select {
		case <-qm.ctx.Done():
			return
		case <-ticker.C:
			qm.collectGarbage()
		}
	}
}

// collectGarbage transitions expired queries to EXPIRED and removes their
// storage results and metadata. Extracted from gcWorker for testability.
func (qm *QueryManager) collectGarbage() {
	ctx, cancel := context.WithTimeout(context.Background(), qm.gcBudget())
	defer cancel()

	// ListExpiredQueries returns the same set on every instance. Without a lock
	// they race to delete the same result: one wins, the rest log a deletion
	// failure, and those false alarms hide the real ones.
	locked, err := qm.metaStore.TryLock(ctx, gcLockName, qm.gcLockTTL())
	if err != nil {
		log.Printf("ERROR: GC worker failed to acquire the cluster lock: %v", err)
		return
	}
	if !locked {
		return
	}

	expiredIDs, err := qm.metaStore.ListExpiredQueries(ctx)
	if err != nil {
		log.Printf("ERROR: GC worker failed to list expired queries: %v", err)
		return
	}

	for _, id := range expiredIDs {
		if ctx.Err() != nil {
			log.Printf("GC: budget exhausted, %d queries left for the next pass", len(expiredIDs))
			return
		}
		qm.collectQuery(ctx, id)
	}
}

// collectQuery expires and removes a single query. Metadata is kept whenever
// the stored result could not be deleted: dropping it would strip the only
// reference to the orphaned object and make a retry impossible.
func (qm *QueryManager) collectQuery(ctx context.Context, id string) {
	rec, err := qm.metaStore.GetQuery(ctx, id)
	if err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			log.Printf("ERROR: GC failed to read query %s: %v", id, err)
		}
		return
	}

	if rec.State != domain.StateExpired {
		if !rec.State.CanTransitionTo(domain.StateExpired) {
			log.Printf("ERROR: GC refusing transition %s -> EXPIRED for query %s", rec.State, id)
			return
		}
		rec.State = domain.StateExpired
		if err := qm.metaStore.UpdateQuery(ctx, rec); err != nil {
			log.Printf("ERROR: GC failed to mark query %s as EXPIRED: %v", id, err)
			return
		}
		qm.notifyWatchers(QueryEvent{QueryID: id, State: domain.StateExpired})
	}

	if rec.Result != nil {
		store, err := storage.GetStore(rec.Result.Backend)
		if err != nil {
			log.Printf("ERROR: GC keeping metadata for query %s: %v", id, err)
			return
		}
		log.Printf("GC: Deleting results storage for query %s", id)
		if err := store.Delete(ctx, *rec.Result); err != nil {
			log.Printf("ERROR: GC keeping metadata for query %s, result deletion failed: %v", id, err)
			return
		}
	}

	log.Printf("GC: Deleting metadata for expired query %s", id)
	if err := qm.metaStore.DeleteQuery(ctx, id); err != nil {
		log.Printf("ERROR: GC failed to delete metadata for query %s: %v", id, err)
	}
}

// ownerReaper detects queries whose owner instance has died (its lease expired)
// while still in a non-terminal state, and fails them with OWNER_LOST (spec §3).
func (qm *QueryManager) ownerReaper() {
	ticker := time.NewTicker(qm.heartbeatTTL())
	defer ticker.Stop()

	for {
		select {
		case <-qm.ctx.Done():
			return
		case <-ticker.C:
			qm.reapStaleOwners()
		}
	}
}

// reapStaleOwners fails non-terminal queries whose owner instance is gone.
// Extracted from ownerReaper for testability.
func (qm *QueryManager) reapStaleOwners() {
	ctx, cancel := context.WithTimeout(context.Background(), reapBudget)
	defer cancel()

	staleIDs, err := qm.metaStore.ListStaleQueries(ctx)
	if err != nil {
		log.Printf("ERROR: owner reaper failed to list stale queries: %v", err)
		return
	}

	// A query created moments ago may not have been heartbeated yet. Without a
	// grace period a Redis hiccup or a GC pause is enough to fail a query that
	// is running perfectly well, after which its owner overwrites the verdict.
	grace := 2 * qm.heartbeatTTL()

	for _, id := range staleIDs {
		qm.activeRegMu.RLock()
		_, local := qm.activeReg[id]
		qm.activeRegMu.RUnlock()
		if local {
			continue
		}

		rec, err := qm.metaStore.GetQuery(ctx, id)
		if err != nil {
			continue
		}
		// Queries this instance owns are its own to finish; a restart is handled
		// once, at startup, by recoverOrphans.
		if rec.OwnerInstanceID == qm.instanceID {
			continue
		}
		if time.Since(rec.CreatedAt) < grace {
			continue
		}

		qm.failOwnerLost(ctx, rec)
	}
}

// recoverOrphans fails queries this instance still owns in the MetaStore but is
// not running. They belong to a previous process that died mid-flight: nothing
// else will ever reap them, because every other instance skips records it does
// not own, and this node would otherwise report can_be_stopped=true while its
// old records sit in RUNNING for ever.
func (qm *QueryManager) recoverOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), reapBudget)
	defer cancel()

	ids, err := qm.metaStore.ListByInstance(ctx, qm.instanceID)
	if err != nil {
		log.Printf("ERROR: failed to list queries owned by %s at startup: %v", qm.instanceID, err)
		return
	}

	for _, id := range ids {
		rec, err := qm.metaStore.GetQuery(ctx, id)
		if err != nil {
			continue
		}
		if rec.State.IsTerminal() {
			continue
		}
		log.Printf("Recovering orphaned query %s left behind by a previous process", id)
		qm.failOwnerLost(ctx, rec)
	}
}

// failOwnerLost marks a query FAILED/OWNER_LOST, but only while it is still
// non-terminal. The conditional write is the fence: without it a reaper can
// overwrite a SUCCEEDED record (and its ResultRef) that landed in between.
func (qm *QueryManager) failOwnerLost(ctx context.Context, rec *domain.QueryRecord) {
	failed := *rec
	failed.State = domain.StateFailed
	failed.FinishedAt = time.Now()
	failed.Error = domain.NewQueryError(domain.ErrCodeOwnerLost, "owner instance lost before query completion", true)

	written, err := qm.metaStore.UpdateQueryIfState(ctx, &failed, domain.StatePending, domain.StateRunning)
	if err != nil {
		log.Printf("ERROR: owner reaper failed to fail query %s: %v", rec.ID, err)
		return
	}
	if !written {
		// Somebody else finished it first; that verdict wins.
		return
	}

	log.Printf("Owner reaper: marked query %s as FAILED (owner_lost)", rec.ID)
	qm.notifyWatchers(QueryEvent{
		QueryID: rec.ID,
		State:   domain.StateFailed,
		Error:   failed.Error,
	})
	telemetry.RecordQueryCompleted(qm.getEngine(rec.DatabaseID), string(domain.StateFailed), 0)
}

func (qm *QueryManager) getEngine(dbID string) string {
	cfg := qm.cfgManager.Get()
	if idx := slices.IndexFunc(cfg.Databases, func(dbCfg config.DatabaseConfig) bool {
		return dbCfg.ID == dbID
	}); idx != -1 {
		return cfg.Databases[idx].Engine
	}
	return "unknown"
}

func (qm *QueryManager) GetConfig() *config.Config {
	return qm.cfgManager.Get()
}

func (qm *QueryManager) GetPool(dbID string) (db.Pool, bool) {
	return qm.lookupPool(dbID)
}

// DatabasesSeen returns the databases that have had at least one query
// submitted, including ones no longer present in the configuration.
func (qm *QueryManager) DatabasesSeen(ctx context.Context) ([]string, error) {
	return qm.metaStore.ListDatabasesSeen(ctx)
}

func (qm *QueryManager) Close() error {
	qm.Drain()

	// Anything still executing has already outlived the shutdown grace period
	// the caller allowed, so it is canceled rather than waited on for ever.
	qm.cancelActive()
	waitFor(&qm.queryWG, queryCloseTimeout)

	// The publisher stops after the goroutines it publishes for, and by way of
	// its own queue rather than the shared context. Stopping it first meant the
	// terminal events of the last queries went into a queue nobody drained, so
	// subscriptions held by other instances were left hanging on a shutdown.
	close(qm.publisherStop)
	select {
	case <-qm.publisherDone:
	case <-time.After(eventDrainTimeout + time.Second):
		log.Print("WARNING: the event publisher did not drain in time, some announcements were not sent")
	}

	qm.cancel()
	qm.wg.Wait()
	qm.poolWG.Wait()

	qm.dbPoolsMu.Lock()
	defer qm.dbPoolsMu.Unlock()
	for id, mp := range qm.dbPools {
		if err := mp.pool.Close(); err != nil {
			log.Printf("ERROR: failed to close database pool %s: %v", id, err)
		}
	}
	return nil
}

func (qm *QueryManager) cancelActive() {
	qm.activeRegMu.RLock()
	cancels := make([]context.CancelFunc, 0, len(qm.activeReg))
	for _, aq := range qm.activeReg {
		cancels = append(cancels, aq.cancel)
	}
	qm.activeRegMu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// waitFor waits for wg, giving up after timeout so shutdown cannot hang on a
// query that refuses to notice its canceled context.
func waitFor(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("WARNING: gave up waiting for query goroutines after %v", timeout)
	}
}

// deletePartial removes a partially written result after a failed or canceled
// stream. The query already carries its own terminal error, so a cleanup
// failure is only reported.
func deletePartial(store storage.ResultStore, ref domain.ResultRef, queryID string) {
	if err := store.Delete(context.Background(), ref); err != nil {
		log.Printf("ERROR: failed to delete partial result for query %s: %v", queryID, err)
	}
}
