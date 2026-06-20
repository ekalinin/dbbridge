package manager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/ekalinin/dbbridge/internal/config"
	"github.com/ekalinin/dbbridge/internal/core/domain"
	"github.com/ekalinin/dbbridge/internal/db"
	"github.com/ekalinin/dbbridge/internal/state"
	"github.com/ekalinin/dbbridge/internal/storage"
	"github.com/ekalinin/dbbridge/internal/telemetry"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type QueryEvent struct {
	QueryID string
	State   domain.QueryState
	Stats   domain.QueryStats
	Error   *domain.QueryError
}

type QueryManager struct {
	cfgManager  *config.Manager
	metaStore   state.MetaStore
	dbPools     map[string]db.Pool
	dbPoolsMu   sync.RWMutex
	instanceID  string
	activeReg   map[string]context.CancelFunc
	activeRegMu sync.RWMutex
	watchers    map[string][]chan QueryEvent
	watchersMu  sync.RWMutex
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewQueryManager(cfgManager *config.Manager, metaStore state.MetaStore) (*QueryManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	qm := &QueryManager{
		cfgManager: cfgManager,
		metaStore:  metaStore,
		dbPools:    make(map[string]db.Pool),
		instanceID: cfgManager.Get().Instance.ID,
		activeReg:  make(map[string]context.CancelFunc),
		watchers:   make(map[string][]chan QueryEvent),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Initialize pools from configuration
	if err := qm.syncPools(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to sync database pools: %w", err)
	}

	// Start background workers
	qm.wg.Go(qm.heartbeatWorker)
	qm.wg.Go(qm.gcWorker)
	qm.wg.Go(qm.controlWorker)
	qm.wg.Go(qm.ownerReaper)

	return qm, nil
}

func (qm *QueryManager) syncPools() error {
	qm.dbPoolsMu.Lock()
	defer qm.dbPoolsMu.Unlock()

	cfg := qm.cfgManager.Get()
	oldPools := qm.dbPools
	newPools := make(map[string]db.Pool)

	for _, dbCfg := range cfg.Databases {
		// If pool exists and config hasn't changed, reuse it.
		// For simplicity of diffing, we can use the diff utility or do it directly.
		existing, ok := oldPools[dbCfg.ID]
		if ok {
			newPools[dbCfg.ID] = existing
			delete(oldPools, dbCfg.ID) // keep only removed/updated ones in oldPools to close later
		} else {
			pool, err := db.OpenPool(qm.ctx, dbCfg.Engine, dbCfg.DSN, dbCfg.MaxConns)
			if err != nil {
				// Log but do not fail hard, database might be temporarily down
				log.Printf("ERROR: Failed to open pool for database %s: %v", dbCfg.ID, err)
				continue
			}
			newPools[dbCfg.ID] = pool
		}
	}

	// Close old pools that were removed or need recreate
	for id, pool := range oldPools {
		log.Printf("Closing database pool %s", id)
		_ = pool.Close()
	}

	qm.dbPools = newPools
	return nil
}

// Reload reloads the configuration and updates DB connection pools dynamically.
// It returns a ReloadReport summarizing which databases were added/removed/updated.
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
	}

	if err := qm.syncPools(); err != nil {
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

func (qm *QueryManager) SubmitQuery(ctx context.Context, dbID string, sql string, opts domain.QueryOptions) (*domain.QueryRecord, error) {
	qm.dbPoolsMu.RLock()
	pool, exists := qm.dbPools[dbID]
	qm.dbPoolsMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("database %s not configured or pool initialization failed", dbID)
	}

	// Ping database to make sure it is ready
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database %s is unreachable: %w", dbID, err)
	}

	queryID := uuid.New().String()

	// Apply defaults before idempotency check so the TTL is correct.
	cfg := qm.cfgManager.Get()
	if opts.ResultTTL == 0 {
		opts.ResultTTL = cfg.Defaults.ResultTTL
	}
	if opts.ResultFormat == "" {
		opts.ResultFormat = "jsonl"
	}
	if opts.StorageBackend == "" {
		opts.StorageBackend = cfg.Instance.DefaultStorage
	}

	// Handle Idempotency
	if opts.IdempotencyKey != "" {
		existingID, acquired, err := qm.metaStore.AcquireIdempotency(ctx, dbID, opts.IdempotencyKey, queryID, opts.ResultTTL)
		if err != nil {
			return nil, fmt.Errorf("failed to process idempotency: %w", err)
		}
		if !acquired {
			log.Printf("Idempotency match! Returning existing query ID %s", existingID)
			telemetry.RecordIdempotencyHit()
			return qm.GetQuery(ctx, existingID)
		}
	}

	record := &domain.QueryRecord{
		ID:              queryID,
		DatabaseID:      dbID,
		SQL:             sql,
		Options:         opts,
		State:           domain.StatePending,
		OwnerInstanceID: qm.instanceID,
		CreatedAt:       time.Now(),
	}

	if err := qm.metaStore.PutQuery(ctx, record); err != nil {
		return nil, fmt.Errorf("failed to persist query state: %w", err)
	}

	// Setup execution context
	var execCtx context.Context
	var cancelExec context.CancelFunc
	if opts.Timeout > 0 {
		execCtx, cancelExec = context.WithTimeout(context.Background(), opts.Timeout)
	} else {
		execCtx, cancelExec = context.WithCancel(context.Background())
	}

	qm.activeRegMu.Lock()
	qm.activeReg[queryID] = cancelExec
	qm.activeRegMu.Unlock()

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
		syncCh, _ = qm.Watch(ctx, queryID)
	}

	// Async execution
	go qm.run(execCtx, record, pool, cancelExec)

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

func (qm *QueryManager) run(ctx context.Context, record *domain.QueryRecord, pool db.Pool, cancelExec context.CancelFunc) {
	defer cancelExec()
	defer func() {
		qm.activeRegMu.Lock()
		delete(qm.activeReg, record.ID)
		qm.activeRegMu.Unlock()
	}()

	ctx, span := otel.Tracer("dbbridge").Start(ctx, "query.run",
		trace.WithAttributes(
			attribute.String("query.id", record.ID),
			attribute.String("query.database_id", record.DatabaseID),
		))
	defer span.End()

	startTime := time.Now()

	// Update to RUNNING
	record.State = domain.StateRunning
	record.StartedAt = startTime
	record.LeaseDeadline = time.Now().Add(qm.cfgManager.Get().Instance.HeartbeatTTL)
	_ = qm.metaStore.PutQuery(context.Background(), record)
	qm.notifyWatchers(QueryEvent{QueryID: record.ID, State: domain.StateRunning})
	telemetry.RecordQueryStarted()

	// Execute Query on Database
	dbStart := time.Now()
	rowsStream, err := pool.Exec(ctx, record.SQL)
	dbDuration := time.Since(dbStart)

	if err != nil {
		qm.finishWithError(record, &domain.QueryError{
			Code:      "DB_EXEC_FAILED",
			Message:   err.Error(),
			Retryable: false, // customizable based on error
		}, dbDuration, 0)
		return
	}
	defer rowsStream.Close()

	// Initialize Result Store Writer
	store, err := storage.GetStore(record.Options.StorageBackend)
	if err != nil {
		qm.finishWithError(record, &domain.QueryError{
			Code:    "STORAGE_INIT_FAILED",
			Message: err.Error(),
		}, dbDuration, 0)
		return
	}

	storeCtx, storeCancel := context.WithCancel(context.Background())
	defer storeCancel()

	writer, ref, err := store.Writer(storeCtx, record.ID, record.Options.ResultFormat)
	if err != nil {
		qm.finishWithError(record, &domain.QueryError{
			Code:    "STORAGE_WRITER_FAILED",
			Message: err.Error(),
		}, dbDuration, 0)
		return
	}

	// Stream and Persist Rows
	storeStart := time.Now()
	rowsCount, bytesWritten, err := storage.EncodeStream(ctx, rowsStream, record.Options.ResultFormat, writer)
	_ = writer.Close()
	storeDuration := time.Since(storeStart)

	if err != nil {
		// If execution was canceled
		if errors.Is(err, context.Canceled) {
			_ = store.Delete(context.Background(), ref) // clean up partial file
			qm.finishWithCancel(record, dbDuration, storeDuration)
			return
		}

		_ = store.Delete(context.Background(), ref) // clean up partial file
		qm.finishWithError(record, &domain.QueryError{
			Code:    "STREAM_ENCODE_FAILED",
			Message: err.Error(),
		}, dbDuration, storeDuration)
		return
	}

	// Update stats & ref
	ref.SizeBytes = bytesWritten
	ref.RowCount = rowsCount

	record.State = domain.StateSucceeded
	record.FinishedAt = time.Now()
	record.Result = &ref
	record.Stats = domain.QueryStats{
		RowsRead:             rowsCount,
		BytesWritten:         bytesWritten,
		DBExecDuration:       dbDuration,
		StorageWriteDuration: storeDuration,
		TotalDuration:        time.Since(startTime),
	}

	_ = qm.metaStore.PutQuery(context.Background(), record)
	qm.notifyWatchers(QueryEvent{
		QueryID: record.ID,
		State:   domain.StateSucceeded,
		Stats:   record.Stats,
	})

	engine := qm.getEngine(record.DatabaseID)
	telemetry.RecordQueryFinished()
	telemetry.RecordQueryCompleted(engine, string(domain.StateSucceeded), record.Stats.TotalDuration)
	telemetry.RecordResultBytes(ref.Backend, bytesWritten)
}

func (qm *QueryManager) finishWithError(rec *domain.QueryRecord, qErr *domain.QueryError, dbDur, storeDur time.Duration) {
	rec.State = domain.StateFailed
	rec.FinishedAt = time.Now()
	rec.Error = qErr
	rec.Stats.DBExecDuration = dbDur
	rec.Stats.StorageWriteDuration = storeDur
	rec.Stats.TotalDuration = time.Since(rec.CreatedAt)

	_ = qm.metaStore.PutQuery(context.Background(), rec)
	qm.notifyWatchers(QueryEvent{
		QueryID: rec.ID,
		State:   domain.StateFailed,
		Error:   qErr,
		Stats:   rec.Stats,
	})

	engine := qm.getEngine(rec.DatabaseID)
	telemetry.RecordQueryFinished()
	telemetry.RecordQueryCompleted(engine, string(domain.StateFailed), rec.Stats.TotalDuration)
}

func (qm *QueryManager) finishWithCancel(rec *domain.QueryRecord, dbDur, storeDur time.Duration) {
	rec.State = domain.StateCanceled
	rec.FinishedAt = time.Now()
	rec.Stats.DBExecDuration = dbDur
	rec.Stats.StorageWriteDuration = storeDur
	rec.Stats.TotalDuration = time.Since(rec.CreatedAt)

	_ = qm.metaStore.PutQuery(context.Background(), rec)
	qm.notifyWatchers(QueryEvent{
		QueryID: rec.ID,
		State:   domain.StateCanceled,
		Stats:   rec.Stats,
	})

	engine := qm.getEngine(rec.DatabaseID)
	telemetry.RecordQueryFinished()
	telemetry.RecordQueryCompleted(engine, string(domain.StateCanceled), rec.Stats.TotalDuration)
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
		qm.activeRegMu.Lock()
		cancel, exists := qm.activeReg[queryID]
		qm.activeRegMu.Unlock()
		if exists {
			cancel()
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

func (qm *QueryManager) CountInFlight() int {
	qm.activeRegMu.RLock()
	defer qm.activeRegMu.RUnlock()
	return len(qm.activeReg)
}

func (qm *QueryManager) Watch(ctx context.Context, queryID string) (<-chan QueryEvent, error) {
	ch := make(chan QueryEvent, 20)

	qm.watchersMu.Lock()
	qm.watchers[queryID] = append(qm.watchers[queryID], ch)
	qm.watchersMu.Unlock()

	go func() {
		<-ctx.Done()
		qm.watchersMu.Lock()
		defer qm.watchersMu.Unlock()
		list := qm.watchers[queryID]
		if i := slices.Index(list, ch); i != -1 {
			qm.watchers[queryID] = append(list[:i], list[i+1:]...)
			close(ch)
		}
	}()

	return ch, nil
}

func (qm *QueryManager) notifyWatchers(ev QueryEvent) {
	qm.watchersMu.RLock()
	watchersList, ok := qm.watchers[ev.QueryID]
	qm.watchersMu.RUnlock()

	if !ok {
		return
	}

	for _, ch := range watchersList {
		select {
		case ch <- ev:
		default:
			// Non-blocking write to avoid blocking during notifications
		}
	}
}

// Background workers

func (qm *QueryManager) heartbeatWorker() {
	ticker := time.NewTicker(qm.cfgManager.Get().Instance.HeartbeatTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-qm.ctx.Done():
			return
		case <-ticker.C:
			qm.activeRegMu.RLock()
			activeIDs := make([]string, 0, len(qm.activeReg))
			for id := range qm.activeReg {
				activeIDs = append(activeIDs, id)
			}
			qm.activeRegMu.RUnlock()

			ttl := qm.cfgManager.Get().Instance.HeartbeatTTL
			if err := qm.metaStore.Heartbeat(qm.ctx, qm.instanceID, activeIDs, ttl); err != nil {
				log.Printf("ERROR: MetaStore Heartbeat failed: %v", err)
			}

			qm.dbPoolsMu.RLock()
			for dbID, pool := range qm.dbPools {
				s := pool.Stat()
				telemetry.RecordPoolStat(dbID, s.Open, s.Idle, s.InUse)
			}
			qm.dbPoolsMu.RUnlock()
		}
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
			if msg.Type == state.ControlStopQuery {
				qm.activeRegMu.Lock()
				cancel, ok := qm.activeReg[msg.QueryID]
				qm.activeRegMu.Unlock()
				if ok {
					log.Printf("Received remote cancellation request for query %s", msg.QueryID)
					cancel()
				}
			}
		}
	}
}

func (qm *QueryManager) gcWorker() {
	ticker := time.NewTicker(1 * time.Minute)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	expiredIDs, err := qm.metaStore.ListExpiredQueries(ctx)
	cancel()

	if err != nil {
		log.Printf("ERROR: GC worker failed to list expired queries: %v", err)
		return
	}

	for _, id := range expiredIDs {
		gcCtx, gcCancel := context.WithTimeout(context.Background(), 15*time.Second)
		rec, err := qm.metaStore.GetQuery(gcCtx, id)
		if err == nil {
			// Transition to EXPIRED before deleting (spec state machine §3).
			if rec.State != domain.StateExpired {
				rec.State = domain.StateExpired
				_ = qm.metaStore.UpdateQuery(gcCtx, rec)
				qm.notifyWatchers(QueryEvent{QueryID: id, State: domain.StateExpired})
			}
			if rec.Result != nil {
				store, err := storage.GetStore(rec.Result.Backend)
				if err == nil {
					log.Printf("GC: Deleting results storage for query %s", id)
					_ = store.Delete(gcCtx, *rec.Result)
				}
			}
		}
		log.Printf("GC: Deleting metadata for expired query %s", id)
		_ = qm.metaStore.DeleteQuery(gcCtx, id)
		gcCancel()
	}
}

// ownerReaper detects queries whose owner instance has died (its heartbeat/lease
// key expired) while still in a non-terminal state, and fails them with OWNER_LOST
// (spec §3). Queries this instance is actively running are skipped.
func (qm *QueryManager) ownerReaper() {
	ticker := time.NewTicker(qm.cfgManager.Get().Instance.HeartbeatTTL)
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	staleIDs, err := qm.metaStore.ListStaleQueries(ctx)
	cancel()
	if err != nil {
		log.Printf("ERROR: owner reaper failed to list stale queries: %v", err)
		return
	}

	for _, id := range staleIDs {
		// Skip queries we own and are actively running locally.
		qm.activeRegMu.RLock()
		_, local := qm.activeReg[id]
		qm.activeRegMu.RUnlock()
		if local {
			continue
		}

		rCtx, rCancel := context.WithTimeout(context.Background(), 10*time.Second)
		rec, err := qm.metaStore.GetQuery(rCtx, id)
		if err == nil && (rec.State == domain.StateRunning || rec.State == domain.StatePending) {
			rec.State = domain.StateFailed
			rec.FinishedAt = time.Now()
			rec.Error = &domain.QueryError{
				Code:      "OWNER_LOST",
				Message:   "owner instance lost before query completion",
				Retryable: true,
			}
			if err := qm.metaStore.UpdateQuery(rCtx, rec); err != nil {
				log.Printf("ERROR: owner reaper failed to fail query %s: %v", id, err)
			} else {
				log.Printf("Owner reaper: marked query %s as FAILED (owner_lost)", id)
				qm.notifyWatchers(QueryEvent{
					QueryID: id,
					State:   domain.StateFailed,
					Error:   rec.Error,
				})
				telemetry.RecordQueryCompleted(qm.getEngine(rec.DatabaseID), string(domain.StateFailed), 0)
			}
		}
		rCancel()
	}
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
	qm.dbPoolsMu.RLock()
	defer qm.dbPoolsMu.RUnlock()
	pool, ok := qm.dbPools[dbID]
	return pool, ok
}

func (qm *QueryManager) Close() error {
	qm.cancel()
	qm.wg.Wait()

	qm.dbPoolsMu.Lock()
	defer qm.dbPoolsMu.Unlock()
	for _, pool := range qm.dbPools {
		_ = pool.Close()
	}
	return nil
}
