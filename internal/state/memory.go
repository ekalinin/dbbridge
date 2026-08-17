package state

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"
)

type MemoryMetaStore struct {
	mu           sync.RWMutex
	queries      map[string]*domain.QueryRecord
	idempotency  map[string]string // key: dbID:key -> value: queryID
	idempExpires map[string]time.Time
	subscribers  []chan ControlMsg
	leases       map[string]time.Time // queryID -> lease expiry
	locks        map[string]time.Time // lock name -> expiry
}

func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		queries:      make(map[string]*domain.QueryRecord),
		idempotency:  make(map[string]string),
		idempExpires: make(map[string]time.Time),
		leases:       make(map[string]time.Time),
		locks:        make(map[string]time.Time),
	}
}

// store writes the record and mirrors the lease bookkeeping the Redis store
// does: a terminal record has no lease. Callers must hold the write lock.
func (m *MemoryMetaStore) store(record *domain.QueryRecord) {
	recCopy := *record
	m.queries[record.ID] = &recCopy
	if record.State.IsTerminal() {
		delete(m.leases, record.ID)
	}
}

func (m *MemoryMetaStore) PutQuery(ctx context.Context, record *domain.QueryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store(record)
	return nil
}

func (m *MemoryMetaStore) GetQuery(ctx context.Context, id string) (*domain.QueryRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.queries[id]
	if !ok {
		return nil, ErrNotFound
	}

	// Clone to avoid data race
	recCopy := *rec
	// LeaseDeadline is derived from the lease, not stored in the record.
	if exp, ok := m.leases[id]; ok {
		recCopy.LeaseDeadline = exp
	}
	return &recCopy, nil
}

func (m *MemoryMetaStore) UpdateQuery(ctx context.Context, record *domain.QueryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queries[record.ID]; !ok {
		return ErrNotFound
	}

	m.store(record)
	return nil
}

func (m *MemoryMetaStore) UpdateQueryIfState(ctx context.Context, record *domain.QueryRecord, expected ...domain.QueryState) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.queries[record.ID]
	if !ok {
		return false, ErrNotFound
	}
	if !slices.Contains(expected, current.State) {
		return false, nil
	}

	m.store(record)
	return true, nil
}

func (m *MemoryMetaStore) AcquireIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullKey := dbID + ":" + key
	if exp, ok := m.idempExpires[fullKey]; ok && time.Now().Before(exp) {
		return m.idempotency[fullKey], false, nil
	}

	m.idempotency[fullKey] = queryID
	m.idempExpires[fullKey] = time.Now().Add(ttl)
	return queryID, true, nil
}

func (m *MemoryMetaStore) ReleaseIdempotency(ctx context.Context, dbID, key, queryID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fullKey := dbID + ":" + key
	if m.idempotency[fullKey] != queryID {
		return nil
	}
	delete(m.idempotency, fullKey)
	delete(m.idempExpires, fullKey)
	return nil
}

func (m *MemoryMetaStore) RefreshIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ttl <= 0 {
		return nil
	}
	fullKey := dbID + ":" + key
	if m.idempotency[fullKey] != queryID {
		return nil
	}
	m.idempExpires[fullKey] = time.Now().Add(ttl)
	return nil
}

func (m *MemoryMetaStore) Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// Leases live beside the records, never inside them, so refreshing one
	// cannot clobber a concurrent terminal write.
	for _, id := range ownedQueryIDs {
		m.leases[id] = now.Add(ttl)
	}
	return nil
}

func (m *MemoryMetaStore) TryLock(ctx context.Context, name string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if exp, ok := m.locks[name]; ok && now.Before(exp) {
		return false, nil
	}
	m.locks[name] = now.Add(ttl)
	return true, nil
}

func (m *MemoryMetaStore) PublishControl(ctx context.Context, msg ControlMsg) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Broadcast to all active memory subscribers
	for _, sub := range m.subscribers {
		select {
		case sub <- msg:
		default:
			// Non-blocking write to avoid hanging if subscriber is slow
		}
	}
	return nil
}

func (m *MemoryMetaStore) SubscribeControl(ctx context.Context) (<-chan ControlMsg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan ControlMsg, 100)
	m.subscribers = append(m.subscribers, ch)

	// Clean up subscriber when context is cancelled
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, sub := range m.subscribers {
			if sub == ch {
				m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}()

	return ch, nil
}

func (m *MemoryMetaStore) CountInFlight(ctx context.Context, instanceID string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, q := range m.queries {
		if q.OwnerInstanceID == instanceID && (q.State == domain.StatePending || q.State == domain.StateRunning) {
			count++
		}
	}
	return count, nil
}

func (m *MemoryMetaStore) ListByInstance(ctx context.Context, instanceID string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []string
	for _, q := range m.queries {
		if q.OwnerInstanceID == instanceID && !q.State.IsTerminal() {
			ids = append(ids, q.ID)
		}
	}
	return ids, nil
}

func (m *MemoryMetaStore) ListDatabasesSeen(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{})
	var dbs []string
	for _, q := range m.queries {
		if _, ok := seen[q.DatabaseID]; ok {
			continue
		}
		seen[q.DatabaseID] = struct{}{}
		dbs = append(dbs, q.DatabaseID)
	}
	return dbs, nil
}

func (m *MemoryMetaStore) ListExpiredQueries(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var expired []string
	now := time.Now()
	for _, q := range m.queries {
		if q.State.IsTerminal() && !q.FinishedAt.IsZero() {
			ttl := q.Options.ResultTTL
			if ttl == 0 {
				ttl = 24 * time.Hour // fallback
			}
			if now.After(q.FinishedAt.Add(ttl)) {
				expired = append(expired, q.ID)
			}
		}
	}
	return expired, nil
}

func (m *MemoryMetaStore) ListStaleQueries(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var stale []string
	for _, q := range m.queries {
		if q.State != domain.StatePending && q.State != domain.StateRunning {
			continue
		}
		exp, ok := m.leases[q.ID]
		if !ok || now.After(exp) {
			stale = append(stale, q.ID)
		}
	}
	return stale, nil
}

func (m *MemoryMetaStore) DeleteQuery(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up related idempotency keys if any
	q, ok := m.queries[id]
	if ok && q.IdempotencyKey != "" {
		fullKey := q.DatabaseID + ":" + q.IdempotencyKey
		delete(m.idempotency, fullKey)
		delete(m.idempExpires, fullKey)
	}

	delete(m.queries, id)
	delete(m.leases, id)
	return nil
}

func (m *MemoryMetaStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sub := range m.subscribers {
		close(sub)
	}
	m.subscribers = nil
	return nil
}
