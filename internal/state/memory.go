package state

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"dbbridge/internal/core/domain"
)

type MemoryMetaStore struct {
	mu           sync.RWMutex
	queries      map[string]*domain.QueryRecord
	idempotency  map[string]string // key: dbID:key -> value: queryID
	idempExpires map[string]time.Time
	subscribers  []chan ControlMsg
	instanceKeys map[string]time.Time
}

func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		queries:      make(map[string]*domain.QueryRecord),
		idempotency:  make(map[string]string),
		idempExpires: make(map[string]time.Time),
		instanceKeys: make(map[string]time.Time),
	}
}

func (m *MemoryMetaStore) PutQuery(ctx context.Context, record *domain.QueryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clone to avoid side effects
	recCopy := *record
	m.queries[record.ID] = &recCopy
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
	return &recCopy, nil
}

func (m *MemoryMetaStore) UpdateQuery(ctx context.Context, record *domain.QueryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queries[record.ID]; !ok {
		return ErrNotFound
	}

	recCopy := *record
	m.queries[record.ID] = &recCopy
	return nil
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

func (m *MemoryMetaStore) Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.instanceKeys[instanceID] = now.Add(ttl)

	// Update lease deadlines for owned queries
	for _, id := range ownedQueryIDs {
		if q, ok := m.queries[id]; ok {
			q.LeaseDeadline = now.Add(ttl)
		}
	}
	return nil
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
		exp, ok := m.instanceKeys[q.OwnerInstanceID]
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

// Marshal helper for deep copying or serialization in tests if needed
func deepCopy(src, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
