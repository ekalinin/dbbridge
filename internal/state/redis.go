package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/ekalinin/dbbridge/internal/core/domain"

	"github.com/redis/go-redis/v9"
)

type RedisMetaStore struct {
	client *redis.Client
}

func NewRedisMetaStore(addr, password string, db int) *RedisMetaStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisMetaStore{client: rdb}
}

func queryKey(id string) string {
	return "dbbridge:query:" + id
}

// leaseKey holds the owner instance ID of an in-flight query and expires with
// the heartbeat TTL. Keeping the lease out of the record is what lets Heartbeat
// run without a read-modify-write of the record itself.
func leaseKey(id string) string {
	return "dbbridge:lease:" + id
}

func lockKey(name string) string {
	return "dbbridge:lock:" + name
}

func idempotencyKey(dbID, key string) string {
	return fmt.Sprintf("dbbridge:idempotency:%s:%s", dbID, key)
}

func instanceKey(id string) string {
	return "dbbridge:instance:" + id
}

func instanceQueriesKey(id string) string {
	return fmt.Sprintf("dbbridge:instance:%s:queries", id)
}

const controlChannel = "dbbridge:control:channel"

const databasesSeenKey = "dbbridge:databases:seen"

// writeQuery queues the commands that persist a record. It is shared by the
// plain write path and the conditional (WATCH-guarded) one.
func (r *RedisMetaStore) writeQuery(ctx context.Context, pipe redis.Pipeliner, record *domain.QueryRecord, data []byte) {
	// We keep the query record metadata for at least the query's TTL + some buffer (e.g. 7 days if TTL is short, or just Max of TTL)
	ttl := record.Options.ResultTTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	// Buffer of 24h to avoid race condition with GC
	expiration := ttl + 24*time.Hour

	pipe.Set(ctx, queryKey(record.ID), data, expiration)

	// Track every database that has been queried (for ListDatabasesSeen).
	if record.DatabaseID != "" {
		pipe.SAdd(ctx, databasesSeenKey, record.DatabaseID)
	}

	// If the query is active, add it to instance owned set
	if !record.State.IsTerminal() {
		pipe.SAdd(ctx, instanceQueriesKey(record.OwnerInstanceID), record.ID)
	} else {
		pipe.SRem(ctx, instanceQueriesKey(record.OwnerInstanceID), record.ID)
		// A terminal query is nobody's lease any more; dropping the key stops
		// the reaper from ever looking at it again.
		pipe.Del(ctx, leaseKey(record.ID))
	}
}

func (r *RedisMetaStore) PutQuery(ctx context.Context, record *domain.QueryRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}

	pipe := r.client.Pipeline()
	r.writeQuery(ctx, pipe, record, data)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to put query in redis: %w", err)
	}

	return nil
}

func (r *RedisMetaStore) GetQuery(ctx context.Context, id string) (*domain.QueryRecord, error) {
	// The lease TTL is fetched in the same round trip: LeaseDeadline is derived
	// from the lease key rather than stored inside the record.
	pipe := r.client.Pipeline()
	get := pipe.Get(ctx, queryKey(id))
	lease := pipe.TTL(ctx, leaseKey(id))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("failed to get query from redis: %w", err)
	}

	val, err := get.Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to get query from redis: %w", err)
	}

	var rec domain.QueryRecord
	if err := json.Unmarshal([]byte(val), &rec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query: %w", err)
	}

	if ttl, err := lease.Result(); err == nil && ttl > 0 {
		rec.LeaseDeadline = time.Now().Add(ttl)
	}

	return &rec, nil
}

func (r *RedisMetaStore) UpdateQuery(ctx context.Context, record *domain.QueryRecord) error {
	return r.PutQuery(ctx, record)
}

func (r *RedisMetaStore) UpdateQueryIfState(ctx context.Context, record *domain.QueryRecord, expected ...domain.QueryState) (bool, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("failed to marshal query: %w", err)
	}

	key := queryKey(record.ID)
	var written bool

	txf := func(tx *redis.Tx) error {
		val, err := tx.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return ErrNotFound
		} else if err != nil {
			return err
		}

		var current domain.QueryRecord
		if err := json.Unmarshal([]byte(val), &current); err != nil {
			return fmt.Errorf("failed to unmarshal query: %w", err)
		}
		if !slices.Contains(expected, current.State) {
			written = false
			return nil
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			r.writeQuery(ctx, pipe, record, data)
			return nil
		})
		if err != nil {
			return err
		}
		written = true
		return nil
	}

	// A concurrent write to the same record aborts the transaction; that is the
	// fencing signal callers care about, so it is reported as "not written"
	// rather than as an error.
	if err := r.client.Watch(ctx, txf, key); err != nil {
		if errors.Is(err, redis.TxFailedErr) {
			return false, nil
		}
		if errors.Is(err, ErrNotFound) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("conditional query update failed: %w", err)
	}

	return written, nil
}

func (r *RedisMetaStore) AcquireIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) (string, bool, error) {
	rkey := idempotencyKey(dbID, key)
	// Try to set query_id if key does not exist
	set, err := r.client.SetNX(ctx, rkey, queryID, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("redis setnx failed: %w", err)
	}

	if set {
		return queryID, true, nil
	}

	// Key already exists, get the existing query ID
	val, err := r.client.Get(ctx, rkey).Result()
	if err != nil {
		return "", false, fmt.Errorf("redis get existing idempotency failed: %w", err)
	}

	return val, false, nil
}

// releaseIdempotencyScript deletes the key only when it still points at the
// query that acquired it.
var releaseIdempotencyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// refreshIdempotencyScript re-arms the TTL only for the owning query.
var refreshIdempotencyScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

func (r *RedisMetaStore) ReleaseIdempotency(ctx context.Context, dbID, key, queryID string) error {
	rkey := idempotencyKey(dbID, key)
	if err := releaseIdempotencyScript.Run(ctx, r.client, []string{rkey}, queryID).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis release idempotency failed: %w", err)
	}
	return nil
}

func (r *RedisMetaStore) RefreshIdempotency(ctx context.Context, dbID, key, queryID string, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	rkey := idempotencyKey(dbID, key)
	err := refreshIdempotencyScript.Run(ctx, r.client, []string{rkey}, queryID, ttl.Milliseconds()).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis refresh idempotency failed: %w", err)
	}
	return nil
}

func (r *RedisMetaStore) Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error {
	pipe := r.client.Pipeline()
	pipe.Set(ctx, instanceKey(instanceID), "alive", ttl)

	// One SET per owned query. The record itself is never read or rewritten
	// here, so a concurrent terminal write can no longer be clobbered.
	for _, id := range ownedQueryIDs {
		pipe.Set(ctx, leaseKey(id), instanceID, ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis heartbeat failed: %w", err)
	}

	return nil
}

func (r *RedisMetaStore) TryLock(ctx context.Context, name string, ttl time.Duration) (bool, error) {
	ok, err := r.client.SetNX(ctx, lockKey(name), "held", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis try lock failed: %w", err)
	}
	return ok, nil
}

func (r *RedisMetaStore) PublishControl(ctx context.Context, msg ControlMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal control message: %w", err)
	}

	err = r.client.Publish(ctx, controlChannel, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish control msg: %w", err)
	}
	return nil
}

func (r *RedisMetaStore) SubscribeControl(ctx context.Context) (<-chan ControlMsg, error) {
	pubsub := r.client.Subscribe(ctx, controlChannel)
	ch := make(chan ControlMsg, 100)

	go func() {
		defer func() {
			if cerr := pubsub.Close(); cerr != nil {
				log.Printf("ERROR: failed to close control subscription: %v", cerr)
			}
		}()
		defer close(ch)

		redisCh := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-redisCh:
				if !ok {
					return
				}
				var ctrl ControlMsg
				if err := json.Unmarshal([]byte(msg.Payload), &ctrl); err == nil {
					ch <- ctrl
				}
			}
		}
	}()

	return ch, nil
}

func (r *RedisMetaStore) CountInFlight(ctx context.Context, instanceID string) (int, error) {
	card, err := r.client.SCard(ctx, instanceQueriesKey(instanceID)).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get active query count from redis: %w", err)
	}
	return int(card), nil
}

func (r *RedisMetaStore) ListByInstance(ctx context.Context, instanceID string) ([]string, error) {
	ids, err := r.client.SMembers(ctx, instanceQueriesKey(instanceID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list queries by instance from redis: %w", err)
	}
	return ids, nil
}

func (r *RedisMetaStore) ListDatabasesSeen(ctx context.Context) ([]string, error) {
	ids, err := r.client.SMembers(ctx, databasesSeenKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list databases seen from redis: %w", err)
	}
	return ids, nil
}

// scanQueries walks every query record with SCAN + GET and calls visit for each
// one. Known limitation: this is O(number of retained records) per pass on every
// node. A `finished_at` index (ZSET) would be the next step if the record count
// grows beyond what a once-a-minute scan can absorb.
func (r *RedisMetaStore) scanQueries(ctx context.Context, visit func(rec *domain.QueryRecord)) error {
	var cursor uint64
	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, "dbbridge:query:*", 100).Result()
		if err != nil {
			return fmt.Errorf("scan queries failed: %w", err)
		}

		for _, k := range keys {
			val, err := r.client.Get(ctx, k).Result()
			if err != nil {
				continue
			}
			var rec domain.QueryRecord
			if err := json.Unmarshal([]byte(val), &rec); err != nil {
				continue
			}
			visit(&rec)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (r *RedisMetaStore) ListExpiredQueries(ctx context.Context) ([]string, error) {
	// Query keys carry their own Redis TTL, but the results in fs/s3/clickhouse
	// do not, so GC still has to find retention-expired records itself. PutQuery
	// adds a 24h buffer to the key TTL exactly so metadata outlives retention
	// and GC gets a chance to clean storage first.
	var expired []string
	now := time.Now()

	err := r.scanQueries(ctx, func(rec *domain.QueryRecord) {
		if !rec.State.IsTerminal() || rec.FinishedAt.IsZero() {
			return
		}
		ttl := rec.Options.ResultTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}
		if now.After(rec.FinishedAt.Add(ttl)) {
			expired = append(expired, rec.ID)
		}
	})
	if err != nil {
		return nil, err
	}

	return expired, nil
}

func (r *RedisMetaStore) ListStaleQueries(ctx context.Context) ([]string, error) {
	// A non-terminal query with no lease key has no live owner refreshing it.
	// Checking the lease rather than the owner's instance key also covers an
	// owner that restarted under the same instance ID.
	var stale []string

	err := r.scanQueries(ctx, func(rec *domain.QueryRecord) {
		if rec.State != domain.StatePending && rec.State != domain.StateRunning {
			return
		}
		exists, err := r.client.Exists(ctx, leaseKey(rec.ID)).Result()
		if err == nil && exists == 0 {
			stale = append(stale, rec.ID)
		}
	})
	if err != nil {
		return nil, err
	}

	return stale, nil
}

func (r *RedisMetaStore) DeleteQuery(ctx context.Context, id string) error {
	rec, err := r.GetQuery(ctx, id)
	pipe := r.client.Pipeline()
	pipe.Del(ctx, queryKey(id))
	pipe.Del(ctx, leaseKey(id))

	if err == nil {
		pipe.SRem(ctx, instanceQueriesKey(rec.OwnerInstanceID), id)
		if rec.IdempotencyKey != "" {
			pipe.Del(ctx, idempotencyKey(rec.DatabaseID, rec.IdempotencyKey))
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis delete query failed: %w", err)
	}
	return nil
}

func (r *RedisMetaStore) Close() error {
	return r.client.Close()
}
