package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"dbbridge/internal/core/domain"

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

func (r *RedisMetaStore) PutQuery(ctx context.Context, record *domain.QueryRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}

	key := queryKey(record.ID)
	// We keep the query record metadata for at least the query's TTL + some buffer (e.g. 7 days if TTL is short, or just Max of TTL)
	ttl := record.Options.ResultTTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	// Buffer of 24h to avoid race condition with GC
	expiration := ttl + 24*time.Hour

	pipe := r.client.Pipeline()
	pipe.Set(ctx, key, data, expiration)

	// Track every database that has been queried (for ListDatabasesSeen).
	if record.DatabaseID != "" {
		pipe.SAdd(ctx, databasesSeenKey, record.DatabaseID)
	}

	// If the query is active, add it to instance owned set
	if !record.State.IsTerminal() {
		pipe.SAdd(ctx, instanceQueriesKey(record.OwnerInstanceID), record.ID)
	} else {
		pipe.SRem(ctx, instanceQueriesKey(record.OwnerInstanceID), record.ID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to put query in redis: %w", err)
	}

	return nil
}

func (r *RedisMetaStore) GetQuery(ctx context.Context, id string) (*domain.QueryRecord, error) {
	key := queryKey(id)
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to get query from redis: %w", err)
	}

	var rec domain.QueryRecord
	if err := json.Unmarshal([]byte(val), &rec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query: %w", err)
	}

	return &rec, nil
}

func (r *RedisMetaStore) UpdateQuery(ctx context.Context, record *domain.QueryRecord) error {
	return r.PutQuery(ctx, record)
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

func (r *RedisMetaStore) Heartbeat(ctx context.Context, instanceID string, ownedQueryIDs []string, ttl time.Duration) error {
	pipe := r.client.Pipeline()
	ikey := instanceKey(instanceID)
	pipe.Set(ctx, ikey, "alive", ttl)

	// Refresh lease deadline for each owned query record
	now := time.Now()
	deadline := now.Add(ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis heartbeat failed: %w", err)
	}

	// Now let's update lease deadlines of owned queries
	for _, id := range ownedQueryIDs {
		rec, err := r.GetQuery(ctx, id)
		if err == nil {
			rec.LeaseDeadline = deadline
			_ = r.UpdateQuery(ctx, rec)
		}
	}

	return nil
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
		defer pubsub.Close()
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

func (r *RedisMetaStore) ListExpiredQueries(ctx context.Context) ([]string, error) {
	// Scan dbbridge:query:* and check if they are expired based on finished_at and result_ttl.
	// Since keys themselves have Redis TTL, Redis will automatically expire them.
	// But we need to clean up results in storage (fs/s3/clickhouse) when query metadata is expired.
	// We can scan keys dbbridge:query:* to find which queries are terminal and finished_at + TTL is in the past.
	// We'll return IDs of those records so that GC-worker can clean up storage results before Redis deletes them.
	// To prevent Redis from deleting them before our GC can clean up, query metadata in Redis is stored with
	// an extra 24-hour buffer (see PutQuery). So metadata is still there after retention has passed.
	var expired []string
	var cursor uint64
	now := time.Now()

	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, "dbbridge:query:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan queries failed: %w", err)
		}

		for _, k := range keys {
			val, err := r.client.Get(ctx, k).Result()
			if err != nil {
				continue
			}
			var rec domain.QueryRecord
			if err := json.Unmarshal([]byte(val), &rec); err == nil {
				if rec.State.IsTerminal() && !rec.FinishedAt.IsZero() {
					ttl := rec.Options.ResultTTL
					if ttl == 0 {
						ttl = 24 * time.Hour
					}
					if now.After(rec.FinishedAt.Add(ttl)) {
						expired = append(expired, rec.ID)
					}
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return expired, nil
}

func (r *RedisMetaStore) ListStaleQueries(ctx context.Context) ([]string, error) {
	// Scan all query records; a non-terminal query whose owner instance key has
	// expired (no heartbeat) is considered stale (owner_lost).
	var stale []string
	var cursor uint64

	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, "dbbridge:query:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan queries failed: %w", err)
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
			if rec.State != domain.StatePending && rec.State != domain.StateRunning {
				continue
			}
			exists, err := r.client.Exists(ctx, instanceKey(rec.OwnerInstanceID)).Result()
			if err == nil && exists == 0 {
				stale = append(stale, rec.ID)
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return stale, nil
}

func (r *RedisMetaStore) DeleteQuery(ctx context.Context, id string) error {
	rec, err := r.GetQuery(ctx, id)
	pipe := r.client.Pipeline()
	pipe.Del(ctx, queryKey(id))

	if err == nil {
		pipe.SRem(ctx, instanceQueriesKey(rec.OwnerInstanceID), id)
		if rec.IdempotencyKey != "" {
			pipe.Del(ctx, idempotencyKey(rec.DatabaseID, rec.IdempotencyKey))
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis delete query failed: %w", err)
	}
	return nil
}

func (r *RedisMetaStore) Close() error {
	return r.client.Close()
}
