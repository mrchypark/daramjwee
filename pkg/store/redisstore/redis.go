
package redisstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/mrchypark/daramjwee"
	"github.com/redis/go-redis/v9"
)

// RedisStore is a Redis-based implementation of the daramjwee.Store.
type RedisStore struct {
	client redis.UniversalClient
	logger log.Logger
}

// New creates a new RedisStore.
func New(client redis.UniversalClient, logger log.Logger) daramjwee.Store {
	return &RedisStore{
		client: client,
		logger: logger,
	}
}

// dataKey generates the Redis key for the data part of an object.
// It uses a hash tag `{key}` to ensure that the data and metadata keys for the
// same object are stored in the same Redis hash slot, which is necessary for
// multi-key operations in a Redis Cluster.
func (rs *RedisStore) dataKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:data", key)
}

// metaKey generates the Redis key for the metadata part of an object.
// It uses a hash tag `{key}` to ensure that the data and metadata keys for the
// same object are stored in the same Redis hash slot, which is necessary for
// multi-key operations in a Redis Cluster.
func (rs *RedisStore) metaKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:meta", key)
}

// tempKey generates a temporary Redis key for writing data.
// It uses a hash tag `{key}` to ensure that the temporary key is in the same
// hash slot as the final data and metadata keys.
func (rs *RedisStore) tempKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:temp:%s", key, uuid.New().String())
}

// getMetadata retrieves and unmarshals the metadata for a given key.
func (rs *RedisStore) getMetadata(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	metaBytes, err := rs.client.Get(ctx, rs.metaKey(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}

	var meta daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetStream retrieves an object and its metadata as a stream from Redis.
func (rs *RedisStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	meta, err := rs.getMetadata(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	data, err := rs.client.Get(ctx, rs.dataKey(key)).Bytes()
	if err != nil {
		if err == redis.Nil {
			// This indicates an inconsistent state where metadata exists but data does not.
			level.Warn(rs.logger).Log("msg", "metadata found but data is missing", "key", key)
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	return io.NopCloser(bytes.NewReader(data)), meta, nil
}

// SetWithWriter returns a writer that streams data into Redis.
func (rs *RedisStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	w := &redisStoreWriter{
		ctx:      ctx,
		rs:       rs,
		key:      key,
		tempKey:  rs.tempKey(key),
		metadata: metadata,
	}
	return w, nil
}

// Delete removes an object and its metadata from Redis.
func (rs *RedisStore) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return rs.client.Del(ctx, rs.metaKey(key), rs.dataKey(key)).Err()
}

// Stat retrieves metadata for an object without its data from Redis.
func (rs *RedisStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return rs.getMetadata(ctx, key)
}

// redisStoreWriter is a helper type that satisfies the io.WriteCloser interface.
// It streams data to a temporary key in Redis and, upon closing, atomically
// renames the temporary key to the final key and sets the metadata.
// This approach ensures that the write operation is atomic and avoids buffering
// the entire file in memory.
type redisStoreWriter struct {
	ctx      context.Context
	rs       *RedisStore
	key      string
	tempKey  string
	metadata *daramjwee.Metadata
}

// Write appends the provided data to the Redis key.
func (w *redisStoreWriter) Write(p []byte) (n int, err error) {
	if err := w.rs.client.Append(w.ctx, w.tempKey, string(p)).Err(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close commits the metadata to Redis.
func (w *redisStoreWriter) Close() error {
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
	}

	metaBytes, err := json.Marshal(w.metadata)
	if err != nil {
		level.Error(w.rs.logger).Log("msg", "failed to marshal metadata", "key", w.key, "err", err)
		return err
	}

	pipe := w.rs.client.Pipeline()
	pipe.Set(w.ctx, w.rs.metaKey(w.key), metaBytes, 0)
	pipe.Rename(w.ctx, w.tempKey, w.rs.dataKey(w.key))

	if _, err := pipe.Exec(w.ctx); err != nil {
		level.Error(w.rs.logger).Log("msg", "failed to commit data and metadata", "key", w.key, "err", err)
		// Attempt to clean up the temporary key
		if delErr := w.rs.client.Del(w.ctx, w.tempKey).Err(); delErr != nil {
			level.Error(w.rs.logger).Log("msg", "failed to delete temporary key", "key", w.key, "err", delErr)
		}
		return err
	}

	return nil
}
