
package redisstore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/redis/go-redis/v9"
)

const (
	dataKeyPrefix = "daramjwee:data:"
	metaKeyPrefix = "daramjwee:meta:"
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

// GetStream retrieves an object and its metadata as a stream from Redis.
func (rs *RedisStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	pipe := rs.client.Pipeline()
	metaCmd := pipe.Get(ctx, metaKeyPrefix+key)
	dataCmd := pipe.Get(ctx, dataKeyPrefix+key)

	_, err := pipe.Exec(ctx)
	if err != nil {
		if err == redis.Nil {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	metaBytes, err := metaCmd.Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	var meta daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, nil, err
	}

	data, err := dataCmd.Bytes()
	if err != nil {
		return nil, nil, err
	}

	return io.NopCloser(bytes.NewReader(data)), &meta, nil
}

// SetWithWriter returns a writer that streams data into Redis.
func (rs *RedisStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	w := &redisStoreWriter{
		ctx:      ctx,
		rs:       rs,
		key:      key,
		metadata: metadata,
		buf:      new(bytes.Buffer),
	}
	return w, nil
}

// Delete removes an object and its metadata from Redis.
func (rs *RedisStore) Delete(ctx context.Context, key string) error {
	return rs.client.Del(ctx, metaKeyPrefix+key, dataKeyPrefix+key).Err()
}

// Stat retrieves metadata for an object without its data from Redis.
func (rs *RedisStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	metaBytes, err := rs.client.Get(ctx, metaKeyPrefix+key).Bytes()
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

// redisStoreWriter is a helper type that satisfies the io.WriteCloser interface.
type redisStoreWriter struct {
	ctx      context.Context
	rs       *RedisStore
	key      string
	metadata *daramjwee.Metadata
	buf      *bytes.Buffer
}

// Write writes the provided data to the internal buffer.
func (w *redisStoreWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

// Close commits the buffered data and metadata to Redis atomically.
func (w *redisStoreWriter) Close() error {
	metaBytes, err := json.Marshal(w.metadata)
	if err != nil {
		level.Error(w.rs.logger).Log("msg", "failed to marshal metadata", "key", w.key, "err", err)
		return err
	}

	_, err = w.rs.client.Pipelined(w.ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(w.ctx, metaKeyPrefix+w.key, metaBytes, 0)
		pipe.Set(w.ctx, dataKeyPrefix+w.key, w.buf.Bytes(), 0)
		return nil
	})

	if err != nil {
		level.Error(w.rs.logger).Log("msg", "failed to set data and metadata in redis", "key", w.key, "err", err)
	}

	return err
}
