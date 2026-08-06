package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/mrchypark/daramjwee"
)

const commitLuaScript = `
if redis.call("EXISTS", KEYS[3]) == 0 then
  redis.call("SET", KEYS[3], "")
end
redis.call("RENAME", KEYS[3], KEYS[1])
redis.call("SET", KEYS[2], ARGV[1])
return 1
`

// RedisStore is a Redis-based implementation of the daramjwee.Store.
type RedisStore struct {
	client redis.UniversalClient
	logger log.Logger
}

func (rs *RedisStore) GetStreamUsesContext() bool { return true }

func (rs *RedisStore) BeginSetUsesContext() bool { return true }

// New creates a new RedisStore.
func New(client redis.UniversalClient, logger log.Logger) daramjwee.Store {
	return &RedisStore{
		client: client,
		logger: logger,
	}
}

// DataKey generates the Redis key for the data part of an object.
// It uses a hash tag `{key}` to ensure that the data and metadata keys for the
// same object are stored in the same Redis hash slot, which is necessary for
// multi-key operations in a Redis Cluster.
func (rs *RedisStore) DataKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:data", key)
}

// MetaKey generates the Redis key for the metadata part of an object.
// It uses a hash tag `{key}` to ensure that the data and metadata keys for the
// same object are stored in the same Redis hash slot, which is necessary for
// multi-key operations in a Redis Cluster.
func (rs *RedisStore) MetaKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:meta", key)
}

// TempKey generates a temporary Redis key for writing data.
// It uses a hash tag `{key}` to ensure that the temporary key is in the same
// hash slot as the final data and metadata keys.
func (rs *RedisStore) TempKey(key string) string {
	return fmt.Sprintf("daramjwee:{%s}:temp:%s", key, uuid.New().String())
}

// getMetadata retrieves and unmarshals the metadata for a given key.
func (rs *RedisStore) getMetadata(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	metaBytes, err := rs.client.Get(ctx, rs.MetaKey(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
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
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	// Pipeline the three metadata/data checks into a single round trip.
	pipe := rs.client.Pipeline()
	metaCmd := pipe.Get(ctx, rs.MetaKey(key))
	existsCmd := pipe.Exists(ctx, rs.DataKey(key))
	sizeCmd := pipe.StrLen(ctx, rs.DataKey(key))
	if _, err := pipe.Exec(ctx); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	metaBytes, err := metaCmd.Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	}

	var meta daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil { //nolint:govet // shadow: sequential error handling
		return nil, nil, err
	}

	exists, err := existsCmd.Result()
	if err != nil {
		return nil, nil, err
	}
	if exists == 0 {
		_ = level.Warn(rs.logger).Log("msg", "metadata found but data is missing", "key", key)
		return nil, nil, daramjwee.ErrNotFound
	}

	size, err := sizeCmd.Result()
	if err != nil {
		return nil, nil, err
	}

	reader := &redisStreamReader{
		ctx:    ctx,
		client: rs.client,
		key:    rs.DataKey(key),
		size:   size,
		offset: 0,
	}

	return reader, &meta, nil
}

// BeginSet returns a sink that streams data into Redis.
func (rs *RedisStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return rs.beginSet(ctx, key, metadata, "begin set")
}

func (rs *RedisStore) BeginStagedSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.StagedWriteSink, error) {
	return rs.beginSet(ctx, key, metadata, "begin staged set")
}

func (rs *RedisStore) beginSet(ctx context.Context, key string, metadata *daramjwee.Metadata, operation string) (*redisStoreWriter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("redisstore: %s: %w", operation, ctx.Err())
	default:
	}

	w := &redisStoreWriter{
		ctx:      ctx,
		rs:       rs,
		key:      key,
		tempKey:  rs.TempKey(key),
		metadata: metadata,
	}
	return w, nil
}

// Delete removes an object and its metadata from Redis.
func (rs *RedisStore) Delete(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return rs.client.Del(ctx, rs.MetaKey(key), rs.DataKey(key)).Err()
}

// Stat retrieves metadata for an object without its data from Redis.
func (rs *RedisStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
	mu       sync.Mutex
	done     bool
}

// Write appends the provided data to the Redis key.
// It holds the mutex across the Append so that a Write already in flight can
// never land after Commit renames the temp key to its final location. Without
// this, a late Append would recreate the temp key and leak an orphaned object.
func (w *redisStoreWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return 0, io.ErrClosedPipe
	}
	if err := w.rs.client.Append(w.ctx, w.tempKey, string(p)).Err(); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close commits the metadata to Redis.
func (w *redisStoreWriter) Close() error {
	return w.Commit(w.ctx)
}

func (w *redisStoreWriter) Commit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !w.markDone() {
		return nil
	}
	select {
	case <-ctx.Done():
		_ = w.rs.client.Del(context.Background(), w.tempKey).Err()
		return fmt.Errorf("redisstore: commit: %w", ctx.Err())
	case <-w.ctx.Done():
		_ = w.rs.client.Del(context.Background(), w.tempKey).Err()
		return fmt.Errorf("redisstore: commit: %w", w.ctx.Err())
	default:
	}

	metaBytes, err := json.Marshal(w.metadata)
	if err != nil {
		_ = level.Error(w.rs.logger).Log("msg", "failed to marshal metadata", "key", w.key, "err", err)
		if delErr := w.rs.client.Del(context.Background(), w.tempKey).Err(); delErr != nil {
			_ = level.Error(w.rs.logger).Log("msg", "failed to delete temporary key", "key", w.key, "err", delErr)
		}
		return fmt.Errorf("redisstore: marshal metadata: %w", err)
	}

	_, err = w.rs.client.Eval(
		ctx,
		commitLuaScript,
		[]string{w.rs.DataKey(w.key), w.rs.MetaKey(w.key), w.tempKey},
		metaBytes,
	).Result()
	if err != nil {
		_ = level.Error(w.rs.logger).Log("msg", "failed to commit data and metadata", "key", w.key, "err", err)
		// Attempt to clean up the temporary key
		if delErr := w.rs.client.Del(context.Background(), w.tempKey).Err(); delErr != nil {
			_ = level.Error(w.rs.logger).Log("msg", "failed to delete temporary key", "key", w.key, "err", delErr)
		}
		return fmt.Errorf("redisstore: commit: %w", err)
	}

	return nil
}

func (w *redisStoreWriter) Abort() error {
	if !w.markDone() {
		return nil
	}
	return w.rs.client.Del(context.Background(), w.tempKey).Err()
}

func (w *redisStoreWriter) markDone() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	return true
}

// redisStreamReader is a helper type that satisfies the io.ReadCloser interface
// for streaming data from Redis.
type redisStreamReader struct {
	ctx    context.Context
	client redis.UniversalClient
	key    string
	size   int64
	offset int64
}

// Read reads a chunk of data from Redis.
func (r *redisStreamReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	// Calculate the end of the range to read
	end := r.offset + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	// Read the chunk from Redis
	chunk, err := r.client.GetRange(r.ctx, r.key, r.offset, end).Result()
	if err != nil {
		return 0, err
	}

	// Copy the chunk to the buffer
	n = copy(p, chunk)
	r.offset += int64(n)

	return n, nil
}

// Close is a no-op for the redisStreamReader.
func (r *redisStreamReader) Close() error {
	return nil
}
