// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
	"golang.org/x/sync/errgroup"
	// "golang.org/x/sync/singleflight" // TODO: Add singleflight.Group later
)

// DaramjweeCache implements the Cache interface, providing a two-tier caching system
// (hot and cold) with background refresh capabilities.
type DaramjweeCache struct {
	// HotStore is the primary, high-speed cache storage.
	HotStore Store
	// ColdStore is the secondary, slower but larger cache storage (optional).
	ColdStore Store
	// Logger is used for logging messages.
	Logger log.Logger
	// Worker manages background tasks like cache refresh and promotion.
	Worker *worker.Manager
	// DefaultTimeout specifies the default timeout for cache operations.
	DefaultTimeout time.Duration
	// flightGroup      singleflight.Group // TODO: Add singleflight.Group later
}

// Ensures at compile time that DaramjweeCache satisfies the Cache interface.
var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves an object. It first checks the hot cache, then the cold cache,
// and finally fetches from the origin if not found in either cache.
// If the item is found in the hot cache, a background refresh is scheduled.
// If found in the cold cache, it's promoted to the hot cache.
// The context controls the timeout for the entire operation.
// The fetcher is used to get the data from the origin.
// It returns an io.ReadCloser for the data stream and an error if any occurred.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Check Hot Cache
	hotStream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)
		c.ScheduleRefresh(context.Background(), key, fetcher) // Schedule background refresh
		return hotStream, nil
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Check Cold Cache
	coldStream, coldMeta, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)
		// Promote to hot cache and tee the stream
		return c.promoteAndTeeStream(ctx, key, coldMeta.ETag, coldStream)
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Fetch from Origin
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

	var oldETag string
	if meta, err := c.statFromStore(ctx, c.HotStore, key); err == nil && meta != nil {
		oldETag = meta.ETag
	}

	result, err := fetcher.Fetch(ctx, oldETag)
	if err != nil {
		if err == ErrNotModified {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			return stream, nil
		}
		return nil, err
	}

	hotTeeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		// If teeing to hot cache fails, return the original stream directly.
		// The error is logged within cacheAndTeeStream.
		return result.Body, nil
	}

	// Schedule a background task to also set the fetched item to the cold store.
	c.scheduleSetToStore(context.Background(), c.ColdStore, key)

	return hotTeeStream, nil
}

// Set returns an io.WriteCloser for streaming data directly into the HotStore.
// The data is written to the HotStore as it is written to the WriteCloser.
// The cache entry is finalized and visible only when the WriteCloser is closed successfully.
// The provided ETag is associated with the cache entry.
// The context controls the timeout for the initial setup of the stream.
// IMPORTANT: The caller MUST call Close() on the returned io.WriteCloser to finalize
// the operation and release associated resources. Failing to do so may leave
// the cache in an inconsistent state or leak resources.
func (c *DaramjweeCache) Set(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	if c.HotStore == nil {
		return nil, &ConfigError{"HotStore is not configured"}
	}

	ctx, cancel := c.newCtxWithTimeout(ctx)

	wc, err := c.setStreamToStore(ctx, c.HotStore, key, etag)
	if err != nil {
		cancel()
		cancel() // Ensure cancellation if an error occurs before returning the writer
		return nil, err
	}

	// newCancelWriteCloser ensures that the context's cancel function is called when the writer is closed.
	return newCancelWriteCloser(wc, cancel), nil
}

// Delete removes an object from all cache tiers (hot and cold) concurrently.
// It attempts to delete from both stores even if one fails.
// The context controls the timeout for the delete operations.
// Returns an error if any of the delete operations fail.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx) // Use an errgroup to manage concurrent deletions

	// Delete from HotStore
	if c.HotStore != nil {
		g.Go(func() error {
			err := c.deleteFromStore(gCtx, c.HotStore, key)
			if err != nil {
				level.Error(c.Logger).Log("msg", "failed to delete from hot store", "key", key, "err", err)
				return err // Return the error to the errgroup
			}
			level.Debug(c.Logger).Log("msg", "deleted from hot store", "key", key)
			return nil
		})
	}

	// Delete from ColdStore
	if c.ColdStore != nil {
		g.Go(func() error {
			err := c.deleteFromStore(gCtx, c.ColdStore, key)
			if err != nil {
				level.Error(c.Logger).Log("msg", "failed to delete from cold store", "key", key, "err", err)
				return err // Return the error to the errgroup
			}
			level.Debug(c.Logger).Log("msg", "deleted from cold store", "key", key)
			return nil
		})
	}

	return g.Wait() // Wait for all delete operations to complete and return the first error encountered
}

// ScheduleRefresh submits a background cache refresh job to the worker.
// This method is non-blocking and returns immediately.
// The actual refresh operation (fetching from origin and updating the hot cache)
// happens asynchronously in a separate goroutine managed by the worker.
// The provided context is for the scheduling operation itself, not the background job.
// The fetcher is used to get the updated data from the origin.
// Returns an error if the worker is not configured or if submitting the job fails.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule refresh", "key", key)
		return errors.New("worker is not configured, cannot schedule refresh") // TODO: Use a more specific error type
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		var oldETag string
		if meta, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil && meta != nil {
			oldETag = meta.ETag
		}

		result, err := fetcher.Fetch(jobCtx, oldETag)
		if err != nil {
			if err == ErrNotModified {
				level.Debug(c.Logger).Log("msg", "background refresh: object not modified", "key", key)
			} else {
				level.Error(c.Logger).Log("msg", "background fetch failed", "key", key, "err", err)
			}
			return
		}
		defer result.Body.Close()

		writer, err := c.setStreamToStore(jobCtx, c.HotStore, key, result.Metadata.ETag)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer for refresh", "key", key, "err", err)
			return
		}

		_, copyErr := io.Copy(writer, result.Body)
		closeErr := writer.Close()

		if copyErr != nil || closeErr != nil {
			level.Error(c.Logger).Log("msg", "failed to write to cache during refresh", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			level.Info(c.Logger).Log("msg", "background refresh successful", "key", key)
		}
	}

	c.Worker.Submit(job) // Submit the job to the worker's queue
	level.Debug(c.Logger).Log("msg", "submitted background refresh job", "key", key)
	return nil
}

// Close gracefully shuts down the cache's worker.
// This ensures that any pending background tasks are completed (or attempt to complete)
// before the cache is considered fully closed.
// It's important to call Close when the cache is no longer needed to prevent resource leaks.
func (c *DaramjweeCache) Close() {
	if c.Worker != nil {
		level.Info(c.Logger).Log("msg", "shutting down daramjwee cache worker")
		c.Worker.Shutdown()
		level.Info(c.Logger).Log("msg", "daramjwee cache worker shut down complete")
	}
}

// --- Internal helper methods and types ---

// newCtxWithTimeout returns a new context with a timeout if the parent context doesn't already have one.
// If the parent context already has a deadline, it returns the parent context and a no-op cancel function.
// Otherwise, it returns a new context with the DefaultTimeout and its cancel function.
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok { // Check if parent context already has a deadline
		return ctx, func() {} // Return parent context and a no-op cancel
	}
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

// getStreamFromStore retrieves a stream from the given store.
func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, *Metadata, error) {
	if store == nil {
		return nil, nil, ErrNotFound // Or a specific error indicating store is nil
	}
	return store.GetStream(ctx, key)
}

// setStreamToStore gets a writer to set a stream to the given store.
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, etag string) (io.WriteCloser, error) {
	if store == nil {
		return nil, &ConfigError{"target store is not configured"}
	}
	return store.SetWithWriter(ctx, key, etag)
}

// deleteFromStore deletes an item from the given store.
func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	if store == nil {
		return nil // Or a specific error indicating store is nil
	}
	err := store.Delete(ctx, key)
	if err == ErrNotFound { // Consider ErrNotFound as a success for delete operations
		return nil
	}
	return err
}

// statFromStore gets metadata of an item from the given store.
func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	if store == nil {
		return nil, ErrNotFound // Or a specific error indicating store is nil
	}
	return store.Stat(ctx, key)
}

// scheduleSetToStore schedules a background job to copy an item from HotStore to another store (typically ColdStore).
// This is used after a cache miss and fetch from origin, to populate the cold store.
func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set to store", "key", key)
		return
	}
	if destStore == nil {
		level.Debug(c.Logger).Log("msg", "destination store is not configured, cannot schedule set", "key", key)
		return
	}
	if c.HotStore == nil { // Source for the set is always HotStore in this context
		level.Warn(c.Logger).Log("msg", "hot store is not configured, cannot schedule set from it", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set to store", "key", key, "dest_type", storeTypeName(destStore))

		srcStream, meta, err := c.getStreamFromStore(jobCtx, c.HotStore, key) // Get from hot
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
			return
		}
		defer srcStream.Close()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, meta.ETag) // Set to destination (e.g., cold)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get writer for dest store for background set", "key", key, "err", err, "dest_type", storeTypeName(destStore))
			return
		}

		_, copyErr := io.Copy(destWriter, srcStream)
		closeErr := destWriter.Close() // This finalizes the write to destStore

		if copyErr != nil || closeErr != nil {
			level.Error(c.Logger).Log("msg", "failed background set to store", "key", key, "copyErr", copyErr, "closeErr", closeErr, "dest_type", storeTypeName(destStore))
		} else {
			level.Info(c.Logger).Log("msg", "background set to store successful", "key", key, "dest_type", storeTypeName(destStore))
		}
	}

	c.Worker.Submit(job)
	level.Debug(c.Logger).Log("msg", "submitted background set to store job", "key", key, "dest_type", storeTypeName(destStore))
}

// promoteAndTeeStream promotes an item from coldStream to the HotStore while simultaneously returning a ReadCloser for the item.
// It uses io.TeeReader to write to HotStore as the coldStream is read.
// The returned ReadCloser needs to be closed by the caller.
func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key, etag string, coldStream io.ReadCloser) (io.ReadCloser, error) {
	if c.HotStore == nil {
		level.Warn(c.Logger).Log("msg", "hot store not configured, cannot promote stream", "key", key)
		return coldStream, nil // Return original cold stream if hot store is unavailable
	}

	hotWriter, err := c.setStreamToStore(ctx, c.HotStore, key, etag)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get hot store writer for promotion", "key", key, "err", err)
		// Still return the coldStream so the caller can use the data, even if promotion fails.
		return coldStream, nil
	}

	// TeeReader reads from coldStream and writes to hotWriter.
	// newMultiCloser ensures both coldStream and hotWriter are closed when the returned ReadCloser is closed.
	teeReader := io.TeeReader(coldStream, hotWriter)
	return newMultiCloser(teeReader, coldStream, hotWriter), nil
}

// cacheAndTeeStream caches the fetched result into HotStore while returning a ReadCloser for the item.
// It's used after fetching from origin. It uses io.TeeReader to write to HotStore as the result.Body is read.
// If HotStore is not configured or if Metadata is nil, it returns the original result.Body without teeing.
// The returned ReadCloser needs to be closed by the caller.
func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if c.HotStore != nil && result.Metadata != nil {
		cacheWriter, err := c.setStreamToStore(ctx, c.HotStore, key, result.Metadata.ETag)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer for teeing origin stream", "key", key, "err", err)
			// Return the original body even if we can't set up the tee.
			return result.Body, nil
		}
		// TeeReader reads from result.Body and writes to cacheWriter.
		// newMultiCloser ensures both result.Body and cacheWriter are closed.
		teeReader := io.TeeReader(result.Body, cacheWriter)
		return newMultiCloser(teeReader, result.Body, cacheWriter), nil
	}
	// If no hot store or no metadata, return the original body directly.
	return result.Body, nil
}

// multiCloser wraps a reader and multiple closers.
// Its Close method calls Close on all underlying closers.
type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func newMultiCloser(r io.Reader, closers ...io.Closer) io.ReadCloser {
	return &multiCloser{reader: r, closers: closers}
}
func (mc *multiCloser) Read(p []byte) (n int, err error) { return mc.reader.Read(p) }
func (mc *multiCloser) Close() error {
	var firstErr error
	for _, c := range mc.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type cancelWriteCloser struct {
	io.WriteCloser      // The underlying WriteCloser.
	cancel context.CancelFunc // The context cancel function to call on Close.
}

// newCancelWriteCloser wraps an io.WriteCloser with a context cancel function.
// The cancel function is called when the returned WriteCloser's Close method is invoked.
func newCancelWriteCloser(wc io.WriteCloser, cancel context.CancelFunc) io.WriteCloser {
	return &cancelWriteCloser{WriteCloser: wc, cancel: cancel}
}

// Close closes the underlying WriteCloser and then calls the associated context cancel function.
// This is important for releasing resources tied to the context when the write operation is finished or aborted.
func (cwc *cancelWriteCloser) Close() error {
	defer cwc.cancel() // Ensure context is cancelled even if WriteCloser.Close() panics or errors.
	return cwc.WriteCloser.Close()
}

// storeTypeName is a helper to get a string representation of the store type for logging.
func storeTypeName(s Store) string {
	if s == nil {
		return "nil"
	}
	// This is a simple way; a more robust way might involve reflection or a StoreType interface method.
	// For now, we assume HotStore and ColdStore are distinct types or identifiable.
	// This part might need adjustment based on actual Store implementations.
	// As a placeholder:
	if _, ok := s.(*noopStore); ok { // Example, if you have a noopStore type
		return "noopStore"
	}
	// Add more specific types if needed.
	return "unknownStore"
}
