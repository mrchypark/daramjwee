// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
)

var ErrCacheClosed = errors.New("daramjwee: cache is closed")
var ErrNilMetadata = errors.New("daramjwee: nil metadata encountered")
var ErrBackgroundJobRejected = errors.New("daramjwee: background job rejected")

type lowerTierPromotionInvalidatedError struct {
	preserveBody bool
}

func (e lowerTierPromotionInvalidatedError) Error() string {
	return ErrTopWriteInvalidated.Error()
}

func (e lowerTierPromotionInvalidatedError) Is(target error) bool {
	return target == ErrTopWriteInvalidated
}

// DaramjweeCache is a concrete implementation of the Cache interface.
type DaramjweeCache struct {
	tiers                  []Store
	logger                 log.Logger
	runtime                backgroundRuntime
	cacheID                string
	runtimeWeight          int
	runtimeQueueLimit      int
	opTimeout              time.Duration
	closeTimeout           time.Duration
	positiveFreshness      time.Duration
	negativeFreshness      time.Duration
	tierFreshnessOverrides map[int]TierFreshnessOverride
	loggingDisabled        bool
	isClosed               atomic.Bool
	topWrites              topWriteManager
	fanoutWrites           fanoutWriteManager
	closeHook              func()
}

var _ Cache = (*DaramjweeCache)(nil)

// Get retrieves data based on the requested caching strategy.
// It checks ordered tiers from top to bottom and finally fetches from the origin.
func (c *DaramjweeCache) Get(ctx context.Context, key string, req GetRequest, fetcher Fetcher) (*GetResponse, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	if fetcher == nil {
		return nil, ErrNilFetcher
	}
	setupCtx, cancel := c.newCtxWithTimeout(ctx)
	topGenerationAtStart := c.currentTopWriteGeneration(key)

	for i, tier := range c.tiers {
		tierStream, tierMeta, err := c.getStreamFromStore(c.getStreamContextForStore(ctx, setupCtx, tier), tier, key)
		if err == nil {
			if i == 0 {
				resp, respErr := c.handleTopTierHit(ctx, key, req, fetcher, tierStream, tierMeta, cancel, topGenerationAtStart)
				if respErr != nil {
					cancel()
					return nil, respErr
				}
				return resp, nil
			}
			resp, respErr := c.handleLowerTierHit(ctx, setupCtx, key, i, req, fetcher, tierStream, tierMeta, cancel, topGenerationAtStart)
			if respErr != nil {
				cancel()
				return nil, respErr
			}
			return resp, nil
		}
		if errors.Is(err, ErrNilMetadata) {
			cancel()
			return nil, err
		}
		if !errors.Is(err, ErrNotFound) {
			c.errorLog("msg", "tier get failed", "key", key, "tier_index", i, "err", err)
		}
	}

	// 3. Fetch from Origin
	resp, respErr := c.handleMiss(ctx, setupCtx, key, req, fetcher, cancel, topGenerationAtStart)
	if respErr != nil {
		cancel()
		return nil, respErr
	}
	return resp, nil
}

// Set returns a WriteCloser to directly write data to the cache.
// The data is written to tier 0.
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	target := c.topWriteStore()
	if !hasRealStore(target) {
		return nil, &ConfigError{"no writable tier is configured"}
	}
	setupCtx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(ctx, setupCtx, target), key, metadata, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteSink(wc, cancel), nil
}

// Delete sequentially deletes an object from all tiers to prevent deadlocks.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	coord := c.topWrites.coordinator(key)
	coord.beginDelete()
	topDeleteSucceeded := false
	defer func() {
		coord.finishDelete(topDeleteSucceeded)
	}()
	var firstErr error
	top := c.topWriteStore()
	startIndex := 0
	if hasRealStore(top) {
		topErr := c.deleteFromStore(ctx, top, key)
		if topErr == nil || errors.Is(topErr, ErrNotFound) {
			topDeleteSucceeded = true
		} else {
			c.errorLog("msg", "failed to delete from tier", "key", key, "tier_index", 0, "err", topErr)
			firstErr = topErr
		}
		startIndex = 1
	}

	for i := startIndex; i < len(c.tiers); i++ {
		tier := c.tiers[i]
		if err := c.deleteFromStore(ctx, tier, key); err != nil && !errors.Is(err, ErrNotFound) {
			c.errorLog("msg", "failed to delete from tier", "key", key, "tier_index", i, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// Close safely shuts down the worker.
func (c *DaramjweeCache) Close() {
	if c.isClosed.Swap(true) {
		// Already closed, do nothing (prevent duplicate calls)
		return
	}

	if c.runtime != nil {
		c.infoLog("msg", "shutting down daramjwee cache")
		if err := c.runtime.CloseCache(c.cacheID, c.closeTimeout); err != nil {
			c.errorLog("msg", "graceful shutdown failed", "err", err)
		} else {
			c.infoLog("msg", "daramjwee cache shutdown complete")
		}
		c.runtime.RemoveCache(c.cacheID)
	}

	if hook := c.closeHook; hook != nil {
		hook()
	}
}

// cancelWriteCloser cancels the context when the WriteCloser is closed.
// safeCloser wraps an io.ReadCloser and executes a callback function upon Close.
// It automatically closes when EOF is reached and prevents duplicate closes using sync.Once.
type safeCloser struct {
	io.ReadCloser
	callback  func()
	closeOnce sync.Once
	closeErr  error
}

// newSafeCloser creates a new ReadCloser that executes a callback function
// after the underlying ReadCloser is closed, with automatic EOF detection and safe duplicate close handling.
func newSafeCloser(rc io.ReadCloser, cb func()) *safeCloser {
	return &safeCloser{
		ReadCloser: rc,
		callback:   cb,
	}
}

// Read reads from the underlying ReadCloser and automatically closes when EOF is reached.
func (c *safeCloser) Read(p []byte) (n int, err error) {
	n, err = c.ReadCloser.Read(p)
	if err == io.EOF {
		c.Close() // 자동으로 닫기
	}
	return n, err
}

// Close closes the underlying ReadCloser and executes the callback function.
// It uses sync.Once to ensure the close operation and callback are executed only once.
func (c *safeCloser) Close() error {
	c.closeOnce.Do(func() {
		defer c.callback()
		c.closeErr = c.ReadCloser.Close()
	})
	return c.closeErr
}

// ReadAll reads all data from the safeCloser and returns it as a byte slice.
// It leverages the safeCloser's automatic EOF handling and close callback execution.
// Unlike io.ReadAll, this method benefits from the automatic resource cleanup
// provided by safeCloser when EOF is reached.
func (c *safeCloser) ReadAll() ([]byte, error) {
	// Use a reasonable initial buffer size to minimize allocations
	buf := make([]byte, 0, 512)
	readBuf := make([]byte, 512)

	for {
		n, err := c.Read(readBuf)
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
		}
		if err == io.EOF {
			// safeCloser automatically closes on EOF, so we're done
			return buf, nil
		}
		if err != nil {
			// For any other error, manually close and return the error
			c.Close()
			return buf, err
		}
	}
}

// ReadAll attempts to use safeCloser.ReadAll() if possible, otherwise falls back to io.ReadAll.
// This helper function allows seamless usage regardless of the underlying ReadCloser type.
func ReadAll(rc io.ReadCloser) ([]byte, error) {
	if sc, ok := rc.(*safeCloser); ok {
		return sc.ReadAll()
	}
	return io.ReadAll(rc)
}
