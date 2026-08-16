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

// Package-level nop cancel function to avoid allocation on hot path.
var nopCancelFunc = func() {}

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

// cacheConfig holds the immutable configuration for a DaramjweeCache instance.
type cacheConfig struct {
	opTimeout              time.Duration
	closeTimeout           time.Duration
	fillLeaseTimeout       time.Duration
	positiveFreshness      time.Duration
	negativeFreshness      time.Duration
	tierFreshnessOverrides map[int]TierFreshnessOverride
	loggingDisabled        bool
}

// DaramjweeCache is a concrete implementation of the Cache interface.
type DaramjweeCache struct {
	tiers        []Store
	logger       log.Logger
	runtime      backgroundRuntime
	cacheID      string
	config       cacheConfig
	closeHook    func()
	isClosed     atomic.Bool
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
	topWrites    topWriteManager
	fanoutWrites fanoutWriteManager
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
	topGenerationAtStart := c.currentTopWriteGeneration(key)
	defer topGenerationAtStart.release()

	// Fast path: try top tier without timeout context
	if len(c.tiers) > 0 {
		topTierStream, topTierMeta, err := c.getStreamFromStore(ctx, c.tiers[0], key)
		if err == nil {
			// Top tier hit — no timeout context needed for the hot path
			resp, respErr := c.handleTopTierHit(ctx, key, req, fetcher, topTierStream, topTierMeta, nopCancelFunc, topGenerationAtStart)
			if respErr != nil {
				return nil, respErr
			}
			return resp, nil
		}
		if errors.Is(err, ErrNilMetadata) {
			return nil, err
		}
	}

	// Slow path: need timeout context for lower tiers and origin fetch
	setupCtx, cancel := c.newCtxWithTimeout(ctx)
	higherTiersClean := true

	for i, tier := range c.tiers {
		tierStream, tierMeta, err := c.getStreamFromStore(c.getStreamContextForStore(ctx, setupCtx, tier), tier, key)
		if err == nil {
			if i == 0 {
				// Already handled in fast path
				cancel()
				continue
			}
			resp, respErr := c.handleLowerTierHit(lowerTierHitParams{
				requestCtx:         ctx,
				setupCtx:           setupCtx,
				key:                key,
				tierIndex:          i,
				req:                req,
				fetcher:            fetcher,
				src:                tierStream,
				meta:               tierMeta,
				cancel:             cancel,
				expectedGeneration: topGenerationAtStart,
				higherTiersClean:   higherTiersClean,
			})
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
			higherTiersClean = false
		}
	}

	// 3. Fetch from Origin
	resp, respErr := c.handleMiss(ctx, setupCtx, key, req, fetcher, cancel, topGenerationAtStart, higherTiersClean)
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
	metadata = cloneMetadata(metadata)
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToTopStoreWithGeneration(c.beginSetContextForStore(ctx, setupCtx, target), key, metadata, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteSink(wc, cancel), nil
}

// Delete sequentially deletes an object from all tiers to prevent deadlocks.
// Deletion order is bottom-up: lower tiers are deleted before the top tier
// to prevent resurrection of deleted values through lower-tier promotion.
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
	if err := coord.beginDelete(ctx); err != nil {
		coord.releaseReference()
		return err
	}
	topDeleteSucceeded := false
	defer func() {
		coord.finishDelete(topDeleteSucceeded)
		coord.releaseReference()
	}()
	var firstErr error
	// Bottom-up deletion: delete lower tiers first, then top tier last.
	for i := len(c.tiers) - 1; i >= 0; i-- {
		tier := c.tiers[i]
		if !hasRealStore(tier) {
			continue
		}
		if err := c.deleteFromStore(ctx, tier, key); err != nil && !errors.Is(err, ErrNotFound) {
			c.errorLog("msg", "failed to delete from tier", "key", key, "tier_index", i, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		} else if i == 0 && (err == nil || errors.Is(err, ErrNotFound)) {
			topDeleteSucceeded = true
		}
	}

	return firstErr
}

// Close safely shuts down the worker.
// Multiple calls are safe: the first call performs the shutdown and subsequent
// calls block until the first shutdown completes, then return the same result.
func (c *DaramjweeCache) Close() {
	c.closeOnce.Do(func() {
		c.closeDone = make(chan struct{})
		defer close(c.closeDone)

		if !c.isClosed.Swap(true) {
			if c.runtime != nil {
				c.infoLog("msg", "shutting down daramjwee cache")
				if err := c.runtime.CloseCache(c.cacheID, c.config.closeTimeout); err != nil {
					c.errorLog("msg", "graceful shutdown failed", "err", err)
					c.closeErr = err
				} else {
					c.infoLog("msg", "daramjwee cache shutdown complete")
				}
				c.runtime.RemoveCache(c.cacheID)
			}

			if hook := c.closeHook; hook != nil {
				hook()
			}
		}
	})

	// Wait for the first Close to complete.
	if c.closeDone != nil {
		<-c.closeDone
	}
}

// closeHandler is the interface for callbacks executed when a safeCloser is closed.
type closeHandler interface {
	handle()
}

// cancelHandler is a closeHandler that calls a cancel function.
type cancelHandler struct {
	cancel func()
}

func (h cancelHandler) handle() { h.cancel() }

// safeCloser wraps an io.ReadCloser and executes a closeHandler upon Close.
// It automatically closes when EOF is reached and prevents duplicate closes using sync.Once.
type safeCloser struct {
	io.ReadCloser
	handler   closeHandler
	closeOnce sync.Once
	closeErr  error
}

// newSafeCloser creates a new ReadCloser that executes a closeHandler
// after the underlying ReadCloser is closed, with automatic EOF detection and safe duplicate close handling.
func newSafeCloser(rc io.ReadCloser, h closeHandler) *safeCloser {
	return &safeCloser{
		ReadCloser: rc,
		handler:    h,
	}
}

// Read reads from the underlying ReadCloser and automatically closes when EOF is reached.
func (c *safeCloser) Read(p []byte) (n int, err error) {
	n, err = c.ReadCloser.Read(p)
	if err == io.EOF {
		if closeErr := c.Close(); closeErr != nil {
			return n, closeErr
		}
	}
	return n, err
}

// Close closes the underlying ReadCloser and executes the closeHandler.
// It uses sync.Once to ensure the close operation and handler are executed only once.
func (c *safeCloser) Close() error {
	c.closeOnce.Do(func() {
		defer c.handler.handle()
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
	buf := make([]byte, 0, 4096)
	readBuf := make([]byte, 4096)

	for {
		n, err := c.Read(readBuf)
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
		}
		if errors.Is(err, io.EOF) {
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
