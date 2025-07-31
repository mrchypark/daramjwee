package daramjwee

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// DefaultBufferPool implements BufferPool using sync.Pool for efficient buffer reuse.
// It maintains separate pools for different buffer sizes to optimize memory usage.
type DefaultBufferPool struct {
	config BufferPoolConfig
	pools  map[int]*sync.Pool
	mutex  sync.RWMutex

	// Statistics counters
	totalGets     int64
	totalPuts     int64
	poolHits      int64
	poolMisses    int64
	activeBuffers int64

	// Pool buffer counters for each size
	poolBufferCounts map[int]*int64

	// Logging related fields
	logger     log.Logger
	logTicker  *time.Ticker
	logCancel  context.CancelFunc
	logContext context.Context
}

// NewDefaultBufferPoolWithLogger creates a new DefaultBufferPool with the given configuration and logger.
func NewDefaultBufferPoolWithLogger(config BufferPoolConfig, logger log.Logger) *DefaultBufferPool {
	if config.DefaultBufferSize <= 0 {
		config.DefaultBufferSize = 32 * 1024 // 32KB default
	}
	if config.MaxBufferSize <= 0 {
		config.MaxBufferSize = 1024 * 1024 // 1MB default
	}
	if config.MinBufferSize <= 0 {
		config.MinBufferSize = 1024 // 1KB default
	}

	bp := &DefaultBufferPool{
		config:           config,
		pools:            make(map[int]*sync.Pool),
		poolBufferCounts: make(map[int]*int64),
		logger:           logger,
	}

	// Initialize logging if enabled
	if config.EnableLogging && config.LoggingInterval > 0 {
		bp.startPeriodicLogging()
	}

	// Pre-create pools for common buffer sizes
	commonSizes := []int{
		4 * 1024,   // 4KB
		16 * 1024,  // 16KB
		32 * 1024,  // 32KB (default)
		64 * 1024,  // 64KB
		128 * 1024, // 128KB
		256 * 1024, // 256KB
		512 * 1024, // 512KB
	}

	for _, size := range commonSizes {
		if size >= config.MinBufferSize && size <= config.MaxBufferSize {
			bp.getOrCreatePool(size)
		}
	}

	return bp
}

// Get retrieves a buffer from the pool with at least the specified size.
func (bp *DefaultBufferPool) Get(size int) []byte {
	atomic.AddInt64(&bp.totalGets, 1)
	atomic.AddInt64(&bp.activeBuffers, 1)

	if !bp.config.Enabled {
		atomic.AddInt64(&bp.poolMisses, 1)
		return make([]byte, size)
	}

	// Find the appropriate pool size (next power of 2 or common size)
	poolSize := bp.selectPoolSize(size)

	// Check if we have buffers available in the pool
	bp.mutex.RLock()
	counter, hasCounter := bp.poolBufferCounts[poolSize]
	bp.mutex.RUnlock()

	pool := bp.getOrCreatePool(poolSize)
	buf := pool.Get().([]byte)

	// Determine if this was a hit or miss based on available buffers
	if hasCounter && atomic.LoadInt64(counter) > 0 {
		atomic.AddInt64(&bp.poolHits, 1)
		atomic.AddInt64(counter, -1) // Decrement available buffer count
	} else {
		atomic.AddInt64(&bp.poolMisses, 1)
	}

	// Return buffer sliced to requested size
	return buf[:size]
}

// Put returns a buffer to the appropriate pool for reuse.
func (bp *DefaultBufferPool) Put(buf []byte) {
	atomic.AddInt64(&bp.totalPuts, 1)
	atomic.AddInt64(&bp.activeBuffers, -1)

	if !bp.config.Enabled || buf == nil {
		return
	}

	bufSize := cap(buf)

	// Don't pool buffers that are too small or too large
	if bufSize < bp.config.MinBufferSize || bufSize > bp.config.MaxBufferSize {
		return
	}

	// Check if we have a pool for this exact buffer size
	bp.mutex.RLock()
	pool, exists := bp.pools[bufSize]
	counter, hasCounter := bp.poolBufferCounts[bufSize]
	bp.mutex.RUnlock()

	if !exists {
		// No pool for this size, don't pool it
		return
	}

	// Reset buffer slice to full capacity before returning to pool
	buf = buf[:cap(buf)]
	pool.Put(buf)

	// Increment available buffer count
	if hasCounter {
		atomic.AddInt64(counter, 1)
	}
}

// selectPoolSize determines the appropriate pool size for a given buffer size.
// It uses common sizes and powers of 2 for efficient memory usage.
func (bp *DefaultBufferPool) selectPoolSize(size int) int {
	// Common sizes in ascending order
	commonSizes := []int{
		4 * 1024,    // 4KB
		16 * 1024,   // 16KB
		32 * 1024,   // 32KB
		64 * 1024,   // 64KB
		128 * 1024,  // 128KB
		256 * 1024,  // 256KB
		512 * 1024,  // 512KB
		1024 * 1024, // 1MB
	}

	// Find the smallest common size that fits
	for _, commonSize := range commonSizes {
		if size <= commonSize && commonSize >= bp.config.MinBufferSize && commonSize <= bp.config.MaxBufferSize {
			return commonSize
		}
	}

	// If no common size fits, use the default size if it's large enough
	if size <= bp.config.DefaultBufferSize {
		return bp.config.DefaultBufferSize
	}

	// For very large sizes, don't pool them
	return size
}

// getOrCreatePool retrieves or creates a sync.Pool for the given size.
func (bp *DefaultBufferPool) getOrCreatePool(size int) *sync.Pool {
	bp.mutex.RLock()
	if pool, exists := bp.pools[size]; exists {
		bp.mutex.RUnlock()
		return pool
	}
	bp.mutex.RUnlock()

	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// Double-check after acquiring write lock
	if pool, exists := bp.pools[size]; exists {
		return pool
	}

	// Create new pool and counter
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, size)
		},
	}
	bp.pools[size] = pool

	// Initialize counter for this pool size
	counter := int64(0)
	bp.poolBufferCounts[size] = &counter

	return pool
}

// CopyBuffer performs optimized copy using pooled buffers.
// It falls back to standard io.Copy if buffer pool is disabled or fails.
func (bp *DefaultBufferPool) CopyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	if !bp.config.Enabled {
		return io.Copy(dst, src)
	}

	buf := bp.Get(bp.config.DefaultBufferSize)
	defer bp.Put(buf)

	return io.CopyBuffer(dst, src, buf)
}

// TeeReader creates an optimized TeeReader using pooled buffers.
// It falls back to standard io.TeeReader if buffer pool is disabled.
func (bp *DefaultBufferPool) TeeReader(r io.Reader, w io.Writer) io.Reader {
	if !bp.config.Enabled {
		return io.TeeReader(r, w)
	}

	return &pooledTeeReader{
		r:          r,
		w:          w,
		bufferPool: bp,
	}
}

// GetStats returns current buffer pool statistics.
func (bp *DefaultBufferPool) GetStats() BufferPoolStats {
	return BufferPoolStats{
		TotalGets:     atomic.LoadInt64(&bp.totalGets),
		TotalPuts:     atomic.LoadInt64(&bp.totalPuts),
		PoolHits:      atomic.LoadInt64(&bp.poolHits),
		PoolMisses:    atomic.LoadInt64(&bp.poolMisses),
		ActiveBuffers: atomic.LoadInt64(&bp.activeBuffers),
	}
}

// pooledTeeReader implements io.Reader using pooled buffers for tee operations.
type pooledTeeReader struct {
	r          io.Reader
	w          io.Writer
	bufferPool *DefaultBufferPool
}

// Read implements io.Reader interface with optimized buffer management.
func (t *pooledTeeReader) Read(p []byte) (n int, err error) {
	n, err = t.r.Read(p)
	if n > 0 {
		// Write the data to the tee writer
		// We don't need to handle partial writes here as io.TeeReader doesn't either
		if _, writeErr := t.w.Write(p[:n]); writeErr != nil {
			// If write fails, return the write error
			return n, writeErr
		}
	}
	return n, err
}

// startPeriodicLogging starts a goroutine that periodically logs buffer pool statistics.
func (bp *DefaultBufferPool) startPeriodicLogging() {
	if bp.logger == nil || bp.config.LoggingInterval <= 0 {
		return
	}

	bp.logContext, bp.logCancel = context.WithCancel(context.Background())
	bp.logTicker = time.NewTicker(bp.config.LoggingInterval)

	go func() {
		defer bp.logTicker.Stop()
		for {
			select {
			case <-bp.logContext.Done():
				return
			case <-bp.logTicker.C:
				bp.logStats()
			}
		}
	}()
}

// stopPeriodicLogging stops the periodic logging goroutine.
func (bp *DefaultBufferPool) stopPeriodicLogging() {
	if bp.logCancel != nil {
		bp.logCancel()
	}
	if bp.logTicker != nil {
		bp.logTicker.Stop()
	}
}

// logStats logs current buffer pool statistics.
func (bp *DefaultBufferPool) logStats() {
	if bp.logger == nil {
		return
	}

	stats := bp.GetStats()

	// Calculate hit rate
	var hitRate float64
	if stats.TotalGets > 0 {
		hitRate = float64(stats.PoolHits) / float64(stats.TotalGets) * 100
	}

	level.Info(bp.logger).Log(
		"msg", "buffer pool statistics",
		"total_gets", stats.TotalGets,
		"total_puts", stats.TotalPuts,
		"pool_hits", stats.PoolHits,
		"pool_misses", stats.PoolMisses,
		"active_buffers", stats.ActiveBuffers,
		"hit_rate_percent", hitRate,
	)
}

// Close stops periodic logging and cleans up resources.
func (bp *DefaultBufferPool) Close() {
	bp.stopPeriodicLogging()
}

// NewDefaultBufferPool creates a new DefaultBufferPool with the given configuration.
// This is a backward compatibility function that creates a buffer pool without logging.
func NewDefaultBufferPool(config BufferPoolConfig) *DefaultBufferPool {
	return NewDefaultBufferPoolWithLogger(config, nil)
}
