package daramjwee

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// AdaptiveBufferPoolImpl implements size-aware buffer management
type AdaptiveBufferPoolImpl struct {
	config BufferPoolConfig
	logger log.Logger

	// Size-specific pools
	smallPool  BufferPool // For objects < 32KB
	mediumPool BufferPool // For objects 32KB-256KB

	// Large object handling
	chunkPool *sync.Pool // Pool of reusable chunks for streaming

	// Metrics and monitoring
	metrics *AdaptiveBufferPoolMetrics

	// Configuration
	sizeThresholds []int
	strategies     []BufferPoolStrategy

	// Memory management
	memoryManager *MemoryManager

	// Concurrency control for large operations
	largeSemaphore chan struct{}
}

// AcquireLargeOpSlot acquires a slot for a large operation.
func (abp *AdaptiveBufferPoolImpl) AcquireLargeOpSlot() {
	abp.largeSemaphore <- struct{}{}
}

// ReleaseLargeOpSlot releases a slot for a large operation.
func (abp *AdaptiveBufferPoolImpl) ReleaseLargeOpSlot() {
	<-abp.largeSemaphore
}

// AdaptiveBufferPoolMetrics tracks detailed metrics for adaptive buffer pool
type AdaptiveBufferPoolMetrics struct {
	// Basic metrics
	totalGets     int64
	totalPuts     int64
	poolHits      int64
	poolMisses    int64
	activeBuffers int64

	// Size-category metrics
	smallObjectOps     int64
	mediumObjectOps    int64
	largeObjectOps     int64
	veryLargeObjectOps int64

	// Strategy usage metrics
	pooledOperations  int64
	chunkedOperations int64
	directOperations  int64

	// Performance metrics
	averageLatencyNs map[string]*int64
	memoryEfficiency int64 // stored as int64 for atomic operations

	// Latency tracking
	latencyMutex sync.RWMutex
	latencyData  map[string][]int64
}

// MemoryManager handles buffer allocation and lifecycle
type MemoryManager struct {
	// Memory pressure detection
	memoryPressureThreshold int64
	currentMemoryUsage      int64

	// Buffer lifecycle management
	bufferLifetime time.Duration
	maxBufferAge   time.Duration

	// Allocation strategies
	allocationStrategy AllocationStrategy
}

// AllocationStrategy defines different memory allocation approaches
type AllocationStrategy int

const (
	AllocateOnDemand AllocationStrategy = iota // Allocate when needed
	PreAllocatePool                            // Pre-allocate buffer pool
	HybridAllocation                           // Mix of on-demand and pre-allocation
)

// NewAdaptiveBufferPoolImpl creates a new adaptive buffer pool with the given configuration
func NewAdaptiveBufferPoolImpl(config BufferPoolConfig, logger log.Logger) (*AdaptiveBufferPoolImpl, error) {
	// Validate configuration
	if err := config.validateAdaptive(); err != nil {
		return nil, err
	}

	// Set default values if not configured
	if config.LargeObjectThreshold <= 0 {
		config.LargeObjectThreshold = 256 * 1024 // 256KB
	}
	if config.VeryLargeObjectThreshold <= 0 {
		config.VeryLargeObjectThreshold = 1024 * 1024 // 1MB
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = 64 * 1024 // 64KB
	}
	if config.MaxConcurrentLargeOps <= 0 {
		config.MaxConcurrentLargeOps = 10
	}

	abp := &AdaptiveBufferPoolImpl{
		config: config,
		logger: logger,
		metrics: &AdaptiveBufferPoolMetrics{
			averageLatencyNs: make(map[string]*int64),
			latencyData:      make(map[string][]int64),
		},
		memoryManager: &MemoryManager{
			memoryPressureThreshold: 1024 * 1024 * 1024, // 1GB default
			bufferLifetime:          5 * time.Minute,
			maxBufferAge:            30 * time.Minute,
			allocationStrategy:      HybridAllocation,
		},
		largeSemaphore: make(chan struct{}, config.MaxConcurrentLargeOps),
	}

	// Initialize latency tracking
	categories := []string{"small", "medium", "large", "very_large"}
	for _, category := range categories {
		abp.metrics.averageLatencyNs[category] = new(int64)
		abp.metrics.latencyData[category] = make([]int64, 0, 100)
	}

	// Create size-specific pools
	smallConfig := config
	smallConfig.MaxBufferSize = 32 * 1024 // 32KB max for small pool
	abp.smallPool = NewDefaultBufferPoolWithLogger(smallConfig, logger)

	mediumConfig := config
	mediumConfig.MinBufferSize = 32 * 1024                           // 32KB min for medium pool
	mediumConfig.MaxBufferSize = config.LargeObjectThreshold         // Up to large threshold
	mediumConfig.DefaultBufferSize = config.LargeObjectThreshold / 2 // Half of large threshold
	abp.mediumPool = NewDefaultBufferPoolWithLogger(mediumConfig, logger)

	// Initialize chunk pool for large object streaming
	abp.chunkPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, config.ChunkSize)
		},
	}

	// Set up size thresholds and strategies
	abp.sizeThresholds = []int{
		32 * 1024,                       // Small/Medium boundary
		config.LargeObjectThreshold,     // Medium/Large boundary
		config.VeryLargeObjectThreshold, // Large/VeryLarge boundary
	}

	abp.strategies = []BufferPoolStrategy{
		StrategyPooled,  // Small objects
		StrategyPooled,  // Medium objects
		StrategyChunked, // Large objects
		StrategyDirect,  // Very large objects
	}

	level.Info(logger).Log(
		"msg", "adaptive buffer pool initialized",
		"large_threshold", config.LargeObjectThreshold,
		"very_large_threshold", config.VeryLargeObjectThreshold,
		"chunk_size", config.ChunkSize,
		"max_concurrent_large_ops", config.MaxConcurrentLargeOps,
	)

	return abp, nil
}

// selectStrategy determines the appropriate strategy based on object size
func (abp *AdaptiveBufferPoolImpl) selectStrategy(size int) BufferPoolStrategy {
	// Check for memory pressure and adapt strategy
	if abp.isMemoryPressure() {
		// Under memory pressure, prefer direct streaming for large objects
		if size >= abp.config.LargeObjectThreshold {
			return StrategyDirect
		}
	}

	// Normal strategy selection based on size
	for i, threshold := range abp.sizeThresholds {
		if size < threshold {
			return abp.strategies[i]
		}
	}

	// Default to direct for very large objects
	return StrategyDirect
}

// classifySize returns the size category for metrics tracking
func (abp *AdaptiveBufferPoolImpl) classifySize(size int) string {
	if size < 32*1024 {
		return "small"
	} else if size < abp.config.LargeObjectThreshold {
		return "medium"
	} else if size < abp.config.VeryLargeObjectThreshold {
		return "large"
	}
	return "very_large"
}

// Get retrieves a buffer using the adaptive strategy
func (abp *AdaptiveBufferPoolImpl) Get(size int) []byte {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Nanoseconds()
		abp.trackLatency(abp.classifySize(size), latency)
	}()

	atomic.AddInt64(&abp.metrics.totalGets, 1)
	atomic.AddInt64(&abp.metrics.activeBuffers, 1)

	// Update size-category metrics
	abp.updateSizeCategoryMetrics(size)

	if !abp.config.Enabled {
		atomic.AddInt64(&abp.metrics.poolMisses, 1)
		return make([]byte, size)
	}

	strategy := abp.selectStrategy(size)
	abp.updateStrategyMetrics(strategy)

	level.Debug(abp.logger).Log(
		"msg", "buffer allocation",
		"size", size,
		"strategy", strategy.String(),
		"category", abp.classifySize(size),
	)

	switch strategy {
	case StrategyPooled:
		return abp.getFromPool(size)
	case StrategyChunked:
		// For chunked strategy, return a chunk-sized buffer
		return abp.GetChunk()
	case StrategyDirect:
		atomic.AddInt64(&abp.metrics.poolMisses, 1)
		return make([]byte, size)
	default:
		atomic.AddInt64(&abp.metrics.poolMisses, 1)
		return make([]byte, size)
	}
}

// getFromPool retrieves buffer from appropriate size-specific pool
func (abp *AdaptiveBufferPoolImpl) getFromPool(size int) []byte {
	if size < 32*1024 {
		atomic.AddInt64(&abp.metrics.poolHits, 1)
		return abp.smallPool.Get(size)
	} else if size < abp.config.LargeObjectThreshold {
		atomic.AddInt64(&abp.metrics.poolHits, 1)
		return abp.mediumPool.Get(size)
	}

	// Fallback to direct allocation for sizes outside pool ranges
	atomic.AddInt64(&abp.metrics.poolMisses, 1)
	return make([]byte, size)
}

// getChunk retrieves a chunk from the chunk pool
func (abp *AdaptiveBufferPoolImpl) GetChunk() []byte {
	chunk := abp.chunkPool.Get().([]byte)
	atomic.AddInt64(&abp.metrics.poolHits, 1)
	return chunk
}

// Put returns a buffer to the appropriate pool
func (abp *AdaptiveBufferPoolImpl) Put(buf []byte) {
	atomic.AddInt64(&abp.metrics.totalPuts, 1)
	atomic.AddInt64(&abp.metrics.activeBuffers, -1)

	if !abp.config.Enabled || buf == nil {
		return
	}

	bufSize := cap(buf)

	// Determine which pool this buffer belongs to
	if bufSize == abp.config.ChunkSize {
		// This is a chunk buffer
		abp.PutChunk(buf)
	} else if bufSize < 32*1024 {
		abp.smallPool.Put(buf)
	} else if bufSize < abp.config.LargeObjectThreshold {
		abp.mediumPool.Put(buf)
	}
	// Large buffers are not pooled, just let them be garbage collected
}

// putChunk returns a chunk to the chunk pool
func (abp *AdaptiveBufferPoolImpl) PutChunk(buf []byte) {
	// Reset buffer to full capacity
	buf = buf[:cap(buf)]
	abp.chunkPool.Put(buf)
}

// updateSizeCategoryMetrics updates metrics based on object size
func (abp *AdaptiveBufferPoolImpl) updateSizeCategoryMetrics(size int) {
	if size < 32*1024 {
		atomic.AddInt64(&abp.metrics.smallObjectOps, 1)
	} else if size < abp.config.LargeObjectThreshold {
		atomic.AddInt64(&abp.metrics.mediumObjectOps, 1)
	} else if size < abp.config.VeryLargeObjectThreshold {
		atomic.AddInt64(&abp.metrics.largeObjectOps, 1)
	} else {
		atomic.AddInt64(&abp.metrics.veryLargeObjectOps, 1)
	}
}

// updateStrategyMetrics updates metrics based on strategy used
func (abp *AdaptiveBufferPoolImpl) updateStrategyMetrics(strategy BufferPoolStrategy) {
	switch strategy {
	case StrategyPooled:
		atomic.AddInt64(&abp.metrics.pooledOperations, 1)
	case StrategyChunked:
		atomic.AddInt64(&abp.metrics.chunkedOperations, 1)
	case StrategyDirect:
		atomic.AddInt64(&abp.metrics.directOperations, 1)
	}
}

// trackLatency tracks latency for performance analysis
func (abp *AdaptiveBufferPoolImpl) trackLatency(category string, latency int64) {
	abp.metrics.latencyMutex.Lock()
	defer abp.metrics.latencyMutex.Unlock()

	// Add to latency data (keep last 100 samples)
	data := abp.metrics.latencyData[category]
	if len(data) >= 100 {
		data = data[1:] // Remove oldest sample
	}
	data = append(data, latency)
	abp.metrics.latencyData[category] = data

	// Calculate and update average
	if len(data) > 0 {
		var sum int64
		for _, l := range data {
			sum += l
		}
		avg := sum / int64(len(data))
		atomic.StoreInt64(abp.metrics.averageLatencyNs[category], avg)
	}
}

// isMemoryPressure checks if system is under memory pressure
func (abp *AdaptiveBufferPoolImpl) isMemoryPressure() bool {
	currentUsage := atomic.LoadInt64(&abp.memoryManager.currentMemoryUsage)
	return currentUsage > abp.memoryManager.memoryPressureThreshold
}

// CopyBuffer performs optimized copy using adaptive strategy
func (abp *AdaptiveBufferPoolImpl) CopyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	if !abp.config.Enabled {
		return io.Copy(dst, src)
	}

	// Try to determine size if possible (for strategy selection)
	// For now, use default strategy based on chunk size
	strategy := StrategyChunked // Default to chunked for copy operations

	switch strategy {
	case StrategyChunked:
		return abp.chunkedCopyBuffer(dst, src)
	case StrategyPooled:
		return abp.pooledCopyBuffer(dst, src)
	default:
		return io.Copy(dst, src)
	}
}

// chunkedCopyBuffer implements optimized copying for large objects
func (abp *AdaptiveBufferPoolImpl) chunkedCopyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	chunk := abp.GetChunk()
	defer abp.PutChunk(chunk)

	return io.CopyBuffer(dst, src, chunk)
}

// pooledCopyBuffer implements standard pooled copying
func (abp *AdaptiveBufferPoolImpl) pooledCopyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	buf := abp.Get(abp.config.DefaultBufferSize)
	defer abp.Put(buf)

	return io.CopyBuffer(dst, src, buf)
}

// TeeReader creates an optimized TeeReader using adaptive strategy
func (abp *AdaptiveBufferPoolImpl) TeeReader(r io.Reader, w io.Writer) io.Reader {
	if !abp.config.Enabled {
		return io.TeeReader(r, w)
	}

	return &adaptiveTeeReaderImpl{
		r:          r,
		w:          w,
		bufferPool: abp,
	}
}

// adaptiveTeeReaderImpl implements io.Reader using adaptive buffer management
type adaptiveTeeReaderImpl struct {
	r          io.Reader
	w          io.Writer
	bufferPool *AdaptiveBufferPoolImpl
}

// Read implements io.Reader interface with adaptive buffer management
func (t *adaptiveTeeReaderImpl) Read(p []byte) (n int, err error) {
	n, err = t.r.Read(p)
	if n > 0 {
		// Write the data to the tee writer
		if _, writeErr := t.w.Write(p[:n]); writeErr != nil {
			return n, writeErr
		}
	}
	return n, err
}

// ChunkedTeeReaderImpl implements optimized streaming for large objects
type ChunkedTeeReaderImpl struct {
	reader     io.Reader
	writer     io.Writer
	chunkPool  *sync.Pool
	chunkSize  int
	bufferPool *AdaptiveBufferPoolImpl

	// Performance tracking
	bytesRead  int64
	chunksUsed int
	startTime  time.Time
}

// NewChunkedTeeReaderImpl creates a new chunked tee reader
func (abp *AdaptiveBufferPoolImpl) NewChunkedTeeReaderImpl(r io.Reader, w io.Writer) *ChunkedTeeReaderImpl {
	return &ChunkedTeeReaderImpl{
		reader:     r,
		writer:     w,
		chunkPool:  abp.chunkPool,
		chunkSize:  abp.config.ChunkSize,
		bufferPool: abp,
		startTime:  time.Now(),
	}
}

// Read implements optimized chunked reading for large objects
func (ctr *ChunkedTeeReaderImpl) Read(p []byte) (n int, err error) {
	n, err = ctr.reader.Read(p)
	if n > 0 {
		atomic.AddInt64(&ctr.bytesRead, int64(n))
		ctr.chunksUsed++

		// Write to tee destination
		if _, writeErr := ctr.writer.Write(p[:n]); writeErr != nil {
			return n, writeErr
		}
	}
	return n, err
}

// ChunkedCopyBuffer implements optimized copying for large objects with size awareness
func (abp *AdaptiveBufferPoolImpl) ChunkedCopyBuffer(dst io.Writer, src io.Reader, size int) (int64, error) {
	// Acquire semaphore for large operations if needed
	if size >= abp.config.LargeObjectThreshold {
		select {
		case abp.largeSemaphore <- struct{}{}:
			defer func() { <-abp.largeSemaphore }()
		default:
			// Semaphore full, fall back to direct copy
			level.Debug(abp.logger).Log(
				"msg", "large operation semaphore full, using direct copy",
				"size", size,
			)
			return io.Copy(dst, src)
		}
	}

	// Determine optimal chunk size based on object size
	chunkSize := abp.calculateOptimalChunkSize(size)

	// Get chunk from pool or create new one
	var chunk []byte
	if chunkSize == abp.config.ChunkSize {
		chunk = abp.GetChunk()
		defer abp.PutChunk(chunk)
	} else {
		chunk = make([]byte, chunkSize)
	}

	// Perform chunked copy with performance tracking
	startTime := time.Now()
	copied, err := io.CopyBuffer(dst, src, chunk)

	// Track performance metrics
	latency := time.Since(startTime).Nanoseconds()
	category := abp.classifySize(size)
	abp.trackLatency(category, latency)

	level.Debug(abp.logger).Log(
		"msg", "chunked copy completed",
		"size", size,
		"copied", copied,
		"chunk_size", chunkSize,
		"latency_ns", latency,
		"category", category,
	)

	return copied, err
}

// calculateOptimalChunkSize determines the best chunk size for a given object size
func (abp *AdaptiveBufferPoolImpl) calculateOptimalChunkSize(size int) int {
	// For small objects, use smaller chunks
	if size < 64*1024 {
		return 16 * 1024 // 16KB
	}

	// For medium objects, use default chunk size
	if size < abp.config.LargeObjectThreshold {
		return abp.config.ChunkSize
	}

	// For large objects, use larger chunks for efficiency
	if size < abp.config.VeryLargeObjectThreshold {
		return abp.config.ChunkSize * 2 // Double chunk size
	}

	// For very large objects, use even larger chunks
	return abp.config.ChunkSize * 4 // Quadruple chunk size
}

// GetStats returns comprehensive buffer pool statistics
func (abp *AdaptiveBufferPoolImpl) GetStats() BufferPoolStats {
	// Calculate memory efficiency
	memoryEfficiency := abp.calculateMemoryEfficiency()

	// Build average latency map
	avgLatency := make(map[string]int64)
	for category, latencyPtr := range abp.metrics.averageLatencyNs {
		avgLatency[category] = atomic.LoadInt64(latencyPtr)
	}

	return BufferPoolStats{
		// Basic metrics (combined from all pools)
		TotalGets:     atomic.LoadInt64(&abp.metrics.totalGets),
		TotalPuts:     atomic.LoadInt64(&abp.metrics.totalPuts),
		PoolHits:      atomic.LoadInt64(&abp.metrics.poolHits),
		PoolMisses:    atomic.LoadInt64(&abp.metrics.poolMisses),
		ActiveBuffers: atomic.LoadInt64(&abp.metrics.activeBuffers),

		// Size-category metrics
		SmallObjectOps:     atomic.LoadInt64(&abp.metrics.smallObjectOps),
		MediumObjectOps:    atomic.LoadInt64(&abp.metrics.mediumObjectOps),
		LargeObjectOps:     atomic.LoadInt64(&abp.metrics.largeObjectOps),
		VeryLargeObjectOps: atomic.LoadInt64(&abp.metrics.veryLargeObjectOps),

		// Strategy usage metrics
		PooledOperations:  atomic.LoadInt64(&abp.metrics.pooledOperations),
		ChunkedOperations: atomic.LoadInt64(&abp.metrics.chunkedOperations),
		DirectOperations:  atomic.LoadInt64(&abp.metrics.directOperations),

		// Performance metrics
		AverageLatencyNs: avgLatency,
		MemoryEfficiency: memoryEfficiency,
	}
}

// calculateMemoryEfficiency calculates memory usage efficiency
func (abp *AdaptiveBufferPoolImpl) calculateMemoryEfficiency() float64 {
	// This is a simplified calculation
	// In a real implementation, you would track actual memory usage
	totalOps := atomic.LoadInt64(&abp.metrics.totalGets)
	poolHits := atomic.LoadInt64(&abp.metrics.poolHits)

	if totalOps == 0 {
		return 1.0
	}

	// Efficiency based on pool hit rate, capped at 1.0
	hitRate := float64(poolHits) / float64(totalOps)
	if hitRate > 1.0 {
		hitRate = 1.0
	}
	return hitRate
}

// Close gracefully shuts down the adaptive buffer pool
func (abp *AdaptiveBufferPoolImpl) Close() {
	level.Info(abp.logger).Log("msg", "shutting down adaptive buffer pool")

	// Close underlying pools if they support it
	if closer, ok := abp.smallPool.(interface{ Close() }); ok {
		closer.Close()
	}
	if closer, ok := abp.mediumPool.(interface{ Close() }); ok {
		closer.Close()
	}

	level.Info(abp.logger).Log("msg", "adaptive buffer pool shutdown complete")
}

// validateAdaptive validates the adaptive buffer pool configuration
func (config *BufferPoolConfig) validateAdaptive() error {
	// Basic validation
	if err := config.validate(); err != nil {
		return err
	}

	// Adaptive-specific validation
	if config.LargeObjectThreshold > 0 && config.LargeObjectThreshold <= config.MaxBufferSize {
		return &ConfigError{"large object threshold must be larger than max buffer size"}
	}

	if config.VeryLargeObjectThreshold > 0 && config.VeryLargeObjectThreshold <= config.LargeObjectThreshold {
		return &ConfigError{"very large object threshold must be larger than large object threshold"}
	}

	if config.ChunkSize > 0 && config.ChunkSize <= 0 {
		return &ConfigError{"chunk size must be positive"}
	}

	if config.MaxConcurrentLargeOps > 0 && config.MaxConcurrentLargeOps <= 0 {
		return &ConfigError{"max concurrent large operations must be positive"}
	}

	return nil
}
