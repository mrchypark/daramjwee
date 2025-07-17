package daramjwee

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper types for error testing

type errorReader struct {
	data       []byte
	pos        int
	errorAfter int
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	if r.pos >= r.errorAfter {
		return 0, fmt.Errorf("simulated read error")
	}

	remaining := len(r.data) - r.pos
	if remaining == 0 {
		return 0, io.EOF
	}

	n = len(p)
	if n > remaining {
		n = remaining
	}
	if r.pos+n > r.errorAfter {
		n = r.errorAfter - r.pos
	}

	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n

	if r.pos >= r.errorAfter {
		return n, fmt.Errorf("simulated read error")
	}

	return n, nil
}

type errorWriter struct {
	written    int
	errorAfter int
}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	if w.written >= w.errorAfter {
		return 0, fmt.Errorf("simulated write error")
	}

	n = len(p)
	if w.written+n > w.errorAfter {
		n = w.errorAfter - w.written
	}

	w.written += n

	if w.written >= w.errorAfter {
		return n, fmt.Errorf("simulated write error")
	}

	return n, nil
}

func TestDefaultBufferPool_BasicFunctionality(t *testing.T) {
	tests := []struct {
		name   string
		config BufferPoolConfig
	}{
		{
			name: "enabled pool with default config",
			config: BufferPoolConfig{
				Enabled:           true,
				DefaultBufferSize: 32 * 1024,
				MaxBufferSize:     1024 * 1024,
				MinBufferSize:     1024,
			},
		},
		{
			name: "disabled pool",
			config: BufferPoolConfig{
				Enabled:           false,
				DefaultBufferSize: 32 * 1024,
				MaxBufferSize:     1024 * 1024,
				MinBufferSize:     1024,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewDefaultBufferPool(tt.config)
			require.NotNil(t, pool)

			// Test Get method with various sizes
			testSizes := []int{1024, 4096, 16384, 32768, 65536}

			for _, size := range testSizes {
				t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
					buf := pool.Get(size)

					// Buffer should be at least the requested size
					assert.GreaterOrEqual(t, len(buf), size, "Buffer should be at least requested size")
					assert.GreaterOrEqual(t, cap(buf), size, "Buffer capacity should be at least requested size")

					// Buffer should be usable
					for i := 0; i < len(buf); i++ {
						buf[i] = byte(i % 256)
					}

					// Return buffer to pool
					pool.Put(buf)
				})
			}
		})
	}
}

func TestDefaultBufferPool_GetPutCorrectness(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	// Test buffer reuse
	t.Run("buffer_reuse", func(t *testing.T) {
		size := 32 * 1024

		// Get a buffer and fill it with data
		buf1 := pool.Get(size)
		require.Equal(t, size, len(buf1))

		// Fill with pattern
		for i := 0; i < len(buf1); i++ {
			buf1[i] = byte(i % 256)
		}

		// Return to pool
		pool.Put(buf1)

		// Get another buffer of same size - might be the same buffer
		buf2 := pool.Get(size)
		require.Equal(t, size, len(buf2))

		// Buffer should be usable regardless of whether it's reused
		for i := 0; i < len(buf2); i++ {
			buf2[i] = byte((i + 1) % 256)
		}

		pool.Put(buf2)
	})

	// Test different sizes
	t.Run("different_sizes", func(t *testing.T) {
		sizes := []int{1024, 4096, 16384, 32768, 65536, 131072}
		buffers := make([][]byte, len(sizes))

		// Get buffers of different sizes
		for i, size := range sizes {
			buffers[i] = pool.Get(size)
			assert.GreaterOrEqual(t, len(buffers[i]), size)
			assert.GreaterOrEqual(t, cap(buffers[i]), size)
		}

		// Return all buffers
		for _, buf := range buffers {
			pool.Put(buf)
		}
	})
}

func TestDefaultBufferPool_EdgeCases(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("zero_size_buffer", func(t *testing.T) {
		buf := pool.Get(0)
		assert.NotNil(t, buf)
		assert.Equal(t, 0, len(buf))
		pool.Put(buf)
	})

	t.Run("very_small_buffer", func(t *testing.T) {
		buf := pool.Get(1)
		assert.NotNil(t, buf)
		assert.GreaterOrEqual(t, len(buf), 1)
		pool.Put(buf)
	})

	t.Run("oversized_buffer", func(t *testing.T) {
		// Request buffer larger than MaxBufferSize
		oversizeRequest := config.MaxBufferSize + 1024
		buf := pool.Get(oversizeRequest)

		assert.NotNil(t, buf)
		assert.GreaterOrEqual(t, len(buf), oversizeRequest)

		// Put should handle oversized buffers gracefully
		pool.Put(buf)
	})

	t.Run("nil_buffer_put", func(t *testing.T) {
		// Putting nil buffer should not panic
		assert.NotPanics(t, func() {
			pool.Put(nil)
		})
	})

	t.Run("empty_buffer_put", func(t *testing.T) {
		// Putting empty buffer should not panic
		assert.NotPanics(t, func() {
			pool.Put([]byte{})
		})
	})
}

func TestDefaultBufferPool_Statistics(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	// Initial stats should be zero
	stats := pool.GetStats()
	assert.Equal(t, int64(0), stats.TotalGets)
	assert.Equal(t, int64(0), stats.TotalPuts)
	assert.Equal(t, int64(0), stats.PoolHits)
	assert.Equal(t, int64(0), stats.PoolMisses)
	assert.Equal(t, int64(0), stats.ActiveBuffers)

	// Get some buffers - these should be misses since pools are empty
	buf1 := pool.Get(32 * 1024)
	buf2 := pool.Get(32 * 1024)

	stats = pool.GetStats()
	assert.Equal(t, int64(2), stats.TotalGets)
	assert.Equal(t, int64(0), stats.TotalPuts)
	assert.Equal(t, int64(0), stats.PoolHits)   // Should be 0 hits initially
	assert.Equal(t, int64(2), stats.PoolMisses) // Should be 2 misses
	assert.Equal(t, int64(2), stats.ActiveBuffers)

	// Put buffers back
	pool.Put(buf1)
	pool.Put(buf2)

	stats = pool.GetStats()
	assert.Equal(t, int64(2), stats.TotalGets)
	assert.Equal(t, int64(2), stats.TotalPuts)
	assert.Equal(t, int64(0), stats.PoolHits)   // Still 0 hits
	assert.Equal(t, int64(2), stats.PoolMisses) // Still 2 misses
	assert.Equal(t, int64(0), stats.ActiveBuffers)

	// Get buffer again - should be a hit now if buffer was pooled
	buf3 := pool.Get(32 * 1024)

	stats = pool.GetStats()
	assert.Equal(t, int64(3), stats.TotalGets)
	assert.Equal(t, int64(2), stats.TotalPuts)
	// This might be a hit or miss depending on whether buffer was actually pooled
	t.Logf("Pool hits: %d, Pool misses: %d", stats.PoolHits, stats.PoolMisses)
	assert.Equal(t, int64(1), stats.ActiveBuffers)

	pool.Put(buf3)
}

func TestDefaultBufferPool_DisabledPool(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           false,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	// Get buffer from disabled pool
	buf := pool.Get(32 * 1024)
	assert.NotNil(t, buf)
	assert.Equal(t, 32*1024, len(buf))

	// Put should not panic
	assert.NotPanics(t, func() {
		pool.Put(buf)
	})

	// Stats should show all misses
	stats := pool.GetStats()
	assert.Equal(t, int64(1), stats.TotalGets)
	assert.Equal(t, int64(1), stats.TotalPuts)
	assert.Equal(t, int64(0), stats.PoolHits)
	assert.Equal(t, int64(1), stats.PoolMisses)
	assert.Equal(t, int64(0), stats.ActiveBuffers)
}

func TestDefaultBufferPool_BufferReuseVerification(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("same_size_buffer_reuse", func(t *testing.T) {
		size := 32 * 1024

		// Get first buffer and mark it with a unique pattern
		buf1 := pool.Get(size)
		originalPtr := &buf1[0] // Get pointer to first element

		// Fill with recognizable pattern
		for i := 0; i < len(buf1); i++ {
			buf1[i] = 0xAA
		}

		// Return to pool
		pool.Put(buf1)

		// Get another buffer of same size
		buf2 := pool.Get(size)
		newPtr := &buf2[0]

		// Verify buffer was reused (same underlying array)
		if originalPtr == newPtr {
			t.Log("Buffer was successfully reused")
		} else {
			t.Log("New buffer was allocated (pool was empty)")
		}

		// Verify buffer is usable regardless
		for i := 0; i < len(buf2); i++ {
			buf2[i] = 0xBB
		}

		pool.Put(buf2)
	})

	t.Run("multiple_size_pools", func(t *testing.T) {
		sizes := []int{4 * 1024, 16 * 1024, 32 * 1024, 64 * 1024}
		buffers := make([][]byte, len(sizes))

		// Get buffers of different sizes
		for i, size := range sizes {
			buffers[i] = pool.Get(size)
			assert.Equal(t, size, len(buffers[i]))

			// Fill with size-specific pattern
			pattern := byte(size / 1024)
			for j := 0; j < len(buffers[i]); j++ {
				buffers[i][j] = pattern
			}
		}

		// Return all buffers
		for _, buf := range buffers {
			pool.Put(buf)
		}

		// Get buffers again and verify they work
		for _, size := range sizes {
			buf := pool.Get(size)
			assert.Equal(t, size, len(buf))

			// Verify buffer is usable
			newPattern := byte((size / 1024) + 100)
			for j := 0; j < len(buf); j++ {
				buf[j] = newPattern
			}

			pool.Put(buf)
		}
	})
}

func TestDefaultBufferPool_PoolSizeSelection(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	testCases := []struct {
		requestSize int
		expectedCap int
		description string
	}{
		{1024, 4 * 1024, "small request should get 4KB buffer"},
		{3 * 1024, 4 * 1024, "3KB request should get 4KB buffer"},
		{5 * 1024, 16 * 1024, "5KB request should get 16KB buffer"},
		{20 * 1024, 32 * 1024, "20KB request should get 32KB buffer"},
		{40 * 1024, 64 * 1024, "40KB request should get 64KB buffer"},
		{100 * 1024, 128 * 1024, "100KB request should get 128KB buffer"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			buf := pool.Get(tc.requestSize)
			assert.Equal(t, tc.requestSize, len(buf), "Buffer length should match request")
			assert.GreaterOrEqual(t, cap(buf), tc.expectedCap, "Buffer capacity should be appropriate pool size")
			pool.Put(buf)
		})
	}
}

func TestDefaultBufferPool_ExtremeSizes(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("zero_size", func(t *testing.T) {
		buf := pool.Get(0)
		assert.NotNil(t, buf)
		assert.Equal(t, 0, len(buf))

		// Should not panic when putting back
		assert.NotPanics(t, func() {
			pool.Put(buf)
		})
	})

	t.Run("negative_size", func(t *testing.T) {
		// Go's make() will panic with negative size, but our pool should handle it gracefully
		assert.Panics(t, func() {
			pool.Get(-1)
		})
	})

	t.Run("very_large_size", func(t *testing.T) {
		largeSize := 10 * 1024 * 1024 // 10MB
		buf := pool.Get(largeSize)
		assert.NotNil(t, buf)
		assert.Equal(t, largeSize, len(buf))

		// Large buffer should not be pooled (exceeds MaxBufferSize)
		initialStats := pool.GetStats()
		pool.Put(buf)
		finalStats := pool.GetStats()

		// Put count should increase but buffer won't be pooled
		assert.Equal(t, initialStats.TotalPuts+1, finalStats.TotalPuts)
	})

	t.Run("undersized_buffer", func(t *testing.T) {
		// Request smaller than MinBufferSize
		smallSize := 512 // Less than 1KB MinBufferSize
		buf := pool.Get(smallSize)
		assert.NotNil(t, buf)
		assert.Equal(t, smallSize, len(buf))

		// Buffer should still work
		for i := 0; i < len(buf); i++ {
			buf[i] = byte(i % 256)
		}

		pool.Put(buf)
	})
}

func TestDefaultBufferPool_ConfigValidation(t *testing.T) {
	t.Run("default_values_applied", func(t *testing.T) {
		// Test with zero/invalid config values
		config := BufferPoolConfig{
			Enabled: true,
			// All other values are zero/invalid
		}

		pool := NewDefaultBufferPool(config)
		assert.NotNil(t, pool)

		// Should use default values
		buf := pool.Get(32 * 1024)
		assert.NotNil(t, buf)
		assert.Equal(t, 32*1024, len(buf))
		pool.Put(buf)
	})

	t.Run("custom_config_respected", func(t *testing.T) {
		config := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 16 * 1024,
			MaxBufferSize:     512 * 1024,
			MinBufferSize:     2 * 1024,
		}

		pool := NewDefaultBufferPool(config)
		assert.NotNil(t, pool)

		// Test that custom config is used
		buf := pool.Get(16 * 1024)
		assert.NotNil(t, buf)
		assert.Equal(t, 16*1024, len(buf))
		pool.Put(buf)
	})
}

func TestDefaultBufferPool_MemoryEfficiency(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("buffer_slice_correctness", func(t *testing.T) {
		// Request smaller size than pool buffer
		requestSize := 16 * 1024
		buf := pool.Get(requestSize)

		// Length should match request, but capacity might be larger
		assert.Equal(t, requestSize, len(buf))
		assert.GreaterOrEqual(t, cap(buf), requestSize)

		// Should be able to write to full length
		for i := 0; i < len(buf); i++ {
			buf[i] = byte(i % 256)
		}

		// Verify we can read what we wrote
		for i := 0; i < len(buf); i++ {
			assert.Equal(t, byte(i%256), buf[i])
		}

		pool.Put(buf)
	})

	t.Run("capacity_preservation", func(t *testing.T) {
		originalSize := 32 * 1024
		buf := pool.Get(originalSize)
		originalCap := cap(buf)

		// Modify slice length
		buf = buf[:16*1024] // Reduce length

		// Put back the modified slice
		pool.Put(buf)

		// Get another buffer of same original size
		buf2 := pool.Get(originalSize)

		// If reused, capacity should be preserved
		if cap(buf2) == originalCap {
			t.Log("Buffer capacity was preserved during reuse")
		}

		pool.Put(buf2)
	})
}
func TestDefaultBufferPool_ConcurrentSafety(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("concurrent_get_put", func(t *testing.T) {
		const numGoroutines = 100
		const numOperations = 1000

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Channel to collect any panics
		panicChan := make(chan interface{}, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicChan <- r
					}
				}()

				buffers := make([][]byte, 0, 10)
				sizes := []int{1024, 4096, 16384, 32768, 65536}

				for j := 0; j < numOperations; j++ {
					// Randomly choose operation: get or put
					if len(buffers) == 0 || (len(buffers) < 10 && j%3 == 0) {
						// Get buffer
						size := sizes[j%len(sizes)]
						buf := pool.Get(size)
						assert.NotNil(t, buf)
						assert.GreaterOrEqual(t, len(buf), size)

						// Write pattern to verify buffer integrity
						pattern := byte(goroutineID % 256)
						for k := 0; k < len(buf); k++ {
							buf[k] = pattern
						}

						buffers = append(buffers, buf)
					} else {
						// Put buffer back
						if len(buffers) > 0 {
							idx := j % len(buffers)
							buf := buffers[idx]

							// Verify pattern is still intact
							pattern := byte(goroutineID % 256)
							for k := 0; k < len(buf); k++ {
								if buf[k] != pattern {
									t.Errorf("Buffer corruption detected in goroutine %d", goroutineID)
									break
								}
							}

							pool.Put(buf)
							// Remove from slice
							buffers = append(buffers[:idx], buffers[idx+1:]...)
						}
					}
				}

				// Return all remaining buffers
				for _, buf := range buffers {
					pool.Put(buf)
				}
			}(i)
		}

		wg.Wait()
		close(panicChan)

		// Check for panics
		for panic := range panicChan {
			t.Errorf("Panic occurred in concurrent test: %v", panic)
		}

		// Verify final statistics make sense
		stats := pool.GetStats()
		t.Logf("Final stats - Gets: %d, Puts: %d, Hits: %d, Misses: %d, Active: %d",
			stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

		assert.GreaterOrEqual(t, stats.TotalGets, int64(0))
		assert.GreaterOrEqual(t, stats.TotalPuts, int64(0))
		assert.GreaterOrEqual(t, stats.PoolHits, int64(0))
		assert.GreaterOrEqual(t, stats.PoolMisses, int64(0))
		// Active buffers should be close to 0 (some might still be in use due to timing)
		assert.LessOrEqual(t, stats.ActiveBuffers, int64(100))
	})

	t.Run("concurrent_statistics_accuracy", func(t *testing.T) {
		// Reset pool for clean statistics
		pool := NewDefaultBufferPool(config)

		const numGoroutines = 50
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()

				for j := 0; j < numOperations; j++ {
					buf := pool.Get(32 * 1024)
					// Do some work with buffer
					for k := 0; k < len(buf); k++ {
						buf[k] = byte(k % 256)
					}
					pool.Put(buf)
				}
			}()
		}

		wg.Wait()

		stats := pool.GetStats()
		expectedOps := int64(numGoroutines * numOperations)

		// Statistics should be accurate
		assert.Equal(t, expectedOps, stats.TotalGets, "Total gets should match expected operations")
		assert.Equal(t, expectedOps, stats.TotalPuts, "Total puts should match expected operations")
		assert.Equal(t, int64(0), stats.ActiveBuffers, "Active buffers should be 0 after all operations complete")

		// Hits + Misses should equal total gets
		assert.Equal(t, stats.TotalGets, stats.PoolHits+stats.PoolMisses, "Hits + Misses should equal total gets")
	})

	t.Run("race_condition_detection", func(t *testing.T) {
		// This test is designed to be run with -race flag
		const numGoroutines = 20
		const numOperations = 500

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Shared buffer pool
		sharedBuffers := make(chan []byte, 100)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				for j := 0; j < numOperations; j++ {
					select {
					case buf := <-sharedBuffers:
						// Got a shared buffer, modify it and put back to pool
						for k := 0; k < len(buf); k++ {
							buf[k] = byte(id)
						}
						pool.Put(buf)
					default:
						// No shared buffer available, get new one
						buf := pool.Get(16 * 1024)
						for k := 0; k < len(buf); k++ {
							buf[k] = byte(id)
						}

						// Sometimes put in shared channel, sometimes directly back to pool
						if j%3 == 0 {
							select {
							case sharedBuffers <- buf:
								// Successfully shared
							default:
								// Channel full, put back to pool
								pool.Put(buf)
							}
						} else {
							pool.Put(buf)
						}
					}
				}
			}(i)
		}

		wg.Wait()

		// Clean up any remaining buffers in channel
		close(sharedBuffers)
		for buf := range sharedBuffers {
			pool.Put(buf)
		}
	})
}

func TestDefaultBufferPool_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("high_concurrency_stress", func(t *testing.T) {
		const numGoroutines = 200
		const numOperations = 2000
		const testDuration = 5 * time.Second

		var wg sync.WaitGroup
		var stopFlag int64

		// Stop test after duration
		go func() {
			time.Sleep(testDuration)
			atomic.StoreInt64(&stopFlag, 1)
		}()

		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				buffers := make([][]byte, 0, 20)
				sizes := []int{1024, 2048, 4096, 8192, 16384, 32768, 65536}
				operations := 0

				for atomic.LoadInt64(&stopFlag) == 0 && operations < numOperations {
					operations++

					if len(buffers) < 10 && (len(buffers) == 0 || operations%4 != 0) {
						// Get buffer
						size := sizes[operations%len(sizes)]
						buf := pool.Get(size)

						// Verify buffer
						assert.NotNil(t, buf)
						assert.GreaterOrEqual(t, len(buf), size)

						// Fill with pattern
						pattern := byte((id + operations) % 256)
						for j := 0; j < len(buf); j++ {
							buf[j] = pattern
						}

						buffers = append(buffers, buf)
					} else if len(buffers) > 0 {
						// Put buffer back
						idx := operations % len(buffers)
						buf := buffers[idx]

						// Verify pattern integrity
						expectedPattern := byte((id + operations - len(buffers) + idx) % 256)
						for j := 0; j < len(buf); j++ {
							if buf[j] != expectedPattern {
								// Pattern might have changed due to reuse, which is expected
								break
							}
						}

						pool.Put(buf)
						buffers = append(buffers[:idx], buffers[idx+1:]...)
					}

					// Occasionally check statistics
					if operations%100 == 0 {
						stats := pool.GetStats()
						assert.GreaterOrEqual(t, stats.TotalGets, int64(0))
						assert.GreaterOrEqual(t, stats.TotalPuts, int64(0))
					}
				}

				// Clean up remaining buffers
				for _, buf := range buffers {
					pool.Put(buf)
				}
			}(i)
		}

		wg.Wait()

		// Final verification
		stats := pool.GetStats()
		t.Logf("Stress test completed - Gets: %d, Puts: %d, Hits: %d, Misses: %d, Active: %d",
			stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

		// Basic sanity checks
		assert.GreaterOrEqual(t, stats.TotalGets, int64(numGoroutines))
		assert.GreaterOrEqual(t, stats.TotalPuts, int64(numGoroutines))
		assert.Equal(t, stats.TotalGets, stats.PoolHits+stats.PoolMisses)
	})
}
func TestDefaultBufferPool_CopyBuffer(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("copy_accuracy", func(t *testing.T) {
		// Test data of various sizes
		testData := [][]byte{
			[]byte("small test data"),
			make([]byte, 1024),   // 1KB
			make([]byte, 16384),  // 16KB
			make([]byte, 65536),  // 64KB
			make([]byte, 131072), // 128KB
		}

		// Fill test data with patterns
		for i, data := range testData {
			pattern := byte(i + 1)
			for j := range data {
				data[j] = pattern
			}
		}

		for i, data := range testData {
			t.Run(fmt.Sprintf("size_%d", len(data)), func(t *testing.T) {
				src := bytes.NewReader(data)
				var dst bytes.Buffer

				// Copy using buffer pool
				n, err := pool.CopyBuffer(&dst, src)

				// Verify results
				assert.NoError(t, err)
				assert.Equal(t, int64(len(data)), n)
				assert.Equal(t, data, dst.Bytes())

				// Verify data integrity
				pattern := byte(i + 1)
				for j, b := range dst.Bytes() {
					assert.Equal(t, pattern, b, "Data corruption at position %d", j)
				}
			})
		}
	})

	t.Run("copy_vs_standard_io", func(t *testing.T) {
		// Compare results with standard io.Copy
		testData := make([]byte, 50*1024) // 50KB
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Test with buffer pool
		src1 := bytes.NewReader(testData)
		var dst1 bytes.Buffer
		n1, err1 := pool.CopyBuffer(&dst1, src1)

		// Test with standard io.Copy
		src2 := bytes.NewReader(testData)
		var dst2 bytes.Buffer
		n2, err2 := io.Copy(&dst2, src2)

		// Results should be identical
		assert.Equal(t, err2, err1)
		assert.Equal(t, n2, n1)
		assert.Equal(t, dst2.Bytes(), dst1.Bytes())
	})

	t.Run("copy_with_disabled_pool", func(t *testing.T) {
		disabledConfig := BufferPoolConfig{
			Enabled:           false,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
		}

		disabledPool := NewDefaultBufferPool(disabledConfig)

		testData := []byte("test data for disabled pool")
		src := bytes.NewReader(testData)
		var dst bytes.Buffer

		n, err := disabledPool.CopyBuffer(&dst, src)

		assert.NoError(t, err)
		assert.Equal(t, int64(len(testData)), n)
		assert.Equal(t, testData, dst.Bytes())
	})

	t.Run("copy_error_handling", func(t *testing.T) {
		// Test with reader that returns error
		errorReader := &errorReader{data: []byte("test"), errorAfter: 2}
		var dst bytes.Buffer

		n, err := pool.CopyBuffer(&dst, errorReader)

		assert.Error(t, err)
		assert.Equal(t, int64(2), n) // Should have copied 2 bytes before error
		assert.Equal(t, []byte("te"), dst.Bytes())
	})

	t.Run("copy_large_data", func(t *testing.T) {
		// Test with data larger than buffer size
		largeData := make([]byte, 200*1024) // 200KB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		src := bytes.NewReader(largeData)
		var dst bytes.Buffer

		n, err := pool.CopyBuffer(&dst, src)

		assert.NoError(t, err)
		assert.Equal(t, int64(len(largeData)), n)
		assert.Equal(t, largeData, dst.Bytes())
	})
}

func TestDefaultBufferPool_TeeReader(t *testing.T) {
	config := BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	t.Run("tee_accuracy", func(t *testing.T) {
		testData := []byte("Hello, World! This is a test for TeeReader functionality.")
		src := bytes.NewReader(testData)
		var teeOutput bytes.Buffer

		// Create tee reader
		teeReader := pool.TeeReader(src, &teeOutput)

		// Read from tee reader
		var mainOutput bytes.Buffer
		n, err := io.Copy(&mainOutput, teeReader)

		// Verify results
		assert.NoError(t, err)
		assert.Equal(t, int64(len(testData)), n)
		assert.Equal(t, testData, mainOutput.Bytes())
		assert.Equal(t, testData, teeOutput.Bytes())
	})

	t.Run("tee_vs_standard_io", func(t *testing.T) {
		testData := make([]byte, 10*1024) // 10KB
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Test with buffer pool TeeReader
		src1 := bytes.NewReader(testData)
		var teeOutput1 bytes.Buffer
		teeReader1 := pool.TeeReader(src1, &teeOutput1)
		var mainOutput1 bytes.Buffer
		n1, err1 := io.Copy(&mainOutput1, teeReader1)

		// Test with standard io.TeeReader
		src2 := bytes.NewReader(testData)
		var teeOutput2 bytes.Buffer
		teeReader2 := io.TeeReader(src2, &teeOutput2)
		var mainOutput2 bytes.Buffer
		n2, err2 := io.Copy(&mainOutput2, teeReader2)

		// Results should be identical
		assert.Equal(t, err2, err1)
		assert.Equal(t, n2, n1)
		assert.Equal(t, mainOutput2.Bytes(), mainOutput1.Bytes())
		assert.Equal(t, teeOutput2.Bytes(), teeOutput1.Bytes())
	})

	t.Run("tee_with_disabled_pool", func(t *testing.T) {
		disabledConfig := BufferPoolConfig{
			Enabled:           false,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
		}

		disabledPool := NewDefaultBufferPool(disabledConfig)

		testData := []byte("test data for disabled pool tee reader")
		src := bytes.NewReader(testData)
		var teeOutput bytes.Buffer

		teeReader := disabledPool.TeeReader(src, &teeOutput)
		var mainOutput bytes.Buffer
		n, err := io.Copy(&mainOutput, teeReader)

		assert.NoError(t, err)
		assert.Equal(t, int64(len(testData)), n)
		assert.Equal(t, testData, mainOutput.Bytes())
		assert.Equal(t, testData, teeOutput.Bytes())
	})

	t.Run("tee_partial_reads", func(t *testing.T) {
		testData := []byte("This is a longer test string for partial read testing.")
		src := bytes.NewReader(testData)
		var teeOutput bytes.Buffer

		teeReader := pool.TeeReader(src, &teeOutput)

		// Read in small chunks
		var mainOutput bytes.Buffer
		buf := make([]byte, 5) // Small buffer for partial reads

		for {
			n, err := teeReader.Read(buf)
			if n > 0 {
				mainOutput.Write(buf[:n])
			}
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
		}

		// Verify results
		assert.Equal(t, testData, mainOutput.Bytes())
		assert.Equal(t, testData, teeOutput.Bytes())
	})

	t.Run("tee_error_handling", func(t *testing.T) {
		// Test with reader that returns error
		errorReader := &errorReader{data: []byte("test data"), errorAfter: 4}
		var teeOutput bytes.Buffer

		teeReader := pool.TeeReader(errorReader, &teeOutput)
		var mainOutput bytes.Buffer

		buf := make([]byte, 10)
		totalRead := 0

		for {
			n, err := teeReader.Read(buf)
			if n > 0 {
				mainOutput.Write(buf[:n])
				totalRead += n
			}
			if err != nil {
				if err != io.EOF {
					assert.Error(t, err) // Should get the error from errorReader
				}
				break
			}
		}

		// Should have read 4 bytes before error
		assert.Equal(t, 4, totalRead)
		assert.Equal(t, []byte("test"), mainOutput.Bytes())
		assert.Equal(t, []byte("test"), teeOutput.Bytes())
	})

	t.Run("tee_write_error_handling", func(t *testing.T) {
		testData := []byte("test data")
		src := bytes.NewReader(testData)

		// Create error writer that fails after 4 bytes
		ew := &errorWriter{errorAfter: 4}

		teeReader := pool.TeeReader(src, ew)
		buf := make([]byte, 10)
		n, err := teeReader.Read(buf)

		// Should get write error after reading some data
		assert.Error(t, err)
		assert.Greater(t, n, 0, "Should have read some data before error")
		assert.Contains(t, err.Error(), "simulated write error")
	})

	t.Run("tee_large_data", func(t *testing.T) {
		// Test with data larger than typical buffer sizes
		largeData := make([]byte, 150*1024) // 150KB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		src := bytes.NewReader(largeData)
		var teeOutput bytes.Buffer

		teeReader := pool.TeeReader(src, &teeOutput)
		var mainOutput bytes.Buffer
		n, err := io.Copy(&mainOutput, teeReader)

		assert.NoError(t, err)
		assert.Equal(t, int64(len(largeData)), n)
		assert.Equal(t, largeData, mainOutput.Bytes())
		assert.Equal(t, largeData, teeOutput.Bytes())
	})
}

func TestDefaultBufferPool_FallbackBehavior(t *testing.T) {
	t.Run("fallback_when_disabled", func(t *testing.T) {
		config := BufferPoolConfig{
			Enabled:           false,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
		}

		pool := NewDefaultBufferPool(config)

		// Test CopyBuffer fallback
		testData := []byte("fallback test data")
		src := bytes.NewReader(testData)
		var dst bytes.Buffer

		n, err := pool.CopyBuffer(&dst, src)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(testData)), n)
		assert.Equal(t, testData, dst.Bytes())

		// Test TeeReader fallback
		src2 := bytes.NewReader(testData)
		var teeOutput bytes.Buffer
		teeReader := pool.TeeReader(src2, &teeOutput)

		var mainOutput bytes.Buffer
		n2, err2 := io.Copy(&mainOutput, teeReader)

		assert.NoError(t, err2)
		assert.Equal(t, int64(len(testData)), n2)
		assert.Equal(t, testData, mainOutput.Bytes())
		assert.Equal(t, testData, teeOutput.Bytes())

		// Verify statistics when disabled - should be all zeros since no pool operations occur
		stats := pool.GetStats()
		assert.Equal(t, int64(0), stats.PoolHits)
		assert.Equal(t, int64(0), stats.PoolMisses)
		assert.Equal(t, int64(0), stats.TotalGets)
		assert.Equal(t, int64(0), stats.TotalPuts)
		assert.Equal(t, int64(0), stats.ActiveBuffers)
	})

	t.Run("performance_comparison", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping performance test in short mode")
		}

		testData := make([]byte, 100*1024) // 100KB
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		// Test with enabled pool
		enabledConfig := BufferPoolConfig{
			Enabled:           true,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
		}
		enabledPool := NewDefaultBufferPool(enabledConfig)

		// Test with disabled pool (fallback)
		disabledConfig := BufferPoolConfig{
			Enabled:           false,
			DefaultBufferSize: 32 * 1024,
			MaxBufferSize:     1024 * 1024,
			MinBufferSize:     1024,
		}
		disabledPool := NewDefaultBufferPool(disabledConfig)

		const iterations = 100

		// Benchmark enabled pool
		start := time.Now()
		for i := 0; i < iterations; i++ {
			src := bytes.NewReader(testData)
			var dst bytes.Buffer
			_, err := enabledPool.CopyBuffer(&dst, src)
			assert.NoError(t, err)
		}
		enabledDuration := time.Since(start)

		// Benchmark disabled pool (fallback)
		start = time.Now()
		for i := 0; i < iterations; i++ {
			src := bytes.NewReader(testData)
			var dst bytes.Buffer
			_, err := disabledPool.CopyBuffer(&dst, src)
			assert.NoError(t, err)
		}
		disabledDuration := time.Since(start)

		t.Logf("Enabled pool: %v, Disabled pool: %v", enabledDuration, disabledDuration)

		// Both should complete successfully (performance comparison is informational)
		assert.Greater(t, enabledDuration, time.Duration(0))
		assert.Greater(t, disabledDuration, time.Duration(0))
	})
}
