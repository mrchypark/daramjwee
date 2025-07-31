package unit

import (
	"github.com/mrchypark/daramjwee"
	"fmt"
	"testing"
)

func TestDebugBufferPoolStats(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     1024 * 1024,
		MinBufferSize:     1024,
	}

	pool := NewDefaultBufferPool(config)

	// Initial stats
	stats := pool.GetStats()
	fmt.Printf("Initial: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Get first buffer
	buf1 := pool.Get(32 * 1024)
	stats = pool.GetStats()
	fmt.Printf("After Get 1: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Get second buffer
	buf2 := pool.Get(32 * 1024)
	stats = pool.GetStats()
	fmt.Printf("After Get 2: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Put first buffer back
	pool.Put(buf1)
	stats = pool.GetStats()
	fmt.Printf("After Put 1: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Put second buffer back
	pool.Put(buf2)
	stats = pool.GetStats()
	fmt.Printf("After Put 2: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	// Get third buffer - should be a hit
	buf3 := pool.Get(32 * 1024)
	stats = pool.GetStats()
	fmt.Printf("After Get 3: Gets=%d, Puts=%d, Hits=%d, Misses=%d, Active=%d\n",
		stats.TotalGets, stats.TotalPuts, stats.PoolHits, stats.PoolMisses, stats.ActiveBuffers)

	pool.Put(buf3)
}
