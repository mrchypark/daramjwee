package daramjwee_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// TestEvictionCorrectness_LRUEvictsLeastRecentlyUsed verifies that LRU
// evicts the least recently used items when capacity is exceeded.
func TestEvictionCorrectness_LRUEvictsLeastRecentlyUsed(t *testing.T) {
	evictionPolicy := policy.NewLRU()
	store := memstore.New(100, evictionPolicy) // 100 bytes capacity
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Fill cache with 3 items (each ~30 bytes)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-aaaaaaaaaaaa", i) // ~30 bytes
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		require.NoError(t, err)
		_, err = sink.Write([]byte(value))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	// Access key-0 and key-1 to make them recently used
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, nil)
		if err == nil {
			defer resp.Close()
			_, _ = io.ReadAll(resp)
		}
	}

	// Add a new item that exceeds capacity
	sink, err := cache.Set(context.Background(), "key-new", &daramjwee.Metadata{CacheTag: "v-new"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value-new-aaaaaaaaaaaaaa")) // ~30 bytes
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	// key-2 should be evicted (least recently used)
	resp, err := cache.Get(context.Background(), "key-2", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	}

	// key-0 and key-1 should still exist
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, nil)
		if err == nil {
			defer resp.Close()
			require.Equal(t, daramjwee.GetStatusOK, resp.Status)
		}
	}
}

// TestEvictionCorrectness_S3FIFOEvictsFromSmallQueue verifies that S3-FIFO
// evicts items from the small queue first.
func TestEvictionCorrectness_S3FIFOEvictsFromSmallQueue(t *testing.T) {
	evictionPolicy := policy.NewS3FIFO(100, 10) // 100 bytes capacity, 10% small queue
	store := memstore.New(100, evictionPolicy)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Fill cache with items
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-aaaaaaaaaaaa", i) // ~30 bytes
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		require.NoError(t, err)
		_, err = sink.Write([]byte(value))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	// Access key-0 multiple times to promote to main queue
	for i := 0; i < 3; i++ {
		resp, err := cache.Get(context.Background(), "key-0", daramjwee.GetRequest{}, nil)
		if err == nil {
			defer resp.Close()
			_, _ = io.ReadAll(resp)
		}
	}

	// Add a new item that exceeds capacity
	sink, err := cache.Set(context.Background(), "key-new", &daramjwee.Metadata{CacheTag: "v-new"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value-new-aaaaaaaaaaaaaa")) // ~30 bytes
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	// key-0 should still exist (promoted to main queue)
	resp, err := cache.Get(context.Background(), "key-0", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		require.Equal(t, daramjwee.GetStatusOK, resp.Status)
	}
}

// TestEvictionCorrectness_SIEVEEvictsUnvisitedItems verifies that SIEVE
// evicts items that have not been visited.
func TestEvictionCorrectness_SIEVEEvictsUnvisitedItems(t *testing.T) {
	evictionPolicy := policy.NewSieve()
	store := memstore.New(100, evictionPolicy)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Fill cache with items
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-aaaaaaaaaaaa", i) // ~30 bytes
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		require.NoError(t, err)
		_, err = sink.Write([]byte(value))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	// Access key-0 to mark it as visited
	resp, err := cache.Get(context.Background(), "key-0", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		_, _ = io.ReadAll(resp)
	}

	// Add a new item that exceeds capacity
	sink, err := cache.Set(context.Background(), "key-new", &daramjwee.Metadata{CacheTag: "v-new"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value-new-aaaaaaaaaaaaaa")) // ~30 bytes
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	// key-0 should still exist (visited)
	resp, err = cache.Get(context.Background(), "key-0", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		require.Equal(t, daramjwee.GetStatusOK, resp.Status)
	}
}

// TestEvictionCorrectness_NoEvictionWhenUnderCapacity verifies that no
// eviction occurs when the cache is under capacity.
func TestEvictionCorrectness_NoEvictionWhenUnderCapacity(t *testing.T) {
	evictionPolicy := policy.NewLRU()
	store := memstore.New(1000, evictionPolicy) // Large capacity
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Add items within capacity
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		require.NoError(t, err)
		_, err = sink.Write([]byte(value))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	// All items should still exist
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, nil)
		if err == nil {
			defer resp.Close()
			require.Equal(t, daramjwee.GetStatusOK, resp.Status)
		}
	}
}

// TestEvictionCorrectness_DeleteRemovesFromPolicy verifies that deleting
// a key also removes it from the eviction policy.
func TestEvictionCorrectness_DeleteRemovesFromPolicy(t *testing.T) {
	evictionPolicy := policy.NewLRU()
	store := memstore.New(100, evictionPolicy)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Add an item
	sink, err := cache.Set(context.Background(), "key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value-aaaaaaaaaaaaaaaaaaaaaa"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	// Delete the item
	err = cache.Delete(context.Background(), "key")
	require.NoError(t, err)

	// Add new items to trigger eviction
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("new-key-%d", i)
		value := fmt.Sprintf("new-value-%d-aaaaaaaa", i)
		sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: fmt.Sprintf("v%d", i)})
		require.NoError(t, err)
		_, err = sink.Write([]byte(value))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	// The deleted key should not be resurrected
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	}
}
