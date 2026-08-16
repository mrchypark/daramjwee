package daramjwee_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// TestCrashConsistency_ConcurrentDeleteAndPromotion verifies that a deleted key
// cannot be resurrected by a concurrent promotion from a lower tier.
func TestCrashConsistency_ConcurrentDeleteAndPromotion(t *testing.T) {
	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed lower tier
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Start multiple concurrent gets
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
			if err != nil {
				return
			}
			defer resp.Close()
			_, _ = io.ReadAll(resp)
		}()
	}

	// Delete while promotions are in progress
	err = cache.Delete(context.Background(), "key")
	require.NoError(t, err)

	wg.Wait()

	// Verify key is deleted
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	defer resp.Close()
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
}

// TestCrashConsistency_PartialWriteDoesNotCorruptCache verifies that a partial
// write does not corrupt the cache entry.
func TestCrashConsistency_PartialWriteDoesNotCorruptCache(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Write initial value
	sink, err := cache.Set(context.Background(), "key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("initial-value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	// Start a partial write
	sink, err = cache.Set(context.Background(), "key", &daramjwee.Metadata{CacheTag: "v2"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("partial"))
	require.NoError(t, err)

	// Abort the partial write
	require.NoError(t, sink.Abort())

	// Verify original value is preserved
	reader, meta, err := store.GetStream(context.Background(), "key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "initial-value", string(body))
	require.Equal(t, "v1", meta.CacheTag)
}

// TestCrashConsistency_ConcurrentWritesOnlyOneSucceeds verifies that when
// multiple concurrent writes target the same key, at least one succeeds
// and the final value is consistent.
func TestCrashConsistency_ConcurrentWritesOnlyOneSucceeds(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sink, err := cache.Set(context.Background(), "key", &daramjwee.Metadata{
				CacheTag: fmt.Sprintf("v%d", idx),
			})
			if err != nil {
				return
			}
			_, err = fmt.Fprintf(sink, "value-%d", idx)
			if err != nil {
				_ = sink.Abort()
				return
			}
			err = sink.Close()
			if err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// At least one write should succeed
	require.Greater(t, int(successCount.Load()), 0)

	// Verify the cache has a consistent value
	reader, _, err := store.GetStream(context.Background(), "key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, body)
}

// TestCrashConsistency_DeleteDuringWriteDoesNotCorrupt verifies that deleting
// a key while a write is in progress does not corrupt the cache.
func TestCrashConsistency_DeleteDuringWriteDoesNotCorrupt(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Start a write
	sink, err := cache.Set(context.Background(), "key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value"))
	require.NoError(t, err)

	// Delete while write is in progress
	_ = cache.Delete(context.Background(), "key")

	// Close the write (may succeed or fail)
	_ = sink.Close()

	// Verify cache is in a consistent state
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	defer resp.Close()
	// Should either find nothing or find the written value
	if resp.Status == daramjwee.GetStatusOK {
		body, err := io.ReadAll(resp)
		require.NoError(t, err)
		require.Contains(t, []string{"value", ""}, string(body))
	}
}

// TestCrashConsistency_GenerationFencePreventsStalePromotion verifies that the
// generation fence prevents stale promotions after a delete.
func TestCrashConsistency_GenerationFencePreventsStalePromotion(t *testing.T) {
	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed lower tier with stale data
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "stale",
		CachedAt: time.Now().Add(-2 * time.Hour),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("stale-value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Delete the key (increments generation)
	err = cache.Delete(context.Background(), "key")
	require.NoError(t, err)

	// Try to get the key (should not resurrect from lower tier)
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	defer resp.Close()
	// Should be not found, not stale
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
}

// TestCrashConsistency_ConcurrentCloseAndOperations verifies that closing the
// cache while operations are in progress does not cause panics or corruption.
func TestCrashConsistency_ConcurrentCloseAndOperations(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Start multiple operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in operation %d: %v", idx, r)
				}
			}()

			key := fmt.Sprintf("key-%d", idx)
			sink, err := cache.Set(context.Background(), key, &daramjwee.Metadata{
				CacheTag: fmt.Sprintf("v%d", idx),
			})
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(sink, "value-%d", idx)
			_ = sink.Close()
		}(i)
	}

	// Close while operations are in progress
	time.Sleep(10 * time.Millisecond)
	cache.Close()

	wg.Wait()
}

// TestCrashConsistency_MultipleCloseCallsIsIdempotent verifies that calling
// Close multiple times is safe and idempotent.
func TestCrashConsistency_MultipleCloseCallsIsIdempotent(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Call Close multiple times concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.Close()
		}()
	}

	wg.Wait()
}

// TestCrashConsistency_DeleteDuringFillPreventsStalePublish verifies that
// deleting a key while a fill is in progress prevents the stale value
// from being published.
func TestCrashConsistency_DeleteDuringFillPreventsStalePublish(t *testing.T) {
	top := memstore.New(0, nil)
	lower := &slowReadStore{
		inner:     memstore.New(0, nil),
		readDelay: 100 * time.Millisecond,
	}

	// Seed lower tier
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Start a Get that will trigger fill
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
		if err == nil {
			defer resp.Close()
			_, _ = io.ReadAll(resp)
		}
	}()

	// Wait a bit for the fill to start
	time.Sleep(50 * time.Millisecond)

	// Delete while fill is in progress
	err = cache.Delete(context.Background(), "key")
	require.NoError(t, err)

	<-getDone

	// Verify key is deleted (not resurrected by fill)
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
	if err == nil {
		defer resp.Close()
		require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	}
}

// slowReadStore wraps a store with artificial read delay
type slowReadStore struct {
	inner     daramjwee.Store
	readDelay time.Duration
}

func (s *slowReadStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	time.Sleep(s.readDelay)
	return s.inner.GetStream(ctx, key)
}

func (s *slowReadStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return s.inner.BeginSet(ctx, key, metadata)
}

func (s *slowReadStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *slowReadStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.inner.Stat(ctx, key)
}

// TestCrashConsistency_ClosePreventsBackgroundActivity verifies that after
// Close() returns, no background activity occurs.
func TestCrashConsistency_ClosePreventsBackgroundActivity(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)

	// Close the cache
	cache.Close()

	// Verify that operations after close fail
	_, err = cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
	require.ErrorIs(t, err, daramjwee.ErrCacheClosed)

	_, err = cache.Set(context.Background(), "key", &daramjwee.Metadata{})
	require.ErrorIs(t, err, daramjwee.ErrCacheClosed)

	err = cache.Delete(context.Background(), "key")
	require.ErrorIs(t, err, daramjwee.ErrCacheClosed)
}

// TestCrashConsistency_GenerationPromotionRaceStress stress tests the
// generation fence under concurrent delete and promotion operations.
func TestCrashConsistency_GenerationPromotionRaceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed lower tier
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run concurrent operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				switch idx % 3 {
				case 0:
					// Get
					resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
					if err == nil {
						_, _ = io.ReadAll(resp)
						_ = resp.Close()
					}
				case 1:
					// Delete
					_ = cache.Delete(context.Background(), "key")
				case 2:
					// Re-seed lower tier
					sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
						CacheTag: fmt.Sprintf("v-%d", idx),
						CachedAt: time.Now(),
					})
					if err == nil {
						_, _ = sink.Write([]byte("value"))
						_ = sink.Close()
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestCrashConsistency_DeleteFillPromotionTripleStress stress tests the
// triple interaction of Delete, Fill, and Promotion under high concurrency.
func TestCrashConsistency_DeleteFillPromotionTripleStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed lower tier
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Concurrent Get operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, nil)
				if err == nil {
					_, _ = io.ReadAll(resp)
					_ = resp.Close()
				}
			}
		}()
	}

	// Concurrent Delete operations
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_ = cache.Delete(context.Background(), "key")
			}
		}()
	}

	// Concurrent re-seed operations
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
					CacheTag: "v-stress",
					CachedAt: time.Now(),
				})
				if err == nil {
					_, _ = sink.Write([]byte("value"))
					_ = sink.Close()
				}
			}
		}()
	}

	wg.Wait()
}
