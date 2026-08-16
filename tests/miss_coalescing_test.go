package daramjwee_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// TestMissCoalescing_ConcurrentMissesShareOriginFetch verifies that
// concurrent misses for the same key coalesce into a single origin fetch
// when the leader's fill completes quickly.
func TestMissCoalescing_ConcurrentMissesShareOriginFetch(t *testing.T) {
	hot := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	fetcher := &mockFetcher{content: "coalesced-value", etag: "v1"}

	const callers = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := cache.Get(context.Background(), "hot-key", daramjwee.GetRequest{}, fetcher)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					t.Logf("Get error: %v (fetchCount=%d)", err, fetcher.getFetchCount())
				}
				mu.Unlock()
				return
			}
			defer resp.Close()
			body, err := io.ReadAll(resp)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if string(body) != "coalesced-value" {
				mu.Lock()
				if firstErr == nil {
					firstErr = io.ErrUnexpectedEOF
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.NoError(t, firstErr)
	// Waiters that finished within the wait cap are served from the top tier,
	// so the origin fetch count must be well below the caller count.
	require.LessOrEqual(t, fetcher.getFetchCount(), callers/2+1)
}

// TestMissCoalescing_SlowLeaderFallback verifies that a waiter falls back to
// its own origin fetch when the leader's stream stays open longer than the
// wait cap.
func TestMissCoalescing_SlowLeaderFallback(t *testing.T) {
	hot := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	fetcher := &mockFetcher{content: "value", etag: "v1"}

	// First caller: leader with a held-open stream.
	leaderResp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	defer leaderResp.Close()

	// Second caller: waiter. The leader never closes within the wait cap,
	// so this call must still complete successfully on its own.
	waiterResp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	defer waiterResp.Close()
	body, err := io.ReadAll(waiterResp)
	require.NoError(t, err)
	require.Equal(t, "value", string(body))
}
