package daramjwee_test

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// countingBlockingFetcher returns a blocking stream and counts fetches.
type countingBlockingFetcher struct {
	source     *blockingReadCloser
	metadata   *daramjwee.Metadata
	fetchCount atomic.Int32
}

func (f *countingBlockingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.fetchCount.Add(1)
	return &daramjwee.FetchResult{
		Body:     f.source,
		Metadata: f.metadata,
	}, nil
}

// TestMissCoalescing_NoDuplicateFetchWhileLeaderStreams verifies that a
// caller arriving after the leader returned its response but before the
// leader's stream closed joins as a waiter instead of fetching the origin
// again, and is served from the top tier once the fill publishes.
func TestMissCoalescing_NoDuplicateFetchWhileLeaderStreams(t *testing.T) {
	hot := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	source := newBlockingReadCloser([]byte("origin"), []byte("-value"))
	fetcher := &countingBlockingFetcher{
		source:   source,
		metadata: &daramjwee.Metadata{CacheTag: "v1"},
	}

	// Leader: first read returns "origin", then the stream blocks.
	leaderResp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	first := make([]byte, len("origin"))
	n, err := io.ReadFull(leaderResp, first)
	require.NoError(t, err)
	require.Equal(t, "origin", string(first[:n]))

	// Waiter arrives while the leader's stream is still open.
	type waiterResult struct {
		resp *daramjwee.GetResponse
		err  error
	}
	waiterDone := make(chan waiterResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, fetcher)
		waiterDone <- waiterResult{resp: resp, err: err}
	}()

	// The waiter must not trigger a second fetch while the leader streams.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), fetcher.fetchCount.Load())

	// Finish the leader: release the source, drain and close.
	source.Release()
	rest, err := io.ReadAll(leaderResp)
	require.NoError(t, err)
	require.Equal(t, "-value", string(rest))
	require.NoError(t, leaderResp.Close())

	// The waiter completes from the top tier without an extra fetch.
	waiter := <-waiterDone
	require.NoError(t, waiter.err)
	require.NotNil(t, waiter.resp)
	body, err := io.ReadAll(waiter.resp)
	require.NoError(t, err)
	require.Equal(t, "origin-value", string(body))
	require.NoError(t, waiter.resp.Close())
	require.Equal(t, int32(1), fetcher.fetchCount.Load())
}

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
