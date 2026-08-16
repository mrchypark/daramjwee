package daramjwee_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// TestRefreshDedup_SingleInFlightRefreshPerKey verifies that only one
// background refresh runs per key at a time.
func TestRefreshDedup_SingleInFlightRefreshPerKey(t *testing.T) {
	store := memstore.New(0, nil)
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(store),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(10),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	fetcher := &mockFetcher{content: "refreshed", etag: "v2", fetchDelay: 150 * time.Millisecond}

	// Two quick refreshes for the same key: the second must be deduplicated.
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "key", fetcher))
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "key", fetcher))

	// Wait for the single in-flight refresh to complete its publish.
	require.Eventually(t, func() bool {
		meta, err := store.Stat(context.Background(), "key")
		return err == nil && meta.CacheTag == "v2"
	}, 2*time.Second, 10*time.Millisecond)

	// Give any (incorrect) duplicate job time to run.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, fetcher.getFetchCount())

	// After the in-flight job completes, a new refresh is allowed again.
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "key", fetcher))
	require.Eventually(t, func() bool {
		return fetcher.getFetchCount() >= 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, 2, fetcher.getFetchCount())
}
