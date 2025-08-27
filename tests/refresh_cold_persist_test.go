package daramjwee_test

import (
    "context"
    "io"
    "testing"
    "time"

    "github.com/mrchypark/daramjwee"
    "github.com/stretchr/testify/require"
)

// TestScheduleRefresh_PersistsToCold ensures that when ScheduleRefresh updates
// the hot cache, the content is also persisted to the cold store.
func TestScheduleRefresh_PersistsToCold(t *testing.T) {
    hot := newMockStore()
    cold := newMockStore()

    cache, err := daramjwee.New(nil,
        daramjwee.WithHotStore(hot),
        daramjwee.WithColdStore(cold),
        daramjwee.WithDefaultTimeout(2*time.Second),
    )
    require.NoError(t, err)
    defer cache.Close()

    ctx := context.Background()
    key := "refresh-cold-persist"

    // Seed initial value into hot via public API
    wc, err := cache.Set(ctx, key, &daramjwee.Metadata{ETag: "v0"})
    require.NoError(t, err)
    _, err = wc.Write([]byte("old-value"))
    require.NoError(t, err)
    require.NoError(t, wc.Close())

    // Schedule a refresh that produces new content
    mf := &mockFetcher{content: "new-value", etag: "v1"}

    // Submit refresh
    require.NoError(t, cache.ScheduleRefresh(ctx, key, mf))

    // Wait until the cold store contains the refreshed value.
    require.Eventually(t, func() bool {
        r, _, err := cold.GetStream(ctx, key)
        if err != nil {
            return false
        }
        defer r.Close()
        got, err := io.ReadAll(r)
        if err != nil {
            return false
        }
        return string(got) == "new-value"
    }, 2*time.Second, 10*time.Millisecond, "timed out waiting for cold store to contain refreshed value")
}
