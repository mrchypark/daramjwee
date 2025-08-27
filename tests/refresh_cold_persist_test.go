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

    // Wait for cold store write to complete
    select {
    case <-cold.writeCompleted:
        // proceed
    case <-time.After(2 * time.Second):
        t.Fatalf("timed out waiting for cold store write completion")
    }

    // Validate that cold now has the refreshed value
    r, _, err := cold.GetStream(ctx, key)
    require.NoError(t, err)
    got, err := io.ReadAll(r)
    require.NoError(t, err)
    require.Equal(t, "new-value", string(got))
    _ = r.Close()
}
