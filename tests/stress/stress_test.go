package stress

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowMockStore simulates a slow storage by adding an intentional delay.
type slowMockStore struct {
	mockStore
	delay time.Duration
}

// newSlowMockStore creates a new slowMockStore with the specified delay.
func newSlowMockStore(delay time.Duration) *slowMockStore {
	return &slowMockStore{
		mockStore: *newMockStore(),
		delay:     delay,
	}
}

// GetStream delays for the configured time before returning data.
func (s *slowMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	time.Sleep(s.delay)
	return s.mockStore.GetStream(ctx, key)
}

// TestCache_WithSlowColdStore verifies that the cold hit scenario completes successfully
// within the timeout even when the Cold daramjwee.Store is slow.
func TestStore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	hot := newMockStore()
	slowCold := newSlowMockStore(1 * time.Millisecond)

	cache, err := daramjwee.New(nil, daramjwee.WithHotStore(hot), daramjwee.WithColdStore(slowCold), daramjwee.WithDefaultTimeout(1*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	key := "slow-item"
	content := "content from slow store"
	slowCold.setData(key, content, &daramjwee.Metadata{ETag: "v-slow"})

	stream, err := cache.Get(context.Background(), key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	err = stream.Close()
	require.NoError(t, err)

	assert.Equal(t, content, string(readBytes))

	hot.mu.RLock()
	promotedData, ok := hot.data[key]
	hot.mu.RUnlock()
	assert.True(t, ok, "Data should be promoted to hot cache")
	assert.Equal(t, content, string(promotedData))
}
