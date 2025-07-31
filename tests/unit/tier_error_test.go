package unit

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTierError(t *testing.T) {
	t.Run("daramjwee.TierError creation and formatting", func(t *testing.T) {
		originalErr := errors.daramjwee.New("connection refused")
		tierErr := NewTierError(1, "get", "test-key", originalErr)

		assert.Equal(t, 1, tierErr.Tier)
		assert.Equal(t, "get", tierErr.Operation)
		assert.Equal(t, "test-key", tierErr.Key)
		assert.Equal(t, originalErr, tierErr.Err)

		expectedMsg := "daramjwee: tier 1 get failed for key 'test-key': connection refused"
		assert.Equal(t, expectedMsg, tierErr.Error())
	})

	t.Run("daramjwee.TierError without key", func(t *testing.T) {
		originalErr := errors.daramjwee.New("disk full")
		tierErr := NewTierError(2, "set", "", originalErr)

		expectedMsg := "daramjwee: tier 2 set failed: disk full"
		assert.Equal(t, expectedMsg, tierErr.Error())
	})

	t.Run("daramjwee.TierError unwrapping", func(t *testing.T) {
		originalErr := context.DeadlineExceeded
		tierErr := NewTierError(0, "delete", "timeout-key", originalErr)

		// Test errors.Is() works with unwrapping
		assert.True(t, errors.Is(tierErr, context.DeadlineExceeded))

		// Test errors.Unwrap() works
		assert.Equal(t, originalErr, errors.Unwrap(tierErr))
	})

	t.Run("daramjwee.TierError with custom error type", func(t *testing.T) {
		originalErr := &customTestError{code: 500, msg: "internal server error"}
		tierErr := NewTierError(1, "get", "custom-key", originalErr)

		// Test errors.As() works with unwrapping
		var customErr *customTestError
		assert.True(t, errors.As(tierErr, &customErr))
		assert.Equal(t, 500, customErr.code)
		assert.Equal(t, "internal server error", customErr.msg)
	})
}

func TestCacheOperations(t *testing.T) {
	t.Run("Get operation with tier errors", func(t *testing.T) {
		// Create a failing store for tier 1
		store0 := newMockStore()
		store1 := &failingStore{err: errors.daramjwee.New("network timeout")}
		store2 := newMockStore()

		cache, err := daramjwee.New(nil, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &simpleFetcher{data: "origin data"}

		// This should succeed by fetching from origin since all stores fail/are empty
		reader, err := cache.Get(ctx, "test-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		// Verify fetcher was called (cache miss)
		assert.True(t, fetcher.called)
	})

	t.Run("Delete operation with tier errors", func(t *testing.T) {
		// Create stores where one fails
		store0 := newMockStore()
		store1 := &failingStore{err: errors.daramjwee.New("permission denied")}
		store2 := newMockStore()

		// Pre-populate stores with data
		store0.data["test-key"] = []byte("data")
		// Initialize failingStore's data map
		if store1.data == nil {
			store1.data = make(map[string][]byte)
		}
		store1.data["test-key"] = []byte("data") // This will fail to delete
		store2.data["test-key"] = []byte("data")

		cache, err := daramjwee.New(nil, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		err = cache.Delete(ctx, "test-key")

		// Should return daramjwee.TierError from the failing store
		require.Error(t, err)
		var tierErr *TierError
		require.ErrorAs(t, err, &tierErr)
		assert.Equal(t, 1, tierErr.Tier)
		assert.Equal(t, "delete", tierErr.Operation)
		assert.Equal(t, "test-key", tierErr.Key)
		assert.Contains(t, tierErr.Error(), "permission denied")

		// Verify successful deletes from other stores
		_, exists := store0.data["test-key"]
		assert.False(t, exists, "Store0 should have deleted the key")
		_, exists = store2.data["test-key"]
		assert.False(t, exists, "Store2 should have deleted the key")

		// Store1 should still have the data due to failure
		_, exists = store1.data["test-key"]
		assert.True(t, exists, "Store1 should still have the key due to delete failure")
	})

	t.Run("Promotion operation with tier errors", func(t *testing.T) {
		// Create stores where promotion target fails
		store0 := &failingStore{err: errors.daramjwee.New("disk full")}
		store1 := newMockStore()
		store2 := newMockStore()

		// Put data only in store2
		store2.data["test-key"] = []byte("tier 2 data")
		store2.meta["test-key"] = &daramjwee.Metadata{ETag: "test-etag"}

		cache, err := daramjwee.New(nil, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &simpleFetcher{data: "origin data"}

		// This should still succeed by returning data from store2
		// even though promotion to store0 fails
		reader, err := cache.Get(ctx, "test-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		// Verify we got the data from store2
		data := make([]byte, 100)
		n, _ := reader.Read(data)
		assert.Equal(t, "tier 2 data", string(data[:n]))

		// Verify fetcher was not called (cache hit)
		assert.False(t, fetcher.called)
	})
}

// failingStore is a mock store that always fails operations
type failingStore struct {
	err  error
	data map[string][]byte
	meta map[string]*daramjwee.Metadata
}

func (f *failingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, f.err
}

func (f *failingStore) SetWithWriter(ctx context.Context, key string, metadata *daramjwee.Metadata) (io.WriteCloser, error) {
	return nil, f.err
}

func (f *failingStore) Delete(ctx context.Context, key string) error {
	return f.err
}

func (f *failingStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, f.err
}

// customTestError is a custom error type for testing error unwrapping
type customTestError struct {
	code int
	msg  string
}

func (e *customTestError) Error() string {
	return e.msg
}

// simpleFetcher implements daramjwee.Fetcher interface for testing
type simpleFetcher struct {
	data   string
	called bool
}

func (f *simpleFetcher) Fetch(ctx context.Context, metadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.called = true
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		daramjwee.Metadata: &daramjwee.Metadata{ETag: "origin-etag"},
	}, nil
}
