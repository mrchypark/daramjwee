package integration

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiTierPromotion(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("3-tier promotion from tier 2 to tiers 0 and 1", func(t *testing.T) {
		store0 := newMockStore() // Primary tier
		store1 := newMockStore() // Secondary tier
		store2 := newMockStore() // Tertiary tier

		// Put data only in tertiary store (tier 2)
		testData := []byte("tier 2 data")
		store2.data["test-key"] = testData
		store2.meta["test-key"] = &daramjwee.Metadata{
			ETag:     "test-etag",
			CachedAt: time.Now(),
		}

		cache, err := daramjwee.New(logger, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &testFetcher{data: "origin data"}

		reader, err := cache.Get(ctx, "test-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		// Read the data
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "tier 2 data", string(data))

		// Verify fetcher was not called
		assert.False(t, fetcher.called, "daramjwee.Fetcher should not be called for tier hit")

		// Note: Promotion verification would require more complex testing
		// as the promotion happens asynchronously via TeeReader
		// The data should eventually be promoted to tiers 0 and 1
	})

	t.Run("2-tier promotion from tier 1 to tier 0", func(t *testing.T) {
		store0 := newMockStore() // Primary tier
		store1 := newMockStore() // Secondary tier

		// Put data only in secondary store (tier 1)
		testData := []byte("tier 1 data")
		store1.data["test-key"] = testData
		store1.meta["test-key"] = &daramjwee.Metadata{
			ETag:     "test-etag",
			CachedAt: time.Now(),
		}

		cache, err := daramjwee.New(logger, WithStores(store0, store1))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &testFetcher{data: "origin data"}

		reader, err := cache.Get(ctx, "test-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		// Read the data
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "tier 1 data", string(data))

		// Verify fetcher was not called
		assert.False(t, fetcher.called, "daramjwee.Fetcher should not be called for tier hit")
	})

	t.Run("no promotion needed for primary tier hit", func(t *testing.T) {
		store0 := newMockStore() // Primary tier
		store1 := newMockStore() // Secondary tier

		// Put data only in primary store (tier 0)
		testData := []byte("tier 0 data")
		store0.data["test-key"] = testData
		store0.meta["test-key"] = &daramjwee.Metadata{
			ETag:     "test-etag",
			CachedAt: time.Now(),
		}

		cache, err := daramjwee.New(logger, WithStores(store0, store1))
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		fetcher := &testFetcher{data: "origin data"}

		reader, err := cache.Get(ctx, "test-key", fetcher)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close()

		// Read the data
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "tier 0 data", string(data))

		// Verify fetcher was not called
		assert.False(t, fetcher.called, "daramjwee.Fetcher should not be called for primary tier hit")

		// Verify no data was written to secondary store (no promotion needed)
		_, exists := store1.data["test-key"]
		assert.False(t, exists, "Secondary store should not have data for primary tier hit")
	})
}

func TestPromoteToUpperTiersMethod(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("valid promotion from tier 2 to tiers 0 and 1", func(t *testing.T) {
		store0 := newMockStore()
		store1 := newMockStore()
		store2 := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(store0, store1, store2))
		require.NoError(t, err)
		defer cache.Close()

		daramjweeCache := cache.(*daramjwee.DaramjweeCache)

		ctx := context.Background()
		key := "test-key"
		tierIndex := 2
		metadata := &daramjwee.Metadata{
			ETag:     "test-etag",
			CachedAt: time.Now(),
		}
		originalData := "test data"
		stream := io.NopCloser(strings.NewReader(originalData))

		result, err := daramjweeCache.promoteToUpperTiers(ctx, key, tierIndex, metadata, stream)
		require.NoError(t, err)
		require.NotNil(t, result)
		defer result.Close()

		// Read the data from the result stream
		data, err := io.ReadAll(result)
		require.NoError(t, err)
		assert.Equal(t, originalData, string(data))

		// Note: Actual promotion verification would require more complex mocking
		// as the writes happen asynchronously through TeeReader
	})

	t.Run("invalid tier index - too low", func(t *testing.T) {
		store0 := newMockStore()
		store1 := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(store0, store1))
		require.NoError(t, err)
		defer cache.Close()

		daramjweeCache := cache.(*daramjwee.DaramjweeCache)

		ctx := context.Background()
		key := "test-key"
		tierIndex := 0 // Invalid: cannot promote from tier 0
		metadata := &daramjwee.Metadata{}
		originalData := "test data"
		stream := io.NopCloser(strings.NewReader(originalData))

		result, err := daramjweeCache.promoteToUpperTiers(ctx, key, tierIndex, metadata, stream)
		require.NoError(t, err)
		assert.Equal(t, stream, result, "Should return original stream for invalid tier index")
	})

	t.Run("invalid tier index - too high", func(t *testing.T) {
		store0 := newMockStore()
		store1 := newMockStore()

		cache, err := daramjwee.New(logger, WithStores(store0, store1))
		require.NoError(t, err)
		defer cache.Close()

		daramjweeCache := cache.(*daramjwee.DaramjweeCache)

		ctx := context.Background()
		key := "test-key"
		tierIndex := 5 // Invalid: higher than available stores
		metadata := &daramjwee.Metadata{}
		originalData := "test data"
		stream := io.NopCloser(strings.NewReader(originalData))

		result, err := daramjweeCache.promoteToUpperTiers(ctx, key, tierIndex, metadata, stream)
		require.NoError(t, err)
		assert.Equal(t, stream, result, "Should return original stream for invalid tier index")
	})
}

func TestMultiCloserFunctionality(t *testing.T) {
	t.Run("multiCloser closes all closers", func(t *testing.T) {
		// Create mock closers
		closer1 := &testCloser{}
		closer2 := &testCloser{}
		closer3 := &testCloser{}

		reader := strings.NewReader("test data")
		multiCloser := newMultiCloser(reader, []io.Closer{closer1, closer2, closer3})

		// Read some data
		data, err := io.ReadAll(multiCloser)
		require.NoError(t, err)
		assert.Equal(t, "test data", string(data))

		// Close the multiCloser
		err = multiCloser.Close()
		require.NoError(t, err)

		// Verify all closers were closed
		assert.True(t, closer1.closed, "Closer1 should be closed")
		assert.True(t, closer2.closed, "Closer2 should be closed")
		assert.True(t, closer3.closed, "Closer3 should be closed")
	})

	t.Run("multiCloser handles close errors", func(t *testing.T) {
		// Create mock closers, one that fails
		closer1 := &testCloser{}
		closer2 := &testCloser{shouldFail: true}
		closer3 := &testCloser{}

		reader := strings.NewReader("test data")
		multiCloser := newMultiCloser(reader, []io.Closer{closer1, closer2, closer3})

		// Close the multiCloser
		err := multiCloser.Close()
		require.Error(t, err, "Should return error from failing closer")

		// Verify all closers were attempted to be closed
		assert.True(t, closer1.closed, "Closer1 should be closed")
		assert.True(t, closer2.closed, "Closer2 should be attempted to close")
		assert.True(t, closer3.closed, "Closer3 should be closed")
	})
}

// testFetcher implements daramjwee.Fetcher interface for testing
type testFetcher struct {
	data   string
	called bool
}

func (f *testFetcher) Fetch(ctx context.Context, metadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.called = true
	return &daramjwee.FetchResult{
		Body: io.NopCloser(strings.NewReader(f.data)),
		daramjwee.Metadata: &daramjwee.Metadata{
			ETag: "origin-etag",
		},
	}, nil
}

// testCloser implements io.Closer for testing
type testCloser struct {
	closed     bool
	shouldFail bool
}

func (m *testCloser) Close() error {
	m.closed = true
	if m.shouldFail {
		return errors.daramjwee.New("mock close error")
	}
	return nil
}
