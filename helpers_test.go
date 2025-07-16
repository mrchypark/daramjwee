package daramjwee

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBytes(t *testing.T) {
	// Setup cache
	logger := log.NewNopLogger()
	hotStore := newMockStore()
	cache, err := New(logger, WithHotStore(hotStore))
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "test-key"
	expectedData := []byte("Hello, World!")

	// Create a simple fetcher
	fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
		return expectedData, &Metadata{
			ETag:     "test-etag",
			CachedAt: time.Now(),
		}, nil
	})

	// Test GetBytes
	data, err := GetBytes(ctx, cache, key, fetcher)
	require.NoError(t, err)
	assert.Equal(t, expectedData, data)

	// Test cache hit (should not call fetcher again)
	data2, err := GetBytes(ctx, cache, key, fetcher)
	require.NoError(t, err)
	assert.Equal(t, expectedData, data2)
}

func TestGetString(t *testing.T) {
	// Setup cache
	logger := log.NewNopLogger()
	hotStore := newMockStore()
	cache, err := New(logger, WithHotStore(hotStore))
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "test-string-key"
	expectedString := "Hello, daramjwee!"

	// Create a simple fetcher
	fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
		return []byte(expectedString), &Metadata{
			ETag:     "string-etag",
			CachedAt: time.Now(),
		}, nil
	})

	// Test GetString
	str, err := GetString(ctx, cache, key, fetcher)
	require.NoError(t, err)
	assert.Equal(t, expectedString, str)
}

func TestSetBytes(t *testing.T) {
	// Setup cache
	logger := log.NewNopLogger()
	hotStore := newMockStore()
	cache, err := New(logger, WithHotStore(hotStore))
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "set-test-key"
	testData := []byte("Test data for SetBytes")

	// Test SetBytes
	metadata := &Metadata{
		ETag:     "set-etag",
		CachedAt: time.Now(),
	}
	err = SetBytes(ctx, cache, key, testData, metadata)
	require.NoError(t, err)

	// Verify data was stored correctly
	fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
		t.Fatal("Fetcher should not be called for cache hit")
		return nil, nil, nil
	})

	retrievedData, err := GetBytes(ctx, cache, key, fetcher)
	require.NoError(t, err)
	assert.Equal(t, testData, retrievedData)
}

func TestSetString(t *testing.T) {
	// Setup cache
	logger := log.NewNopLogger()
	hotStore := newMockStore()
	cache, err := New(logger, WithHotStore(hotStore))
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	key := "set-string-key"
	testString := "Test string for SetString"

	// Test SetString
	metadata := &Metadata{
		ETag:     "set-string-etag",
		CachedAt: time.Now(),
	}
	err = SetString(ctx, cache, key, testString, metadata)
	require.NoError(t, err)

	// Verify string was stored correctly
	fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
		t.Fatal("Fetcher should not be called for cache hit")
		return nil, nil, nil
	})

	retrievedString, err := GetString(ctx, cache, key, fetcher)
	require.NoError(t, err)
	assert.Equal(t, testString, retrievedString)
}

func TestSimpleFetcher(t *testing.T) {
	expectedData := []byte("Simple fetcher test data")
	expectedMetadata := &Metadata{
		ETag:     "simple-fetcher-etag",
		CachedAt: time.Now(),
	}

	// Create SimpleFetcher
	fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
		return expectedData, expectedMetadata, nil
	})

	// Test Fetch method
	ctx := context.Background()
	result, err := fetcher.Fetch(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expectedMetadata, result.Metadata)

	// Read body
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	require.NoError(t, err)
	assert.Equal(t, expectedData, data)
}

func TestHelperFunctionsIntegration(t *testing.T) {
	// Setup cache
	logger := log.NewNopLogger()
	hotStore := newMockStore()
	cache, err := New(logger, WithHotStore(hotStore))
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	// Test complete workflow: Set -> Get -> Verify
	testCases := []struct {
		key  string
		data string
		etag string
	}{
		{"key1", "Hello World", "etag1"},
		{"key2", "안녕하세요", "etag2"},
		{"key3", "JSON data: {\"test\": true}", "etag3"},
	}

	// Set all test data
	for _, tc := range testCases {
		metadata := &Metadata{
			ETag:     tc.etag,
			CachedAt: time.Now(),
		}
		err := SetString(ctx, cache, tc.key, tc.data, metadata)
		require.NoError(t, err, "Failed to set data for key: %s", tc.key)
	}

	// Retrieve and verify all test data
	for _, tc := range testCases {
		// This fetcher should never be called since data is already cached
		fetcher := SimpleFetcher(func(ctx context.Context, oldMetadata *Metadata) ([]byte, *Metadata, error) {
			t.Fatalf("Fetcher should not be called for cached key: %s", tc.key)
			return nil, nil, nil
		})

		retrievedData, err := GetString(ctx, cache, tc.key, fetcher)
		require.NoError(t, err, "Failed to get data for key: %s", tc.key)
		assert.Equal(t, tc.data, retrievedData, "Data mismatch for key: %s", tc.key)
	}
}
