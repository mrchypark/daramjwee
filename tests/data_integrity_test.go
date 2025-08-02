package daramjwee_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/cache"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// UnmarshalableType is a type that will fail JSON marshaling
type UnmarshalableType struct {
	Channel chan int // channels cannot be marshaled to JSON (no json:"-" tag)
}

// TestGenericCache_Set_MarshalError tests that marshal errors are handled properly
// without corrupting the cache
func TestGenericCache_Set_MarshalError(t *testing.T) {
	logger := log.NewNopLogger()
	memStore := memstore.New(1*1024*1024, policy.NewLRU())

	baseCache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithCache(1*time.Minute),
	)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	// Use a cache for the unmarshalable type
	unmarshalableCache := cache.NewGeneric[UnmarshalableType](baseCache)
	ctx := context.Background()

	// First, set a valid value in a different cache to ensure the store is not corrupted
	stringCache := cache.NewGeneric[string](baseCache)
	key := "test-key"
	validValue := "valid-data"
	err = stringCache.Set(ctx, key, validValue, &daramjwee.Metadata{ETag: "v1"})
	if err != nil {
		t.Fatalf("Failed to set valid value: %v", err)
	}

	// Attempt to set a value that will cause a marshal error
	unmarshalableValue := UnmarshalableType{Channel: make(chan int)}
	err = unmarshalableCache.Set(ctx, "bad-key", unmarshalableValue, &daramjwee.Metadata{ETag: "v1"})
	if err == nil {
		t.Fatal("Expected a marshal error, but got nil")
	}

	// Verify the valid value is still stored and retrievable, ensuring the cache is not corrupted
	fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		t.Error("Fetcher should not be called for cache hit")
		return "", nil, nil
	})

	retrieved, err := stringCache.Get(ctx, key, fetcher)
	if err != nil {
		t.Fatalf("Failed to get valid value after marshal error: %v", err)
	}
	if retrieved != validValue {
		t.Errorf("Expected %q, got %q", validValue, retrieved)
	}

	// Also verify that the bad key was not written to the cache.
	// A fetcher that always returns ErrNotFound confirms that the Get call is a cache miss.
	failingFetcher := cache.GenericFetcher[UnmarshalableType](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (UnmarshalableType, *daramjwee.Metadata, error) {
		return UnmarshalableType{}, nil, daramjwee.ErrNotFound
	})

	_, err = unmarshalableCache.Get(ctx, "bad-key", failingFetcher)
	if err != daramjwee.ErrNotFound {
		t.Errorf("Expected ErrNotFound for the bad key, but got: %v", err)
	}
}

// TestGenericCache_Set_WriteError tests error handling during write operations
func TestGenericCache_Set_WriteError(t *testing.T) {
	logger := log.NewNopLogger()
	memStore := memstore.New(1*1024*1024, policy.NewLRU())

	baseCache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithCache(1*time.Minute),
	)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	stringCache := cache.NewGeneric[string](baseCache)
	ctx := context.Background()

	// Test with a very large value that might cause write issues
	key := "large-key"
	largeValue := string(make([]byte, 10*1024*1024)) // 10MB string

	// This should either succeed or fail cleanly without corrupting the cache
	err = stringCache.Set(ctx, key, largeValue, &daramjwee.Metadata{ETag: "v1"})
	// We don't assert on the error here because it might succeed or fail depending on system limits
	// The important thing is that it doesn't corrupt the cache

	// Set a small valid value to ensure cache is still functional
	smallKey := "small-key"
	smallValue := "small-data"
	err = stringCache.Set(ctx, smallKey, smallValue, &daramjwee.Metadata{ETag: "v1"})
	if err != nil {
		t.Fatalf("Failed to set small value after large value attempt: %v", err)
	}

	// Verify the small value can be retrieved
	fetcher := cache.GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		t.Error("Fetcher should not be called for cache hit")
		return "", nil, nil
	})

	retrieved, err := stringCache.Get(ctx, smallKey, fetcher)
	if err != nil {
		t.Fatalf("Failed to get small value: %v", err)
	}
	if retrieved != smallValue {
		t.Errorf("Expected %q, got %q", smallValue, retrieved)
	}
}

// TestGenericCache_Set_DataIntegrity tests that only complete, valid data is stored
func TestGenericCache_Set_DataIntegrity(t *testing.T) {
	logger := log.NewNopLogger()
	memStore := memstore.New(1*1024*1024, policy.NewLRU())

	baseCache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithCache(1*time.Minute),
	)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer baseCache.Close()

	// Test with a complex struct
	type ComplexData struct {
		ID       int                    `json:"id"`
		Name     string                 `json:"name"`
		Tags     []string               `json:"tags"`
		Metadata map[string]interface{} `json:"metadata"`
	}

	complexCache := cache.NewGeneric[ComplexData](baseCache)
	ctx := context.Background()

	originalData := ComplexData{
		ID:   123,
		Name: "test-data",
		Tags: []string{"tag1", "tag2", "tag3"},
		Metadata: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		},
	}

	key := "complex-key"
	err = complexCache.Set(ctx, key, originalData, &daramjwee.Metadata{ETag: "v1"})
	if err != nil {
		t.Fatalf("Failed to set complex data: %v", err)
	}

	// Retrieve and verify data integrity
	fetcher := cache.GenericFetcher[ComplexData](func(_ context.Context, _ *daramjwee.Metadata) (ComplexData, *daramjwee.Metadata, error) {
		t.Error("Fetcher should not be called for cache hit")
		return ComplexData{}, nil, nil
	})

	retrieved, err := complexCache.Get(ctx, key, fetcher)
	if err != nil {
		t.Fatalf("Failed to get complex data: %v", err)
	}

	// Verify all fields are intact
	if retrieved.ID != originalData.ID {
		t.Errorf("ID mismatch: expected %d, got %d", originalData.ID, retrieved.ID)
	}
	if retrieved.Name != originalData.Name {
		t.Errorf("Name mismatch: expected %q, got %q", originalData.Name, retrieved.Name)
	}
	if !reflect.DeepEqual(retrieved.Tags, originalData.Tags) {
		t.Errorf("Tags mismatch: expected %v, got %v", originalData.Tags, retrieved.Tags)
	}

	// Verify metadata map
	if len(retrieved.Metadata) != len(originalData.Metadata) {
		t.Errorf("Metadata length mismatch: expected %d, got %d", len(originalData.Metadata), len(retrieved.Metadata))
	}
	for key, value := range originalData.Metadata {
		if retrievedValue, exists := retrieved.Metadata[key]; !exists {
			t.Errorf("Metadata key %q missing", key)
		} else {
			// JSON unmarshaling might change number types, so we need to be flexible
			originalJSON, _ := json.Marshal(value)
			retrievedJSON, _ := json.Marshal(retrievedValue)
			if string(originalJSON) != string(retrievedJSON) {
				t.Errorf("Metadata[%q] mismatch: expected %v, got %v", key, value, retrievedValue)
			}
		}
	}
}
