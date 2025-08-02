package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Config struct {
	AppName string `json:"app_name"`
	Debug   bool   `json:"debug"`
	Port    int    `json:"port"`
}

func createTestCache() (daramjwee.Cache, error) {
	logger := log.NewNopLogger()
	memStore := memstore.New(1*1024*1024, policy.NewLRU())

	return daramjwee.New(
		logger,
		daramjwee.WithHotStore(memStore),
		daramjwee.WithDefaultTimeout(10*time.Second),
		daramjwee.WithCache(1*time.Minute),
	)
}

func TestGenericCache_SetAndGet(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	userCache := NewGeneric[User](cache)
	ctx := context.Background()

	// Test Set and Get
	user := User{
		ID:    1,
		Name:  "John Doe",
		Email: "john@example.com",
	}
	metadata := &daramjwee.Metadata{ETag: "v1"}

	err = userCache.Set(ctx, "user:1", user, metadata)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Create a fetcher - it should not be called if cache hit occurs
	var fetcherCalled int32
	fetcher := GenericFetcher[User](func(_ context.Context, _ *daramjwee.Metadata) (User, *daramjwee.Metadata, error) {
		atomic.StoreInt32(&fetcherCalled, 1)
		return user, metadata, nil
	})

	retrieved, err := userCache.Get(ctx, "user:1", fetcher)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved != user {
		t.Errorf("Expected %+v, got %+v", user, retrieved)
	}

	// The fetcher should not have been called for a cache hit
	if atomic.LoadInt32(&fetcherCalled) == 1 {
		t.Fatal("Fetcher was called on an expected cache hit")
	}
}

func TestGenericCache_FetcherOnMiss(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	userCache := NewGeneric[User](cache)
	ctx := context.Background()

	expectedUser := User{
		ID:    2,
		Name:  "Jane Smith",
		Email: "jane@example.com",
	}

	var fetcherCalled int32
	fetcher := GenericFetcher[User](func(_ context.Context, _ *daramjwee.Metadata) (User, *daramjwee.Metadata, error) {
		atomic.StoreInt32(&fetcherCalled, 1)
		return expectedUser, &daramjwee.Metadata{ETag: "v2"}, nil
	})

	// First call should trigger fetcher (cache miss)
	retrieved, err := userCache.Get(ctx, "user:2", fetcher)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if atomic.LoadInt32(&fetcherCalled) != 1 {
		t.Error("Fetcher should have been called on cache miss")
	}

	if retrieved != expectedUser {
		t.Errorf("Expected %+v, got %+v", expectedUser, retrieved)
	}

	// Second call should use cache (fetcher should not be called again)
	atomic.StoreInt32(&fetcherCalled, 0)
	retrieved2, err := userCache.Get(ctx, "user:2", fetcher)
	if err != nil {
		t.Fatalf("Second Get failed: %v", err)
	}

	if atomic.LoadInt32(&fetcherCalled) == 1 {
		t.Error("Fetcher should not have been called on cache hit")
	}

	if retrieved2 != expectedUser {
		t.Errorf("Expected %+v, got %+v", expectedUser, retrieved2)
	}
}

func TestGenericCache_String(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	stringCache := NewGeneric[string](cache)
	ctx := context.Background()

	value := "Hello, Generic Cache!"
	metadata := &daramjwee.Metadata{ETag: "v1"}

	err = stringCache.Set(ctx, "greeting", value, metadata)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	fetcher := GenericFetcher[string](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		return value, metadata, nil
	})

	retrieved, err := stringCache.Get(ctx, "greeting", fetcher)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved != value {
		t.Errorf("Expected %q, got %q", value, retrieved)
	}
}

func TestGenericCache_GetOrSet(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	stringCache := NewGeneric[string](cache)
	ctx := context.Background()

	var factoryCalled int32
	factory := func() (string, *daramjwee.Metadata, error) {
		atomic.StoreInt32(&factoryCalled, 1)
		return "factory value", &daramjwee.Metadata{ETag: "v1"}, nil
	}

	// First call should trigger factory
	value, err := stringCache.GetOrSet(ctx, "test-key", factory)
	if err != nil {
		t.Fatalf("GetOrSet failed: %v", err)
	}

	if atomic.LoadInt32(&factoryCalled) != 1 {
		t.Error("Factory should have been called")
	}

	if value != "factory value" {
		t.Errorf("Expected 'factory value', got %q", value)
	}

	// Second call should use cache
	atomic.StoreInt32(&factoryCalled, 0)
	value2, err := stringCache.GetOrSet(ctx, "test-key", factory)
	if err != nil {
		t.Fatalf("Second GetOrSet failed: %v", err)
	}

	if atomic.LoadInt32(&factoryCalled) == 1 {
		t.Error("Factory should not have been called on second GetOrSet")
	}

	if value2 != "factory value" {
		t.Errorf("Expected 'factory value', got %q", value2)
	}
}

func TestGenericCache_Must(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	stringCache := NewGeneric[string](cache)
	ctx := context.Background()

	value := "must work"
	metadata := &daramjwee.Metadata{ETag: "v1"}

	// Should not panic
	stringCache.MustSet(ctx, "must-key", value, metadata)

	fetcher := GenericFetcher[string](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		return value, metadata, nil
	})

	retrieved := stringCache.MustGet(ctx, "must-key", fetcher)
	if retrieved != value {
		t.Errorf("Expected %q, got %q", value, retrieved)
	}
}

func TestGenericCache_GetWithDefault(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	stringCache := NewGeneric[string](cache)
	ctx := context.Background()

	defaultValue := "default"

	fetcher := GenericFetcher[string](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		return "", nil, daramjwee.ErrNotFound
	})

	retrieved := stringCache.GetWithDefault(ctx, "non-existent", defaultValue, fetcher)
	if retrieved != defaultValue {
		t.Errorf("Expected %q, got %q", defaultValue, retrieved)
	}
}

func TestGenericCache_Delete(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	stringCache := NewGeneric[string](cache)
	ctx := context.Background()

	// Set a value
	value := "to be deleted"
	metadata := &daramjwee.Metadata{ETag: "v1"}
	err = stringCache.Set(ctx, "delete-me", value, metadata)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete it
	err = stringCache.Delete(ctx, "delete-me")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Try to get it - should call fetcher
	var fetcherCalled int32
	fetcher := GenericFetcher[string](func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
		atomic.StoreInt32(&fetcherCalled, 1)
		return "from fetcher", &daramjwee.Metadata{ETag: "v2"}, nil
	})

	retrieved, err := stringCache.Get(ctx, "delete-me", fetcher)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}

	if atomic.LoadInt32(&fetcherCalled) != 1 {
		t.Error("Fetcher should have been called after delete")
	}

	if retrieved != "from fetcher" {
		t.Errorf("Expected 'from fetcher', got %q", retrieved)
	}
}

func TestGenericCache_Config(t *testing.T) {
	cache, err := createTestCache()
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	configCache := NewGeneric[Config](cache)
	ctx := context.Background()

	config := Config{
		AppName: "TestApp",
		Debug:   true,
		Port:    8080,
	}
	metadata := &daramjwee.Metadata{ETag: "v1"}

	err = configCache.Set(ctx, "config", config, metadata)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	fetcher := GenericFetcher[Config](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (Config, *daramjwee.Metadata, error) {
		return config, metadata, nil
	})

	retrieved, err := configCache.Get(ctx, "config", fetcher)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved != config {
		t.Errorf("Expected %+v, got %+v", config, retrieved)
	}
}
