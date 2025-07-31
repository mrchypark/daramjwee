package unit

import (
	"context"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStoreForOptions creates a simple mock store for testing options
func newMockStoreForOptions() daramjwee.Store {
	store, _ := memstore.New(memstore.Config{
		MaxSize: 1024 * 1024, // 1MB
	})
	return store
}

func TestWithStoresOptions(t *testing.T) {
	t.Run("WithHotStore option", func(t *testing.T) {
		hotStore := newMockStoreForOptions()

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(hotStore),
		)

		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("WithColdStore option", func(t *testing.T) {
		hotStore := newMockStoreForOptions()
		coldStore := newMockStoreForOptions()

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithColdStore(coldStore),
		)

		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("nil store should cause error", func(t *testing.T) {
		_, err := daramjwee.New(
			daramjwee.WithHotStore(nil),
		)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "store cannot be nil")
	})

	t.Run("no stores should cause error", func(t *testing.T) {
		_, err := daramjwee.New()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one store must be configured")
	})
}

func TestStoreConfiguration(t *testing.T) {
	t.Run("single hot store configuration", func(t *testing.T) {
		hotStore := newMockStoreForOptions()

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(hotStore),
		)

		require.NoError(t, err)
		require.NotNil(t, cache)

		// Test basic functionality
		ctx := context.Background()
		key := "test-key"

		// Should return not found initially
		_, err = cache.GetStream(ctx, key)
		assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	})

	t.Run("hot and cold store configuration", func(t *testing.T) {
		hotStore := newMockStore()
		coldStore := newMockStore()

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithColdStore(coldStore),
		)

		require.NoError(t, err)
		require.NotNil(t, cache)

		// Test basic functionality
		ctx := context.Background()
		key := "test-key"

		// Should return not found initially
		_, err = cache.GetStream(ctx, key)
		assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	})
}
