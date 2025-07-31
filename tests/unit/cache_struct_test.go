package unit

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheCreation(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("single store configuration", func(t *testing.T) {
		store := memstore.New(1024*1024, nil) // 1MB capacity, no eviction policy

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(store),
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("multi-tier store configuration", func(t *testing.T) {
		hotStore := memstore.New(512*1024, nil)     // 512KB capacity
		coldStore := memstore.New(2*1024*1024, nil) // 2MB capacity

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(hotStore),
			daramjwee.WithColdStore(coldStore),
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("cache with buffer pool", func(t *testing.T) {
		store := memstore.New(1024*1024, nil) // 1MB capacity

		cache, err := daramjwee.New(
			daramjwee.WithHotStore(store),
			daramjwee.WithBufferPool(true, 32*1024), // enabled, 32KB default size
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})
}

func TestCacheConfigurationErrors(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("no stores configured", func(t *testing.T) {
		_, err := daramjwee.New(
			daramjwee.WithLogger(logger),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one store must be configured")
	})

	t.Run("nil store", func(t *testing.T) {
		_, err := daramjwee.New(
			daramjwee.WithHotStore(nil),
			daramjwee.WithLogger(logger),
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store cannot be nil")
	})
}
