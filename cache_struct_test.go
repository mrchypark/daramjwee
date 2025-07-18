package daramjwee

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDaramjweeCacheStructInitialization(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("single store configuration", func(t *testing.T) {
		cache, err := New(logger, WithStores(newMockStore()))
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()

		daramjweeCache := cache.(*DaramjweeCache)
		assert.Len(t, daramjweeCache.Stores, 1)
		assert.NotNil(t, daramjweeCache.Stores[0])
	})

	t.Run("multi-tier store configuration", func(t *testing.T) {
		cache, err := New(logger, WithStores(newMockStore(), newMockStore(), newMockStore()))
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()

		daramjweeCache := cache.(*DaramjweeCache)
		assert.Len(t, daramjweeCache.Stores, 3)
		for i, store := range daramjweeCache.Stores {
			assert.NotNil(t, store, "Store at index %d should not be nil", i)
		}
	})

	t.Run("legacy HotStore configuration", func(t *testing.T) {
		cache, err := New(logger, WithHotStore(newMockStore()))
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()

		daramjweeCache := cache.(*DaramjweeCache)
		assert.Len(t, daramjweeCache.Stores, 1)
		assert.NotNil(t, daramjweeCache.Stores[0])
	})

	t.Run("legacy HotStore + ColdStore configuration", func(t *testing.T) {
		cache, err := New(logger, WithHotStore(newMockStore()), WithColdStore(newMockStore()))
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()

		daramjweeCache := cache.(*DaramjweeCache)
		assert.Len(t, daramjweeCache.Stores, 2)
		assert.NotNil(t, daramjweeCache.Stores[0])
		assert.NotNil(t, daramjweeCache.Stores[1])
	})

	t.Run("no stores configured", func(t *testing.T) {
		cache, err := New(logger)
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "either WithStores or WithHotStore must be provided")
	})
}
