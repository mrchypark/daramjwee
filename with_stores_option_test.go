package daramjwee

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithStoresOptionErrors tests errors that occur in the WithStores option function itself
func TestWithStoresOptionErrors(t *testing.T) {
	t.Run("error: empty stores slice", func(t *testing.T) {
		cfg := &Config{}
		opt := WithStores()
		err := opt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "at least one store must be provided")
	})

	t.Run("error: nil store in WithStores", func(t *testing.T) {
		cfg := &Config{}
		opt := WithStores(newMockStore(), nil)
		err := opt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "store at index 1 cannot be nil")
	})

	t.Run("error: WithStores conflicts with existing HotStore", func(t *testing.T) {
		cfg := &Config{}

		// First set HotStore
		hotOpt := WithHotStore(newMockStore())
		err := hotOpt(cfg)
		require.NoError(t, err)

		// Then try WithStores - should fail
		storesOpt := WithStores(newMockStore())
		err = storesOpt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore or WithColdStore")
	})

	t.Run("error: WithStores conflicts with existing ColdStore", func(t *testing.T) {
		cfg := &Config{}

		// First set ColdStore
		coldOpt := WithColdStore(newMockStore())
		err := coldOpt(cfg)
		require.NoError(t, err)

		// Then try WithStores - should fail
		storesOpt := WithStores(newMockStore())
		err = storesOpt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore or WithColdStore")
	})
}

// TestLegacyOptionErrors tests errors that occur in legacy option functions
func TestLegacyOptionErrors(t *testing.T) {
	t.Run("error: WithHotStore conflicts with existing Stores", func(t *testing.T) {
		cfg := &Config{}

		// First set Stores
		storesOpt := WithStores(newMockStore())
		err := storesOpt(cfg)
		require.NoError(t, err)

		// Then try WithHotStore - should fail
		hotOpt := WithHotStore(newMockStore())
		err = hotOpt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithHotStore with WithStores")
	})

	t.Run("error: WithColdStore conflicts with existing Stores", func(t *testing.T) {
		cfg := &Config{}

		// First set Stores
		storesOpt := WithStores(newMockStore())
		err := storesOpt(cfg)
		require.NoError(t, err)

		// Then try WithColdStore - should fail
		coldOpt := WithColdStore(newMockStore())
		err = coldOpt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithColdStore with WithStores")
	})

	t.Run("error: nil HotStore", func(t *testing.T) {
		cfg := &Config{}
		opt := WithHotStore(nil)
		err := opt(cfg)

		require.Error(t, err)
		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "hot store cannot be nil")
	})
}
