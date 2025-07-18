package daramjwee

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Use existing mockStore from cache_test.go

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		setupConfig func() *Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid new configuration with WithStores",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithStores(newMockStore(), newMockStore())
				opt(cfg)
				return cfg
			},
			expectError: false,
		},
		{
			name: "valid legacy configuration with HotStore only",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithHotStore(newMockStore())
				opt(cfg)
				return cfg
			},
			expectError: false,
		},
		{
			name: "valid legacy configuration with HotStore and ColdStore",
			setupConfig: func() *Config {
				cfg := &Config{}
				hotOpt := WithHotStore(newMockStore())
				coldOpt := WithColdStore(newMockStore())
				hotOpt(cfg)
				coldOpt(cfg)
				return cfg
			},
			expectError: false,
		},
		{
			name: "error: empty stores slice",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithStores()
				err := opt(cfg)
				if err != nil {
					// Option validation failed, but we still need to test cfg.validate()
					// Set up a config that will fail validation
					return &Config{}
				}
				return cfg
			},
			expectError: true,
			errorMsg:    "either WithStores or WithHotStore must be provided",
		},
		{
			name: "error: nil store in WithStores",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithStores(newMockStore(), nil)
				err := opt(cfg)
				if err != nil {
					// Option validation failed, but we still need to test cfg.validate()
					// Set up a config that will fail validation
					return &Config{}
				}
				return cfg
			},
			expectError: true,
			errorMsg:    "either WithStores or WithHotStore must be provided",
		},
		{
			name: "error: mixing WithStores with WithHotStore in validate",
			setupConfig: func() *Config {
				// Manually create a config that has both fields set
				// to test the validate() method's mixed configuration detection
				cfg := &Config{
					HotStore: newMockStore(),
					Stores:   []Store{newMockStore()},
				}
				return cfg
			},
			expectError: true,
			errorMsg:    "cannot mix WithStores with WithHotStore or WithColdStore options",
		},
		{
			name: "error: no stores configured",
			setupConfig: func() *Config {
				return &Config{} // Empty config
			},
			expectError: true,
			errorMsg:    "either WithStores or WithHotStore must be provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setupConfig()
			err := cfg.validate()

			if tt.expectError {
				require.Error(t, err)
				var configErr *ConfigError
				require.ErrorAs(t, err, &configErr)
				assert.Contains(t, configErr.Message, tt.errorMsg)
			} else {
				require.NoError(t, err)
				// Verify that Stores slice is properly set
				assert.NotEmpty(t, cfg.Stores)
				// Verify that legacy fields are cleared after validation
				assert.Nil(t, cfg.HotStore)
				assert.Nil(t, cfg.ColdStore)
			}
		})
	}
}

func TestConfigValidationConversion(t *testing.T) {
	tests := []struct {
		name           string
		setupConfig    func() *Config
		expectedStores int
	}{
		{
			name: "convert HotStore only to single-tier Stores",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithHotStore(newMockStore())
				opt(cfg)
				return cfg
			},
			expectedStores: 1,
		},
		{
			name: "convert HotStore + ColdStore to two-tier Stores",
			setupConfig: func() *Config {
				cfg := &Config{}
				hotOpt := WithHotStore(newMockStore())
				coldOpt := WithColdStore(newMockStore())
				hotOpt(cfg)
				coldOpt(cfg)
				return cfg
			},
			expectedStores: 2,
		},
		{
			name: "preserve WithStores configuration",
			setupConfig: func() *Config {
				cfg := &Config{}
				opt := WithStores(
					newMockStore(),
					newMockStore(),
					newMockStore(),
				)
				opt(cfg)
				return cfg
			},
			expectedStores: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setupConfig()
			err := cfg.validate()
			require.NoError(t, err)

			assert.Len(t, cfg.Stores, tt.expectedStores)
			assert.Nil(t, cfg.HotStore, "HotStore should be cleared after validation")
			assert.Nil(t, cfg.ColdStore, "ColdStore should be cleared after validation")

			// Verify all stores are non-nil
			for i, store := range cfg.Stores {
				assert.NotNil(t, store, "Store at index %d should not be nil", i)
			}
		})
	}
}

func TestWithStoresValidation(t *testing.T) {
	tests := []struct {
		name        string
		stores      []Store
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid single store",
			stores:      []Store{newMockStore()},
			expectError: false,
		},
		{
			name:        "valid multiple stores",
			stores:      []Store{newMockStore(), newMockStore()},
			expectError: false,
		},
		{
			name:        "error: empty stores",
			stores:      []Store{},
			expectError: true,
			errorMsg:    "at least one store must be provided",
		},
		{
			name:        "error: nil store",
			stores:      []Store{newMockStore(), nil},
			expectError: true,
			errorMsg:    "store at index 1 cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			opt := WithStores(tt.stores...)
			err := opt(cfg)

			if tt.expectError {
				require.Error(t, err)
				var configErr *ConfigError
				require.ErrorAs(t, err, &configErr)
				assert.Contains(t, configErr.Message, tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.stores, cfg.Stores)
			}
		})
	}
}

func TestLegacyConfigurationConflicts(t *testing.T) {
	t.Run("WithHotStore conflicts with WithStores", func(t *testing.T) {
		cfg := &Config{}

		// First set WithStores
		storesOpt := WithStores(newMockStore())
		err := storesOpt(cfg)
		require.NoError(t, err)

		// Then try to set WithHotStore - should fail
		hotOpt := WithHotStore(newMockStore())
		err = hotOpt(cfg)
		require.Error(t, err)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithHotStore with WithStores")
	})

	t.Run("WithColdStore conflicts with WithStores", func(t *testing.T) {
		cfg := &Config{}

		// First set WithStores
		storesOpt := WithStores(newMockStore())
		err := storesOpt(cfg)
		require.NoError(t, err)

		// Then try to set WithColdStore - should fail
		coldOpt := WithColdStore(newMockStore())
		err = coldOpt(cfg)
		require.Error(t, err)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithColdStore with WithStores")
	})

	t.Run("WithStores conflicts with existing legacy config", func(t *testing.T) {
		cfg := &Config{}

		// First set legacy config
		hotOpt := WithHotStore(newMockStore())
		err := hotOpt(cfg)
		require.NoError(t, err)

		// Then try to set WithStores - should fail
		storesOpt := WithStores(newMockStore())
		err = storesOpt(cfg)
		require.Error(t, err)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore or WithColdStore")
	})
}
