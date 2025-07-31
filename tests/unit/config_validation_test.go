package unit

import (
	"github.com/mrchypark/daramjwee"
	"testing"
	"time"

	"github.com/go-kit/log"
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
				opt := daramjwee.WithHotStore(newMockStore())
				opt(cfg)
				return cfg
			},
			expectError: false,
		},
		{
			name: "valid legacy configuration with HotStore and ColdStore",
			setupConfig: func() *Config {
				cfg := &Config{}
				hotOpt := daramjwee.WithHotStore(newMockStore())
				coldOpt := daramjwee.WithColdStore(newMockStore())
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
					// daramjwee.Option validation failed, but we still need to test cfg.validate()
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
					// daramjwee.Option validation failed, but we still need to test cfg.validate()
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
					Stores:   []daramjwee.Store{newMockStore()},
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
				opt := daramjwee.WithHotStore(newMockStore())
				opt(cfg)
				return cfg
			},
			expectedStores: 1,
		},
		{
			name: "convert HotStore + ColdStore to two-tier Stores",
			setupConfig: func() *Config {
				cfg := &Config{}
				hotOpt := daramjwee.WithHotStore(newMockStore())
				coldOpt := daramjwee.WithColdStore(newMockStore())
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
				assert.NotNil(t, store, "daramjwee.Store at index %d should not be nil", i)
			}
		})
	}
}

func TestStoresValidation(t *testing.T) {
	tests := []struct {
		name        string
		stores      []Store
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid single store",
			stores:      []daramjwee.Store{newMockStore()},
			expectError: false,
		},
		{
			name:        "valid multiple stores",
			stores:      []daramjwee.Store{newMockStore(), newMockStore()},
			expectError: false,
		},
		{
			name:        "error: empty stores",
			stores:      []daramjwee.Store{},
			expectError: true,
			errorMsg:    "at least one store must be provided",
		},
		{
			name:        "error: nil store",
			stores:      []daramjwee.Store{newMockStore(), nil},
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
		hotOpt := daramjwee.WithHotStore(newMockStore())
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
		coldOpt := daramjwee.WithColdStore(newMockStore())
		err = coldOpt(cfg)
		require.Error(t, err)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithColdStore with WithStores")
	})

	t.Run("WithStores conflicts with existing legacy config", func(t *testing.T) {
		cfg := &Config{}

		// First set legacy config
		hotOpt := daramjwee.WithHotStore(newMockStore())
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

// TestConfigValidationErrorMessages tests that error messages are clear and actionable
func TestConfigValidationErrorMessages(t *testing.T) {
	tests := []struct {
		name           string
		setupConfig    func() *Config
		expectedErrMsg string
	}{
		{
			name: "clear error for missing stores",
			setupConfig: func() *Config {
				return &Config{} // Empty config
			},
			expectedErrMsg: "either WithStores or WithHotStore must be provided",
		},
		{
			name: "clear error for mixed configuration in validate",
			setupConfig: func() *Config {
				return &Config{
					HotStore: newMockStore(),
					Stores:   []daramjwee.Store{newMockStore()},
				}
			},
			expectedErrMsg: "cannot mix WithStores with WithHotStore or WithColdStore options",
		},
		{
			name: "clear error for nil store with specific index",
			setupConfig: func() *Config {
				return &Config{
					Stores: []daramjwee.Store{newMockStore(), nil, newMockStore()},
				}
			},
			expectedErrMsg: "store at index 1 cannot be nil",
		},
		{
			name: "clear error for empty stores slice",
			setupConfig: func() *Config {
				return &Config{
					Stores: []daramjwee.Store{},
				}
			},
			expectedErrMsg: "at least one store must be configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setupConfig()
			err := cfg.validate()
			require.Error(t, err)

			var configErr *ConfigError
			require.ErrorAs(t, err, &configErr)
			assert.Equal(t, tt.expectedErrMsg, configErr.Message)

			// Verify the full error message format
			expectedFullMsg := "daramjwee: configuration error: " + tt.expectedErrMsg
			assert.Equal(t, expectedFullMsg, configErr.Error())
		})
	}
}

// TestConfigValidationStoreSliceIntegrity tests store slice integrity validation
func TestStoreSliceIntegrity(t *testing.T) {
	t.Run("validates all stores are non-nil after conversion", func(t *testing.T) {
		cfg := &Config{}
		hotOpt := daramjwee.WithHotStore(newMockStore())
		coldOpt := daramjwee.WithColdStore(newMockStore())

		err := hotOpt(cfg)
		require.NoError(t, err)
		err = coldOpt(cfg)
		require.NoError(t, err)

		// Before validation, legacy fields should be set
		assert.NotNil(t, cfg.HotStore)
		assert.NotNil(t, cfg.ColdStore)
		assert.Nil(t, cfg.Stores)

		// After validation, Stores should be set and legacy fields cleared
		err = cfg.validate()
		require.NoError(t, err)

		assert.Len(t, cfg.Stores, 2)
		assert.NotNil(t, cfg.Stores[0])
		assert.NotNil(t, cfg.Stores[1])
		assert.Nil(t, cfg.HotStore)
		assert.Nil(t, cfg.ColdStore)
	})

	t.Run("preserves store order during conversion", func(t *testing.T) {
		hotStore := newMockStore()
		coldStore := newMockStore()

		cfg := &Config{}
		hotOpt := daramjwee.WithHotStore(hotStore)
		coldOpt := daramjwee.WithColdStore(coldStore)

		err := hotOpt(cfg)
		require.NoError(t, err)
		err = coldOpt(cfg)
		require.NoError(t, err)

		err = cfg.validate()
		require.NoError(t, err)

		// Verify order: HotStore becomes stores[0], ColdStore becomes stores[1]
		assert.Equal(t, hotStore, cfg.Stores[0])
		assert.Equal(t, coldStore, cfg.Stores[1])
	})

	t.Run("handles single store conversion correctly", func(t *testing.T) {
		hotStore := newMockStore()

		cfg := &Config{}
		hotOpt := daramjwee.WithHotStore(hotStore)

		err := hotOpt(cfg)
		require.NoError(t, err)

		err = cfg.validate()
		require.NoError(t, err)

		// Should have single store in slice
		assert.Len(t, cfg.Stores, 1)
		assert.Equal(t, hotStore, cfg.Stores[0])
		assert.Nil(t, cfg.HotStore)
		assert.Nil(t, cfg.ColdStore)
	})
}

// TestNewCacheWithValidation tests that the New function properly validates configuration
func TestCacheWithValidation(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("successful cache creation with new configuration", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			WithStores(newMockStore(), newMockStore()),
			WithDefaultTimeout(10*time.Second),
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("successful cache creation with legacy configuration", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			daramjwee.WithHotStore(newMockStore()),
			daramjwee.WithColdStore(newMockStore()),
			WithDefaultTimeout(10*time.Second),
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("successful cache creation with single store", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			daramjwee.WithHotStore(newMockStore()),
			WithDefaultTimeout(10*time.Second),
		)
		require.NoError(t, err)
		require.NotNil(t, cache)
		defer cache.Close()
	})

	t.Run("error: no stores configured", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			WithDefaultTimeout(10*time.Second),
		)
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "either WithStores or WithHotStore must be provided")
	})

	t.Run("error: mixed configuration options", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			daramjwee.WithHotStore(newMockStore()),
			WithStores(newMockStore()),
		)
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore or WithColdStore")
	})

	t.Run("error: nil store in WithStores", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			WithStores(newMockStore(), nil),
		)
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "store at index 1 cannot be nil")
	})

	t.Run("error: empty stores slice", func(t *testing.T) {
		cache, err := daramjwee.New(logger,
			WithStores(),
		)
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "at least one store must be provided")
	})
}
