// Filename: options_test.go
package daramjwee

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Store for Testing ---
// Simple Mock Store used only within this test file.
type optionsTestMockStore struct{}

func (s *optionsTestMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}
func (s *optionsTestMockStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	return nil, nil
}
func (s *optionsTestMockStore) Delete(ctx context.Context, key string) error {
	return nil
}
func (s *optionsTestMockStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, nil
}

// --- Basic Validation Tests ---

func TestNew_OptionValidation(t *testing.T) {
	// Default HotStore for validation tests
	validHotStore := &optionsTestMockStore{}

	testCases := []struct {
		name        string
		options     []Option
		expectErr   bool
		expectedMsg string // Expected message part when error occurs
	}{
		// --- Happy Path (Success Cases) ---
		{
			name:      "Success with only mandatory hot store",
			options:   []Option{WithHotStore(validHotStore)},
			expectErr: false,
		},
		{
			name: "Success with all options valid",
			options: []Option{
				WithHotStore(validHotStore),
				WithColdStore(&optionsTestMockStore{}),
				WithWorker("pool", 10, 100, 5*time.Second),
				WithDefaultTimeout(10 * time.Second),
				WithShutdownTimeout(20 * time.Second), // **Modified**: Added ShutdownTimeout test
				WithCache(1 * time.Minute),            // **Modified**: WithGracePeriod -> WithCache
				WithNegativeCache(5 * time.Minute),
			},
			expectErr: false,
		},
		{
			name: "Success with positive cache TTL of zero", // **Modified**: 0 is a valid value, so changed to success case
			options: []Option{
				WithHotStore(validHotStore),
				WithCache(0),
			},
			expectErr: false,
		},
		{
			name: "Success with negative cache TTL of zero",
			options: []Option{
				WithHotStore(validHotStore),
				WithNegativeCache(0),
			},
			expectErr: false,
		},

		// --- Failure Cases ---
		{
			name:        "Failure without any options",
			options:     []Option{},
			expectErr:   true,
			expectedMsg: "either WithStores or WithHotStore must be provided",
		},
		{
			name:        "Failure with nil HotStore",
			options:     []Option{WithHotStore(nil)},
			expectErr:   true,
			expectedMsg: "hot store cannot be nil",
		},
		{
			name: "Failure with empty worker strategy",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("", 10, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker strategy type cannot be empty",
		},
		{
			name: "Failure with zero worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 0, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker pool size must be positive",
		},
		{
			name: "Failure with negative worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", -5, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker pool size must be positive",
		},
		{
			name: "Failure with zero worker job timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 10, 100, 0),
			},
			expectErr:   true,
			expectedMsg: "worker job timeout must be positive",
		},
		{
			name: "Failure with zero default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "default timeout must be positive",
		},
		{
			name: "Failure with negative default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(-5 * time.Second),
			},
			expectErr:   true,
			expectedMsg: "default timeout must be positive",
		},
		{
			name: "Failure with zero shutdown timeout", // **Added**: ShutdownTimeout failure case
			options: []Option{
				WithHotStore(validHotStore),
				WithShutdownTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "Shutdown timeout must be positive",
		},
		{
			name: "Failure with negative shutdown timeout", // **Added**: ShutdownTimeout failure case
			options: []Option{
				WithHotStore(validHotStore),
				WithShutdownTimeout(-10 * time.Second),
			},
			expectErr:   true,
			expectedMsg: "Shutdown timeout must be positive",
		},
		{
			name: "Failure with negative value for positive cache", // **Modified**: WithGracePeriod -> WithCache
			options: []Option{
				WithHotStore(validHotStore),
				WithCache(-1 * time.Minute),
			},
			expectErr:   true,
			expectedMsg: "positive cache TTL cannot be a negative value",
		},
		{
			name: "Failure with negative value for negative cache",
			options: []Option{
				WithHotStore(validHotStore),
				WithNegativeCache(-1 * time.Second),
			},
			expectErr:   true,
			expectedMsg: "negative cache TTL cannot be a negative value",
		},
		{
			name: "Success with buffer pool enabled",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPool(true, 32*1024),
			},
			expectErr: false,
		},
		{
			name: "Success with buffer pool disabled",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPool(false, 32*1024),
			},
			expectErr: false,
		},
		{
			name: "Failure with zero buffer pool default size",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPool(true, 0),
			},
			expectErr:   true,
			expectedMsg: "buffer pool default size must be positive",
		},
		{
			name: "Failure with negative buffer pool default size",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPool(true, -1024),
			},
			expectErr:   true,
			expectedMsg: "buffer pool default size must be positive",
		},
		{
			name: "Success with advanced buffer pool configuration",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 32 * 1024,
					MaxBufferSize:     64 * 1024,
					MinBufferSize:     4 * 1024,
				}),
			},
			expectErr: false,
		},
		{
			name: "Failure with advanced buffer pool - zero default size",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 0,
					MaxBufferSize:     64 * 1024,
					MinBufferSize:     4 * 1024,
				}),
			},
			expectErr:   true,
			expectedMsg: "buffer pool default size must be positive",
		},
		{
			name: "Failure with advanced buffer pool - zero min size",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 32 * 1024,
					MaxBufferSize:     64 * 1024,
					MinBufferSize:     0,
				}),
			},
			expectErr:   true,
			expectedMsg: "buffer pool minimum size must be positive",
		},
		{
			name: "Failure with advanced buffer pool - zero max size",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 32 * 1024,
					MaxBufferSize:     0,
					MinBufferSize:     4 * 1024,
				}),
			},
			expectErr:   true,
			expectedMsg: "buffer pool maximum size must be positive",
		},
		{
			name: "Failure with advanced buffer pool - min > default",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 16 * 1024,
					MaxBufferSize:     64 * 1024,
					MinBufferSize:     32 * 1024,
				}),
			},
			expectErr:   true,
			expectedMsg: "buffer pool minimum size cannot be larger than default size",
		},
		{
			name: "Failure with advanced buffer pool - default > max",
			options: []Option{
				WithHotStore(validHotStore),
				WithBufferPoolAdvanced(BufferPoolConfig{
					Enabled:           true,
					DefaultBufferSize: 64 * 1024,
					MaxBufferSize:     32 * 1024,
					MinBufferSize:     4 * 1024,
				}),
			},
			expectErr:   true,
			expectedMsg: "buffer pool default size cannot be larger than maximum size",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := New(nil, tc.options...)

			if tc.expectErr {
				require.Error(t, err, "Expected an error for invalid option")
				assert.Contains(t, err.Error(), tc.expectedMsg, "Error message mismatch")
				assert.Nil(t, cache, "Cache should be nil on creation failure")
			} else {
				require.NoError(t, err, "Expected no error for valid options")
				assert.NotNil(t, cache, "Cache should not be nil on successful creation")
				// Clean up resources on success.
				cache.Close()
			}
		})
	}
}

// --- Edge Case Tests ---

// TestNew_OptionOverrides verifies that when the same option is provided multiple times,
// the last provided option is applied.
func TestNew_OptionOverrides(t *testing.T) {
	validHotStore := &optionsTestMockStore{}
	finalTimeout := 15 * time.Second
	finalFreshFor := 10 * time.Minute // **Modified**: Variable name and value changed

	options := []Option{
		WithHotStore(validHotStore),
		WithDefaultTimeout(5 * time.Second), // Initial value
		WithCache(1 * time.Minute),          // **Modified**: WithGracePeriod -> WithCache
		WithDefaultTimeout(finalTimeout),    // Final value
		WithCache(finalFreshFor),            // **Modified**: WithGracePeriod -> WithCache
	}

	cache, err := New(nil, options...)
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.Close()

	// Type assert the Cache interface to the actual implementation DaramjweeCache
	// to check internal configuration values.
	dCache, ok := cache.(*DaramjweeCache)
	require.True(t, ok, "Failed to assert cache to *DaramjweeCache")

	assert.Equal(t, finalTimeout, dCache.DefaultTimeout, "The last DefaultTimeout option should be applied")
	assert.Equal(t, finalFreshFor, dCache.PositiveFreshFor, "The last WithCache option should be applied") // **Modified**: Verification field changed
}

// TestNew_NilColdStoreIsValid verifies that passing nil as ColdStore is valid,
// and the cache is properly configured with a single store.
func TestNew_NilColdStoreIsValid(t *testing.T) {
	validHotStore := &optionsTestMockStore{}

	options := []Option{
		WithHotStore(validHotStore),
		WithColdStore(nil), // Explicitly set nil ColdStore
	}

	cache, err := New(nil, options...)
	require.NoError(t, err, "Providing a nil ColdStore should be valid")
	require.NotNil(t, cache)
	defer cache.Close()

	// Verify that the cache has only one store (the HotStore)
	dCache, ok := cache.(*DaramjweeCache)
	require.True(t, ok)

	// With the new architecture, HotStore + nil ColdStore should result in single store
	assert.Len(t, dCache.Stores, 1, "Cache should have only one store when ColdStore is nil")
	assert.NotNil(t, dCache.Stores[0], "Primary store should not be nil")
}
