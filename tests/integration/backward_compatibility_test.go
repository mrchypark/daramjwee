package integration

import (
	"github.com/mrchypark/daramjwee"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backwardCompatibilityFetcher is a basic fetcher implementation for testing
type backwardCompatibilityFetcher struct {
	data string
}

func (f *backwardCompatibilityFetcher) Fetch(ctx context.Context, metadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{},
	}, nil
}

// TestBackwardCompatibilityConfiguration tests that existing WithHotStore/WithColdStore
// configurations continue to work identically to the new WithStores configuration
func TestBackwardCompatibilityConfiguration(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("WithHotStore only - legacy vs new", func(t *testing.T) {
		hotStore := newMockStore()

		// Legacy configuration
		legacyCache, err := daramjwee.New(logger, daramjwee.WithHotStore(hotStore))
		require.NoError(t, err)
		defer legacyCache.Close()

		// New configuration equivalent
		newCache, err := daramjwee.New(logger, daramjwee.WithStores(hotStore))
		require.NoError(t, err)
		defer newCache.Close()

		// Both should have identical structure
		legacyDaramjwee := legacyCache.(*daramjwee.DaramjweeCache)
		newDaramjwee := newCache.(*daramjwee.DaramjweeCache)

		assert.Len(t, legacyDaramjwee.Stores, 1)
		assert.Len(t, newDaramjwee.Stores, 1)
		assert.Equal(t, legacyDaramjwee.Stores[0], newDaramjwee.Stores[0])
	})

	t.Run("WithHotStore + WithColdStore - legacy vs new", func(t *testing.T) {
		hotStore := newMockStore()
		coldStore := newMockStore()

		// Legacy configuration
		legacyCache, err := daramjwee.New(logger, daramjwee.WithHotStore(hotStore), daramjwee.WithColdStore(coldStore))
		require.NoError(t, err)
		defer legacyCache.Close()

		// New configuration equivalent
				newCache, err := daramjwee.New(logger, daramjwee.WithStores(hotStore, coldStore))
		require.NoError(t, err)
		defer newCache.Close()

		// Both should have identical structure
		legacyDaramjwee := legacyCache.(*daramjwee.DaramjweeCache)
		newDaramjwee := newCache.(*daramjwee.DaramjweeCache)

		assert.Len(t, legacyDaramjwee.Stores, 2)
		assert.Len(t, newDaramjwee.Stores, 2)
		assert.Equal(t, legacyDaramjwee.Stores[0], newDaramjwee.Stores[0])
		assert.Equal(t, legacyDaramjwee.Stores[1], newDaramjwee.Stores[1])
	})

	t.Run("WithColdStore without WithHotStore should fail", func(t *testing.T) {
		coldStore := newMockStore()

		cache, err := daramjwee.New(logger, daramjwee.WithColdStore(coldStore))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "either WithStores or WithHotStore must be provided")
	})
}

// TestBackwardCompatibilityBehavior tests that cache behavior is identical
// between legacy and new configurations
func TestBackwardCompatibilityBehavior(t *testing.T) {
	logger := log.NewNopLogger()

	testCases := []struct {
		name           string
		setupLegacy    func() (daramjwee.Cache, error)
		setupNew       func() (daramjwee.Cache, error)
		expectedStores int
	}{
		{
			name: "single tier behavior",
			setupLegacy: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()))
			},
			setupNew: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithStores(newMockStore()))
			},
			expectedStores: 1,
		},
		{
			name: "two tier behavior",
			setupLegacy: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()), daramjwee.WithColdStore(newMockStore()))
			},
			setupNew: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithStores(newMockStore(), newMockStore()))
			},
			expectedStores: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			legacyCache, err := tc.setupLegacy()
			require.NoError(t, err)
			defer legacyCache.Close()

			newCache, err := tc.setupNew()
			require.NoError(t, err)
			defer newCache.Close()

			// Verify both caches have the same structure
			legacyDaramjwee := legacyCache.(*daramjwee.DaramjweeCache)
			newDaramjwee := newCache.(*daramjwee.DaramjweeCache)

			assert.Len(t, legacyDaramjwee.Stores, tc.expectedStores)
			assert.Len(t, newDaramjwee.Stores, tc.expectedStores)

			// Test basic cache operations behave identically
			ctx := context.Background()
			key := "test-key"
			testData := "test data for backward compatibility"

			// Test Set operation
			legacyWriter, err := legacyCache.Set(ctx, key, nil)
			require.NoError(t, err)
			_, err = legacyWriter.Write([]byte(testData))
			require.NoError(t, err)
			err = legacyWriter.Close()
			require.NoError(t, err)

			newWriter, err := newCache.Set(ctx, key, nil)
			require.NoError(t, err)
			_, err = newWriter.Write([]byte(testData))
			require.NoError(t, err)
			err = newWriter.Close()
			require.NoError(t, err)

			// Test Get operation with a simple fetcher to avoid nil pointer issues
			simpleFetcher := &backwardCompatibilityFetcher{data: testData}

			legacyReader, err := legacyCache.Get(ctx, key, simpleFetcher)
			require.NoError(t, err)
			legacyData, err := io.ReadAll(legacyReader)
			require.NoError(t, err)
			err = legacyReader.Close()
			require.NoError(t, err)

			newReader, err := newCache.Get(ctx, key, simpleFetcher)
			require.NoError(t, err)
			newData, err := io.ReadAll(newReader)
			require.NoError(t, err)
			err = newReader.Close()
			require.NoError(t, err)

			// Data should be identical
			assert.Equal(t, string(legacyData), string(newData))
			assert.Equal(t, testData, string(legacyData))
			assert.Equal(t, testData, string(newData))

			// Test Delete operation
			err = legacyCache.Delete(ctx, key)
			require.NoError(t, err)

			err = newCache.Delete(ctx, key)
			require.NoError(t, err)

			// After deletion, both should behave identically (either return data from fetcher or error)
			legacyReader2, legacyErr2 := legacyCache.Get(ctx, key, simpleFetcher)
			newReader2, newErr2 := newCache.Get(ctx, key, simpleFetcher)

			// Both should have the same error status
			if legacyErr2 != nil && newErr2 != nil {
				// Both failed - this is acceptable
				assert.True(t, true, "Both caches failed consistently")
			} else if legacyErr2 == nil && newErr2 == nil {
				// Both succeeded - read and compare data
				legacyData2, err := io.ReadAll(legacyReader2)
				require.NoError(t, err)
				err = legacyReader2.Close()
				require.NoError(t, err)

				newData2, err := io.ReadAll(newReader2)
				require.NoError(t, err)
				err = newReader2.Close()
				require.NoError(t, err)

				assert.Equal(t, string(legacyData2), string(newData2))
			} else {
				// One succeeded, one failed - this indicates inconsistent behavior
				t.Errorf("Inconsistent behavior after deletion: legacy error=%v, new error=%v", legacyErr2, newErr2)
			}
		})
	}
}

// TestLegacyConfigurationMigration tests migration scenarios from legacy to new configuration
func TestLegacyConfigurationMigration(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("migrate from WithHotStore only", func(t *testing.T) {
		hotStore := newMockStore()

		// Original legacy configuration
		legacyCache, err := daramjwee.New(logger, daramjwee.WithHotStore(hotStore))
		require.NoError(t, err)
		defer legacyCache.Close()

		// Migrated configuration
		migratedCache, err := daramjwee.New(logger, daramjwee.WithStores(hotStore))
		require.NoError(t, err)
		defer migratedCache.Close()

		// Test that migration preserves functionality
		ctx := context.Background()
		key := "migration-test"
		testData := "migration test data"

		// Set data in legacy cache
		writer, err := legacyCache.Set(ctx, key, nil)
		require.NoError(t, err)
		_, err = writer.Write([]byte(testData))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		// Should be able to read from migrated cache (same store instance)
		simpleFetcher := &backwardCompatibilityFetcher{data: testData}
		reader, err := migratedCache.Get(ctx, key, simpleFetcher)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)

		assert.Equal(t, testData, string(data))
	})

	t.Run("migrate from WithHotStore + WithColdStore", func(t *testing.T) {
		hotStore := newMockStore()
		coldStore := newMockStore()

		// Original legacy configuration
		legacyCache, err := daramjwee.New(logger, daramjwee.WithHotStore(hotStore), daramjwee.WithColdStore(coldStore))
		require.NoError(t, err)
		defer legacyCache.Close()

		// Migrated configuration
		migratedCache, err := daramjwee.New(logger, daramjwee.WithStores(hotStore, coldStore))
		require.NoError(t, err)
		defer migratedCache.Close()

		// Test multi-tier behavior is preserved
		ctx := context.Background()
		key := "multi-tier-migration-test"
		testData := "multi-tier migration test data"

		// Set data in legacy cache
		writer, err := legacyCache.Set(ctx, key, nil)
		require.NoError(t, err)
		_, err = writer.Write([]byte(testData))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		// Should be able to read from migrated cache
		simpleFetcher := &backwardCompatibilityFetcher{data: testData}
		reader, err := migratedCache.Get(ctx, key, simpleFetcher)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)

		assert.Equal(t, testData, string(data))
	})
}

// TestLegacyConfigurationConflictPrevention tests that mixing legacy and new
// configuration options produces clear error messages
func TestLegacyConfigurationConflictPrevention(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("WithHotStore conflicts with WithStores", func(t *testing.T) {
		cache, err := daramjwee.New(logger, daramjwee.WithStores(newMockStore()), daramjwee.WithHotStore(newMockStore()))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore")
	})

	t.Run("WithColdStore conflicts with WithStores", func(t *testing.T) {
		cache, err := daramjwee.New(logger, daramjwee.WithColdStore(newMockStore()), daramjwee.WithStores(newMockStore()))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithStores with WithHotStore or WithColdStore")
	})

	t.Run("WithStores conflicts with existing WithHotStore", func(t *testing.T) {
		cache, err := daramjwee.New(logger, daramjwee.WithStores(newMockStore()), daramjwee.WithHotStore(newMockStore()))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "cannot use WithHotStore with WithStores")
	})
}

// TestLegacyConfigurationErrorMessages tests that legacy configuration errors
// provide helpful guidance for migration
func TestLegacyConfigurationErrorMessages(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("nil hot store error message", func(t *testing.T) {
		cache, err := daramjwee.New(logger, daramjwee.WithHotStore(nil))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "hot store cannot be nil")
	})

	t.Run("missing hot store error message", func(t *testing.T) {
		cache, err := daramjwee.New(logger, daramjwee.WithColdStore(newMockStore()))
		require.Error(t, err)
		require.Nil(t, cache)

		var configErr *daramjwee.ConfigError
		require.ErrorAs(t, err, &configErr)
		assert.Contains(t, configErr.Message, "either WithStores or WithHotStore must be provided")
	})
}

// BenchmarkBackwardCompatibilityPerformance ensures no performance regression
// between legacy and new configuration approaches
func BenchmarkBackwardCompatibilityPerformance(b *testing.B) {
	logger := log.NewNopLogger()

	// Test data sizes
	dataSizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"32KB", 32 * 1024},
		{"256KB", 256 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, dataSize := range dataSizes {
		b.Run(dataSize.name, func(b *testing.B) {
			b.Run("SingleTier", func(b *testing.B) {
				b.Run("Legacy_WithHotStore", func(b *testing.B) {
					benchmarkCacheOperations(b, logger, dataSize.size, func() (daramjwee.Cache, error) {
						return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()))
					})
				})

				b.Run("New_WithStores", func(b *testing.B) {
					benchmarkCacheOperations(b, logger, dataSize.size, func() (daramjwee.Cache, error) {
						return daramjwee.New(logger, daramjwee.WithStores(newMockStore()))
					})
				})
			})

			b.Run("TwoTier", func(b *testing.B) {
				b.Run("Legacy_WithHotColdStore", func(b *testing.B) {
					benchmarkCacheOperations(b, logger, dataSize.size, func() (daramjwee.Cache, error) {
						return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()), daramjwee.WithColdStore(newMockStore()))
					})
				})

				b.Run("New_WithStores", func(b *testing.B) {
					benchmarkCacheOperations(b, logger, dataSize.size, func() (daramjwee.Cache, error) {
						return daramjwee.New(logger, daramjwee.WithStores(newMockStore(), newMockStore()))
					})
				})
			})
		})
	}
}

// benchmarkCacheOperations performs standard cache operations for performance comparison
func benchmarkCacheOperations(b *testing.B, logger log.Logger, dataSize int, setupCache func() (daramjwee.Cache, error)) {
	cache, err := setupCache()
	if err != nil {
		b.Fatalf("Failed to setup cache: %v", err)
	}
	defer cache.Close()

	// Generate test data
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	ctx := context.Background()
	key := "benchmark-key"

	// Pre-populate cache for Get benchmarks
	writer, err := cache.Set(ctx, key, nil)
	if err != nil {
		b.Fatalf("Failed to set initial data: %v", err)
	}
	_, err = writer.Write(testData)
	if err != nil {
		b.Fatalf("Failed to write initial data: %v", err)
	}
	err = writer.Close()
	if err != nil {
		b.Fatalf("Failed to close initial writer: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark Get operations
	b.Run("Get", func(b *testing.B) {
		fetcher := &backwardCompatibilityFetcher{data: string(testData)}
		for i := 0; i < b.N; i++ {
			reader, err := cache.Get(ctx, key, fetcher)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			_, err = io.ReadAll(reader)
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
			err = reader.Close()
			if err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})

	// Benchmark Set operations
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			setKey := key + "-set-" + string(rune(i))
			writer, err := cache.Set(ctx, setKey, nil)
			if err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			_, err = writer.Write(testData)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			err = writer.Close()
			if err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})

	// Benchmark Delete operations
	b.Run("Delete", func(b *testing.B) {
		// Pre-populate keys for deletion
		numDeleteKeys := 100 // Limit the number of keys for deletion setup
		deleteKeys := make([]string, numDeleteKeys)
		for i := 0; i < numDeleteKeys; i++ {
			deleteKey := key + "-delete-" + string(rune(i))
			deleteKeys[i] = deleteKey
			writer, err := cache.Set(ctx, deleteKey, nil)
			if err != nil {
				b.Fatalf("Failed to setup delete key: %v", err)
			}
			_, err = writer.Write(testData)
			if err != nil {
				b.Fatalf("Failed to write delete data: %v", err)
			}
			err = writer.Close()
			if err != nil {
				b.Fatalf("Failed to close delete writer: %v", err)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := cache.Delete(ctx, deleteKeys[i%numDeleteKeys])
			if err != nil {
				b.Fatalf("Delete failed: %v", err)
			}
		}
	})
}

// BenchmarkBackwardCompatibilityMemoryUsage compares memory allocation patterns
// between legacy and new configurations
func BenchmarkBackwardCompatibilityMemoryUsage(b *testing.B) {
	logger := log.NewNopLogger()
	testData := make([]byte, 64*1024) // 64KB test data
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	configurations := []struct {
		name  string
		setup func() (daramjwee.Cache, error)
	}{
		{
			name: "Legacy_SingleTier",
			setup: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()))
			},
		},
		{
			name: "New_SingleTier",
			setup: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithStores(newMockStore()))
			},
		},
		{
			name: "Legacy_TwoTier",
			setup: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()), daramjwee.WithColdStore(newMockStore()))
			},
		},
		{
			name: "New_TwoTier",
			setup: func() (daramjwee.Cache, error) {
				return daramjwee.New(logger, daramjwee.WithStores(newMockStore(), newMockStore()))
			},
		},
	}

	for _, config := range configurations {
		b.Run(config.name, func(b *testing.B) {
			cache, err := config.setup()
			if err != nil {
				b.Fatalf("Failed to setup cache: %v", err)
			}
			defer cache.Close()

			ctx := context.Background()
			key := "memory-test-key"

			b.ResetTimer()
			b.ReportAllocs()

			fetcher := &backwardCompatibilityFetcher{data: string(testData)}
			for i := 0; i < b.N; i++ {
				// Set operation
				writer, err := cache.Set(ctx, key, nil)
				if err != nil {
					b.Fatalf("Set failed: %v", err)
				}
				_, err = writer.Write(testData)
				if err != nil {
					b.Fatalf("Write failed: %v", err)
				}
				err = writer.Close()
				if err != nil {
					b.Fatalf("Close failed: %v", err)
				}
				

				// Get operation
				reader, err := cache.Get(ctx, key, fetcher)
				if err != nil {
					b.Fatalf("Get failed: %v", err)
				}
				_, err = io.ReadAll(reader)
				if err != nil {
					b.Fatalf("Read failed: %v", err)
				}
				err = reader.Close()
				if err != nil {
					b.Fatalf("Close failed: %v", err)
				}
				
			}
		})
	}
}

// BenchmarkBackwardCompatibilityConcurrency tests concurrent performance
// to ensure no regression in multi-threaded scenarios
func BenchmarkBackwardCompatibilityConcurrency(b *testing.B) {
	logger := log.NewNopLogger()
	testData := make([]byte, 32*1024) // 32KB test data
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	concurrencyLevels := []int{1, 4, 8, 16}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(b *testing.B) {
			b.Run("Legacy_TwoTier", func(b *testing.B) {
				benchmarkBackwardCompatibilityConcurrentOperations(b, logger, testData, concurrency, func() (daramjwee.Cache, error) {
					return daramjwee.New(logger, daramjwee.WithHotStore(newMockStore()), daramjwee.WithColdStore(newMockStore()))
				})
			})

			b.Run("New_TwoTier", func(b *testing.B) {
				benchmarkBackwardCompatibilityConcurrentOperations(b, logger, testData, concurrency, func() (daramjwee.Cache, error) {
					return daramjwee.New(logger, daramjwee.WithStores(newMockStore(), newMockStore()))
				})
			})
		})
	}
}

// benchmarkBackwardCompatibilityConcurrentOperations performs concurrent cache operations
func benchmarkBackwardCompatibilityConcurrentOperations(b *testing.B, logger log.Logger, testData []byte, concurrency int, setupCache func() (daramjwee.Cache, error)) {
	cache, err := setupCache()
	if err != nil {
		b.Fatalf("Failed to setup cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		fetcher := &backwardCompatibilityFetcher{data: string(testData)}
		for pb.Next() {
			key := fmt.Sprintf("concurrent-key-%d", i)
			i++

			// Set operation
			writer, err := cache.Set(ctx, key, nil)
			if err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			_, err = writer.Write(testData)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
			err = writer.Close()
			if err != nil {
				b.Fatalf("Close failed: %v", err)
			}

			// Get operation
			reader, err := cache.Get(ctx, key, fetcher)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			_, err = io.ReadAll(reader)
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
			err = reader.Close()
			if err != nil {
				b.Fatalf("Close failed: %v", err)
			}
		}
	})
}
