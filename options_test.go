package daramjwee

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type optionsTestMockStore struct {
	id int
}

func (s *optionsTestMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *optionsTestMockStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &nullWriteSink{}, nil
}

func (s *optionsTestMockStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *optionsTestMockStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type nonComparableStore struct {
	data []byte
}

func (s nonComparableStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s nonComparableStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &nullWriteSink{}, nil
}

func (s nonComparableStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s nonComparableStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

type comparableValueStore struct {
	id int
}

func (s comparableValueStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s comparableValueStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &nullWriteSink{}, nil
}

func (s comparableValueStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s comparableValueStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

func TestNew_OptionValidation(t *testing.T) {
	regularA := &optionsTestMockStore{id: 1}
	regularB := &optionsTestMockStore{id: 2}
	durable := &optionsTestMockStore{id: 3}

	testCases := []struct {
		name        string
		options     []Option
		expectErr   bool
		expectedMsg string
	}{
		{
			name:      "success with single regular tier",
			options:   []Option{WithTiers(regularA)},
			expectErr: false,
		},
		{
			name:      "success with ordered tiers",
			options:   []Option{WithTiers(regularA, regularB, durable)},
			expectErr: false,
		},
		{
			name:      "success with single ordered tier",
			options:   []Option{WithTiers(durable)},
			expectErr: false,
		},
		{
			name:        "failure with empty config",
			options:     nil,
			expectErr:   true,
			expectedMsg: "at least one tier is required",
		},
		{
			name:        "failure with nil regular tier",
			options:     []Option{WithTiers(nil)},
			expectErr:   true,
			expectedMsg: "tier cannot be nil",
		},
		{
			name:        "failure with duplicate regular tier instance",
			options:     []Option{WithTiers(regularA, regularA)},
			expectErr:   true,
			expectedMsg: "duplicate tier store instance",
		},
		{
			name:      "success with non comparable tier store",
			options:   []Option{WithTiers(nonComparableStore{data: []byte("x")})},
			expectErr: false,
		},
		{
			name:      "success with equal comparable value stores in separate tiers",
			options:   []Option{WithTiers(comparableValueStore{id: 1}, comparableValueStore{id: 1})},
			expectErr: false,
		},
		{
			name: "failure with negative tier freshness",
			options: []Option{
				WithTiers(regularA),
				WithFreshness(-time.Second, time.Second),
			},
			expectErr:   true,
			expectedMsg: "positive freshness cannot be a negative value",
		},
		{
			name: "failure with negative per-tier freshness",
			options: []Option{
				WithTiers(regularA),
				WithTierFreshness(1, -time.Second, time.Second),
			},
			expectErr:   true,
			expectedMsg: "positive freshness cannot be a negative value",
		},
		{
			name: "failure with negative per-tier negative freshness",
			options: []Option{
				WithTiers(regularA),
				WithTierFreshness(1, time.Second, -time.Second),
			},
			expectErr:   true,
			expectedMsg: "negative freshness cannot be a negative value",
		},
		{
			name: "success with worker and freshness options",
			options: []Option{
				WithTiers(regularA, durable),
				WithWorkerStrategy("all"),
				WithWorkers(10),
				WithWorkerQueue(100),
				WithWorkerTimeout(5 * time.Second),
				WithOpTimeout(10 * time.Second),
				WithCloseTimeout(20 * time.Second),
				WithFreshness(time.Minute, 5*time.Minute),
				WithTierFreshness(1, 2*time.Minute, 3*time.Minute),
			},
			expectErr: false,
		},
		{
			name: "failure with empty worker strategy",
			options: []Option{
				WithTiers(regularA),
				WithWorkerStrategy(""),
			},
			expectErr:   true,
			expectedMsg: "worker strategy cannot be empty",
		},
		{
			name: "failure with non-positive workers",
			options: []Option{
				WithTiers(regularA),
				WithWorkers(0),
			},
			expectErr:   true,
			expectedMsg: "worker count must be positive",
		},
		{
			name: "failure with non-positive worker queue size",
			options: []Option{
				WithTiers(regularA),
				WithWorkerQueue(0),
			},
			expectErr:   true,
			expectedMsg: "worker queue size must be positive",
		},
		{
			name: "failure with non-positive worker timeout",
			options: []Option{
				WithTiers(regularA),
				WithWorkerTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "worker job timeout must be positive",
		},
		{
			name: "failure with non-positive op timeout",
			options: []Option{
				WithTiers(regularA),
				WithOpTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "operation timeout must be positive",
		},
		{
			name: "failure with non-positive close timeout",
			options: []Option{
				WithTiers(regularA),
				WithCloseTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "close timeout must be positive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := New(nil, tc.options...)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedMsg)
				assert.Nil(t, cache)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cache)
			cache.Close()
		})
	}
}

func TestOptionOverrides(t *testing.T) {
	cfg := Config{}

	require.NoError(t, WithWorkerStrategy("pool")(&cfg))
	require.NoError(t, WithWorkerStrategy("all")(&cfg))
	require.NoError(t, WithOpTimeout(5*time.Second)(&cfg))
	require.NoError(t, WithOpTimeout(15*time.Second)(&cfg))
	require.NoError(t, WithFreshness(10*time.Minute, 20*time.Minute)(&cfg))
	require.NoError(t, WithFreshness(30*time.Minute, 40*time.Minute)(&cfg))
	require.NoError(t, WithTierFreshness(1, 50*time.Minute, 20*time.Minute)(&cfg))
	require.NoError(t, WithTierFreshness(2, 30*time.Minute, 60*time.Minute)(&cfg))

	assert.Equal(t, "all", cfg.WorkerStrategy)
	assert.Equal(t, 15*time.Second, cfg.OpTimeout)
	assert.Equal(t, 30*time.Minute, cfg.PositiveFreshness)
	assert.Equal(t, 40*time.Minute, cfg.NegativeFreshness)
	require.Len(t, cfg.TierFreshnessOverrides, 2)
	assert.Equal(t, 50*time.Minute, cfg.TierFreshnessOverrides[1].Positive)
	assert.Equal(t, 20*time.Minute, cfg.TierFreshnessOverrides[1].Negative)
	assert.Equal(t, 30*time.Minute, cfg.TierFreshnessOverrides[2].Positive)
	assert.Equal(t, 60*time.Minute, cfg.TierFreshnessOverrides[2].Negative)
}

func TestNew_WithWorkerStrategyAllUsesAllStrategy(t *testing.T) {
	cache, err := New(nil,
		WithTiers(&optionsTestMockStore{id: 1}),
		WithWorkerStrategy("all"),
	)
	require.NoError(t, err)
	typedCache := cache.(*DaramjweeCache)
	t.Cleanup(typedCache.Close)

	strategyField := reflect.ValueOf(typedCache.worker).Elem().FieldByName("strategy")
	require.True(t, strategyField.IsValid())
	require.False(t, strategyField.IsNil())

	strategyType := strategyField.Elem().Type()
	assert.Equal(t, reflect.TypeOf((*worker.AllStrategy)(nil)), strategyType)
}
