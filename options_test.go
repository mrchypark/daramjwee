package daramjwee

import (
	"context"
	"io"
	"testing"
	"time"

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
				WithTierFreshness(-time.Second, time.Second),
			},
			expectErr:   true,
			expectedMsg: "tier positive cache TTL cannot be a negative value",
		},
		{
			name: "failure with negative per-tier positive freshness",
			options: []Option{
				WithTiers(regularA),
				WithTierPositiveFreshness(1, -time.Second),
			},
			expectErr:   true,
			expectedMsg: "tier positive cache TTL cannot be a negative value",
		},
		{
			name: "failure with negative per-tier negative freshness",
			options: []Option{
				WithTiers(regularA),
				WithTierNegativeFreshness(1, -time.Second),
			},
			expectErr:   true,
			expectedMsg: "tier negative cache TTL cannot be a negative value",
		},
		{
			name: "success with worker and freshness options",
			options: []Option{
				WithTiers(regularA, durable),
				WithWorker("pool", 10, 100, 5*time.Second),
				WithDefaultTimeout(10 * time.Second),
				WithShutdownTimeout(20 * time.Second),
				WithCache(time.Minute),
				WithNegativeCache(5 * time.Minute),
				WithTierFreshness(2*time.Minute, 3*time.Minute),
			},
			expectErr: false,
		},
		{
			name: "failure with empty worker strategy",
			options: []Option{
				WithTiers(regularA),
				WithWorker("", 10, 100, 5*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker strategy type cannot be empty",
		},
		{
			name: "failure with non-positive worker pool size",
			options: []Option{
				WithTiers(regularA),
				WithWorker("pool", 0, 100, 5*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker pool size must be positive",
		},
		{
			name: "failure with non-positive worker timeout",
			options: []Option{
				WithTiers(regularA),
				WithWorker("pool", 1, 100, 0),
			},
			expectErr:   true,
			expectedMsg: "worker job timeout must be positive",
		},
		{
			name: "failure with non-positive default timeout",
			options: []Option{
				WithTiers(regularA),
				WithDefaultTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "default timeout must be positive",
		},
		{
			name: "failure with non-positive shutdown timeout",
			options: []Option{
				WithTiers(regularA),
				WithShutdownTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "Shutdown timeout must be positive",
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

	require.NoError(t, WithDefaultTimeout(5*time.Second)(&cfg))
	require.NoError(t, WithDefaultTimeout(15*time.Second)(&cfg))
	require.NoError(t, WithTierFreshness(10*time.Minute, 20*time.Minute)(&cfg))
	require.NoError(t, WithCache(30*time.Minute)(&cfg))
	require.NoError(t, WithNegativeCache(40*time.Minute)(&cfg))
	require.NoError(t, WithTierPositiveFreshness(1, 50*time.Minute)(&cfg))
	require.NoError(t, WithTierNegativeFreshness(2, 60*time.Minute)(&cfg))

	assert.Equal(t, 15*time.Second, cfg.DefaultTimeout)
	assert.Equal(t, 30*time.Minute, cfg.TierPositiveFreshFor)
	assert.Equal(t, 40*time.Minute, cfg.TierNegativeFreshFor)
	require.Len(t, cfg.TierFreshnessOverrides, 2)
	require.NotNil(t, cfg.TierFreshnessOverrides[1].Positive)
	require.NotNil(t, cfg.TierFreshnessOverrides[2].Negative)
	assert.Equal(t, 50*time.Minute, *cfg.TierFreshnessOverrides[1].Positive)
	assert.Equal(t, 60*time.Minute, *cfg.TierFreshnessOverrides[2].Negative)
}
