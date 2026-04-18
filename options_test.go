package daramjwee

import (
	"context"
	"io"
	"strings"
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

type optionsTestNoopFetcher struct{}

func (optionsTestNoopFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return &FetchResult{
		Body:     io.NopCloser(strings.NewReader("noop")),
		Metadata: &Metadata{CacheTag: "noop"},
	}, nil
}

type optionsTestBlockingFetcher struct {
	started chan struct{}
	release chan struct{}
}

func (f optionsTestBlockingFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	close(f.started)
	<-f.release
	return &FetchResult{
		Body:     io.NopCloser(strings.NewReader("block")),
		Metadata: &Metadata{CacheTag: "block"},
	}, nil
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
			name: "failure with group-attached cache weight",
			options: []Option{
				WithTiers(regularA),
				WithWeight(2),
			},
			expectErr:   true,
			expectedMsg: "group-attached cache option",
		},
		{
			name: "failure with group-attached cache queue limit",
			options: []Option{
				WithTiers(regularA),
				WithQueueLimit(8),
			},
			expectErr:   true,
			expectedMsg: "group-attached cache option",
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

func TestNewGroup_OptionValidation(t *testing.T) {
	group, err := NewGroup(nil,
		WithGroupWorkers(2),
		WithGroupWorkerTimeout(5*time.Second),
		WithGroupWorkerQueueDefault(8),
		WithGroupCloseTimeout(10*time.Second),
	)
	require.NoError(t, err)
	require.NotNil(t, group)

	cache, err := group.NewCache("shared", WithTiers(&optionsTestMockStore{id: 1}), WithWeight(3), WithQueueLimit(11))
	require.NoError(t, err)
	require.NotNil(t, cache)

	defaultCache, err := group.NewCache("shared-default", WithTiers(&optionsTestMockStore{id: 2}), WithWeight(2))
	require.NoError(t, err)
	require.NotNil(t, defaultCache)
	cache.Close()
	defaultCache.Close()
	group.Close()
}

func TestNewGroup_RejectsEmptyAndDuplicateNames(t *testing.T) {
	group, err := NewGroup(nil, WithGroupWorkers(1))
	require.NoError(t, err)
	defer group.Close()

	_, err = group.NewCache("", WithTiers(&optionsTestMockStore{id: 1}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache name cannot be empty")

	_, err = group.NewCache("dup", WithTiers(&optionsTestMockStore{id: 1}))
	require.NoError(t, err)

	_, err = group.NewCache("dup", WithTiers(&optionsTestMockStore{id: 1}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate cache name")
}

func TestGroupNewCache_RejectsStandaloneWorkerOptions(t *testing.T) {
	group, err := NewGroup(nil, WithGroupWorkers(1))
	require.NoError(t, err)
	defer group.Close()

	testCases := []struct {
		name string
		opt  Option
		msg  string
	}{
		{name: "WithWorkers", opt: WithWorkers(2), msg: "standalone worker option"},
		{name: "WithWorkerQueue", opt: WithWorkerQueue(8), msg: "standalone worker option"},
		{name: "WithWorkerTimeout", opt: WithWorkerTimeout(5 * time.Second), msg: "standalone worker option"},
		{name: "WithWorkerStrategy", opt: WithWorkerStrategy("pool"), msg: "standalone worker option"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := group.NewCache("bad", WithTiers(&optionsTestMockStore{id: 1}), tc.opt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}
}

func TestNewGroup_CloseClosesCreatedCaches(t *testing.T) {
	group, err := NewGroup(nil, WithGroupWorkers(1), WithGroupCloseTimeout(2*time.Second))
	require.NoError(t, err)

	cache, err := group.NewCache("closable", WithTiers(&optionsTestMockStore{id: 1}))
	require.NoError(t, err)

	group.Close()

	_, err = cache.Get(context.Background(), "k", GetRequest{}, optionsTestNoopFetcher{})
	require.ErrorIs(t, err, ErrCacheClosed)
}

func TestNewGroup_ReusesClosedCacheName(t *testing.T) {
	group, err := NewGroup(nil, WithGroupWorkers(1))
	require.NoError(t, err)
	defer group.Close()

	first, err := group.NewCache("reusable", WithTiers(&optionsTestMockStore{id: 1}))
	require.NoError(t, err)

	first.Close()

	second, err := group.NewCache("reusable", WithTiers(&optionsTestMockStore{id: 1}))
	require.NoError(t, err)
	defer second.Close()
}

func TestNewGroup_CloseTimeoutAllowsCacheNameReuse(t *testing.T) {
	group, err := NewGroup(nil, WithGroupWorkers(1), WithGroupCloseTimeout(10*time.Millisecond))
	require.NoError(t, err)

	cache, err := group.NewCache("tombstone", WithTiers(&optionsTestMockStore{id: 1}), WithCloseTimeout(10*time.Millisecond))
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		group.Close()
	})

	require.NoError(t, cache.ScheduleRefresh(context.Background(), "k", optionsTestBlockingFetcher{
		started: started,
		release: release,
	}))
	<-started

	done := make(chan struct{})
	go func() {
		cache.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for cache close to return")
	}

	reused, err := group.NewCache("tombstone", WithTiers(&optionsTestMockStore{id: 1}))
	require.NoError(t, err)
	defer reused.Close()
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

func TestNew_WithUnknownWorkerStrategyFails(t *testing.T) {
	cache, err := New(nil,
		WithTiers(&optionsTestMockStore{id: 1}),
		WithWorkerStrategy("bogus"),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown worker strategy")
	assert.Nil(t, cache)
}

func TestNew_WithManualNegativeTierFreshnessOverrideFails(t *testing.T) {
	cache, err := New(nil,
		WithTiers(&optionsTestMockStore{id: 1}),
		func(cfg *Config) error {
			cfg.TierFreshnessOverrides = map[int]TierFreshnessOverride{
				-1: {Positive: time.Second, Negative: time.Second},
			}
			return nil
		},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "tier freshness override index -1 is out of range")
	assert.Nil(t, cache)
}
