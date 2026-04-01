package daramjwee_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nonComparableTierStore struct {
	inner *mockStore
	tag   []byte
}

func newNonComparableTierStore() nonComparableTierStore {
	return nonComparableTierStore{
		inner: newMockStore(),
		tag:   []byte("tier"),
	}
}

func (s nonComparableTierStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return s.inner.GetStream(ctx, key)
}

func (s nonComparableTierStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return s.inner.BeginSet(ctx, key, metadata)
}

func (s nonComparableTierStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s nonComparableTierStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.inner.Stat(ctx, key)
}

func TestCache_LowerTierHitSynchronouslyFillsTopAndAsyncBackfillsIntermediate(t *testing.T) {
	top := newMockStore()
	mid := newSlowSetStore(150 * time.Millisecond)
	low := newMockStore()
	low.setData("tier-key", "tier-value", &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, low),
		daramjwee.WithTierFreshness(time.Hour, time.Hour),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "tier-key", &mockFetcher{})
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "tier-value", string(body))
	require.NoError(t, stream.Close())

	topReader, _, err := top.GetStream(context.Background(), "tier-key")
	require.NoError(t, err)
	_ = topReader.Close()

	_, _, err = mid.GetStream(context.Background(), "tier-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.Eventually(t, func() bool {
		reader, _, err := mid.GetStream(context.Background(), "tier-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_LowestTierHitSynchronouslyFillsTopAndAsyncBackfillsRemainingTiers(t *testing.T) {
	top := newMockStore()
	mid := newSlowSetStore(150 * time.Millisecond)
	lowest := newMockStore()
	lowest.setData("lowest-key", "lowest-value", &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lowest),
		daramjwee.WithTierFreshness(time.Hour, time.Hour),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "lowest-key", &mockFetcher{})
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "lowest-value", string(body))
	require.NoError(t, stream.Close())

	topReader, _, err := top.GetStream(context.Background(), "lowest-key")
	require.NoError(t, err)
	_ = topReader.Close()

	_, _, err = mid.GetStream(context.Background(), "lowest-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.Eventually(t, func() bool {
		reader, _, err := mid.GetStream(context.Background(), "lowest-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_LowerTierStaleHitServesStaleWithoutPromotingAndSchedulesRefresh(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("stale-lower-key", "stale-value", &daramjwee.Metadata{
		ETag:     "old",
		CachedAt: time.Now().Add(-time.Hour),
	})
	fetcher := &mockFetcher{content: "fresh-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithTierFreshness(time.Second, time.Second),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "stale-lower-key", fetcher)
	require.NoError(t, err)

	_, _, err = top.GetStream(context.Background(), "stale-lower-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "stale-value", string(body))

	require.NoError(t, stream.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := top.GetStream(context.Background(), "stale-lower-key")
		if err != nil {
			return false
		}
		defer reader.Close()

		readBody, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(readBody) == "fresh-value" && meta.ETag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	lastOldMetadata := fetcher.getLastOldMetadata()
	require.NotNil(t, lastOldMetadata)
	assert.Equal(t, "old", lastOldMetadata.ETag)
}

func TestCache_LowerTierStaleHitNotModifiedPromotesToTopAndStopsRevalidation(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("stale-lower-304-key", "stale-value", &daramjwee.Metadata{
		ETag:     "old",
		CachedAt: oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithTierFreshness(time.Second, time.Second),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "stale-lower-304-key", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "stale-value", string(body))

	require.Eventually(t, func() bool {
		reader, meta, err := top.GetStream(context.Background(), "stale-lower-304-key")
		if err != nil {
			return false
		}
		defer reader.Close()

		persisted, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(persisted) == "stale-value" && meta.ETag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	lastOldMetadata := fetcher.getLastOldMetadata()
	require.NotNil(t, lastOldMetadata)
	assert.Equal(t, "old", lastOldMetadata.ETag)

	fetchCount := fetcher.getFetchCount()
	stream, err = cache.Get(context.Background(), "stale-lower-304-key", fetcher)
	require.NoError(t, err)
	body, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "stale-value", string(body))

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, fetchCount, fetcher.getFetchCount())
}

func TestCache_LowerTierNegativeStaleHitNotModifiedPromotesNegativeToTop(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("stale-negative-304-key", "", &daramjwee.Metadata{
		ETag:       "old-neg",
		IsNegative: true,
		CachedAt:   oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithTierFreshness(time.Second, time.Second),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	_, err = cache.Get(context.Background(), "stale-negative-304-key", fetcher)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.Eventually(t, func() bool {
		meta, err := top.Stat(context.Background(), "stale-negative-304-key")
		if err != nil {
			return false
		}
		return meta.IsNegative && meta.ETag == "old-neg" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	fetchCount := fetcher.getFetchCount()
	_, err = cache.Get(context.Background(), "stale-negative-304-key", fetcher)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, fetchCount, fetcher.getFetchCount())
}

func TestCache_LowerTierHitWithZeroCachedAtSchedulesRefresh(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("legacy-key", "legacy-value", &daramjwee.Metadata{ETag: "legacy"})
	fetcher := &mockFetcher{content: "fresh-value", etag: "fresh"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithTierFreshness(time.Second, time.Second),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "legacy-key", fetcher)
	require.NoError(t, err)

	_, _, err = top.GetStream(context.Background(), "legacy-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "legacy-value", string(body))

	require.NoError(t, stream.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := top.GetStream(context.Background(), "legacy-key")
		if err != nil {
			return false
		}
		defer reader.Close()

		persisted, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(persisted) == "fresh-value" && meta.ETag == "fresh" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_LowerTierNegativeHitReturnsNotFoundAndPromotesNegativeToTop(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("negative-key", "", &daramjwee.Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithTierFreshness(time.Hour, time.Hour),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "negative-key", &mockFetcher{})
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	require.Nil(t, stream)

	reader, meta, err := top.GetStream(context.Background(), "negative-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.True(t, meta.IsNegative)
}

func TestCache_Set_WritesToSingleTierConfig(t *testing.T) {
	tier := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "single-tier-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write([]byte("single-tier-value"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := tier.GetStream(context.Background(), "single-tier-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "single-tier-value", string(body))
	assert.Equal(t, "v1", meta.ETag)
}

func TestCache_AbortPreventsFanoutAfterNonTopHit(t *testing.T) {
	testCases := []struct {
		name        string
		buildCache  func(top, mid, lower *mockStore) (daramjwee.Cache, error)
		expectedKey string
	}{
		{
			name: "lower-tier-hit",
			buildCache: func(top, mid, lower *mockStore) (daramjwee.Cache, error) {
				lower.setData("fanout-key", "fanout-value", &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})
				return daramjwee.New(
					nil,
					daramjwee.WithTiers(top, mid, lower),
					daramjwee.WithTierFreshness(time.Hour, time.Hour),
					daramjwee.WithDefaultTimeout(2*time.Second),
				)
			},
			expectedKey: "fanout-key",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			top := newMockStore()
			mid := newSlowSetStore(150 * time.Millisecond)
			lower := newMockStore()

			cache, err := tc.buildCache(top, mid.mockStore, lower)
			require.NoError(t, err)
			defer cache.Close()

			stream, err := cache.Get(context.Background(), tc.expectedKey, &mockFetcher{})
			require.NoError(t, err)

			buf := make([]byte, 4)
			_, err = stream.Read(buf)
			require.NoError(t, err)
			require.NoError(t, stream.Close())

			_, _, err = top.GetStream(context.Background(), tc.expectedKey)
			require.ErrorIs(t, err, daramjwee.ErrNotFound)

			_, _, err = mid.GetStream(context.Background(), tc.expectedKey)
			require.ErrorIs(t, err, daramjwee.ErrNotFound)

			select {
			case key := <-top.writeCompleted:
				t.Fatalf("unexpected top-tier publish for %s", key)
			case key := <-mid.writeCompleted:
				t.Fatalf("unexpected fan-out publish for %s", key)
			case <-time.After(250 * time.Millisecond):
			}
		})
	}
}

func TestCache_MissSynchronouslyFillsTopAndAsyncBackfillsRemainingTiers(t *testing.T) {
	top := newMockStore()
	mid := newSlowSetStore(150 * time.Millisecond)
	lowest := newSlowSetStore(150 * time.Millisecond)
	fetcher := &mockFetcher{content: "miss-value", etag: "v1"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lowest),
		daramjwee.WithTierFreshness(time.Hour, time.Hour),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-fanout-key", fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "miss-value", string(body))
	require.NoError(t, stream.Close())

	topReader, _, err := top.GetStream(context.Background(), "miss-fanout-key")
	require.NoError(t, err)
	_ = topReader.Close()

	_, _, err = mid.GetStream(context.Background(), "miss-fanout-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, _, err = lowest.GetStream(context.Background(), "miss-fanout-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	require.Eventually(t, func() bool {
		reader, _, err := mid.GetStream(context.Background(), "miss-fanout-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		reader, _, err := lowest.GetStream(context.Background(), "miss-fanout-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_FanoutHandlesNonComparableTierStores(t *testing.T) {
	top := newNonComparableTierStore()
	mid := newNonComparableTierStore()
	lower := newMockStore()
	lower.setData("fanout-noncomparable", "value", &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lower),
		daramjwee.WithTierFreshness(time.Hour, time.Hour),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	require.NotPanics(t, func() {
		stream, err := cache.Get(context.Background(), "fanout-noncomparable", &mockFetcher{})
		require.NoError(t, err)

		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.Equal(t, "value", string(body))
		require.NoError(t, stream.Close())
	})

	require.Eventually(t, func() bool {
		reader, _, err := mid.GetStream(context.Background(), "fanout-noncomparable")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierMissPublishesToOnlyTier(t *testing.T) {
	tier := newMockStore()
	fetcher := &mockFetcher{content: "single-tier-body", etag: "v1"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-miss", fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "single-tier-body", string(body))

	reader, meta, err := tier.GetStream(context.Background(), "single-tier-miss")
	require.NoError(t, err)
	defer reader.Close()

	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "single-tier-body", string(persisted))
	assert.Equal(t, "v1", meta.ETag)
}

func TestCache_SingleTierStaleHitRefreshesOnlyTier(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-stale", "old-value", &daramjwee.Metadata{
		ETag:     "old",
		CachedAt: time.Now().Add(-time.Hour),
	})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithTierFreshness(time.Second, 0),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-stale", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	require.Eventually(t, func() bool {
		reader, meta, err := tier.GetStream(context.Background(), "single-tier-stale")
		if err != nil {
			return false
		}
		defer reader.Close()

		readBody, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(readBody) == "new-value" && meta.ETag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierStaleNotModifiedRefreshesCachedAt(t *testing.T) {
	tier := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	tier.setData("single-tier-stale-304", "old-value", &daramjwee.Metadata{
		ETag:     "old",
		CachedAt: oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithTierFreshness(time.Second, 0),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-stale-304", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	require.Eventually(t, func() bool {
		meta, err := tier.Stat(context.Background(), "single-tier-stale-304")
		if err != nil {
			return false
		}
		return meta.ETag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	fetchCount := fetcher.getFetchCount()
	stream, err = cache.Get(context.Background(), "single-tier-stale-304", fetcher)
	require.NoError(t, err)
	body, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, fetchCount, fetcher.getFetchCount())
}

func TestCache_SingleTierFilestoreStaleNotModifiedRefreshesWithoutSelfDeadlock(t *testing.T) {
	dir := t.TempDir()
	tier, err := filestore.New(dir, log.NewNopLogger())
	require.NoError(t, err)

	oldCachedAt := time.Now().Add(-time.Hour)
	writer, err := tier.BeginSet(context.Background(), "filestore-stale-304", &daramjwee.Metadata{
		ETag:     "old",
		CachedAt: oldCachedAt,
	})
	require.NoError(t, err)
	_, err = writer.Write([]byte("old-value"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithTierFreshness(time.Second, 0),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "filestore-stale-304", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	require.Eventually(t, func() bool {
		meta, err := tier.Stat(context.Background(), "filestore-stale-304")
		if err != nil {
			return false
		}
		return meta.ETag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierZeroCachedAtHitRefreshesOnlyTier(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-legacy", "old-value", &daramjwee.Metadata{ETag: "old"})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithTierFreshness(time.Second, 0),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-legacy", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	require.Eventually(t, func() bool {
		reader, meta, err := tier.GetStream(context.Background(), "single-tier-legacy")
		if err != nil {
			return false
		}
		defer reader.Close()

		readBody, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(readBody) == "new-value" && meta.ETag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierZeroCachedAtIsStaleWhenFreshnessIsZero(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-immediate-stale", "old-value", &daramjwee.Metadata{ETag: "old"})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithTierFreshness(0, 0),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-immediate-stale", fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "old-value", string(body))

	require.Eventually(t, func() bool {
		reader, meta, err := tier.GetStream(context.Background(), "single-tier-immediate-stale")
		if err != nil {
			return false
		}
		defer reader.Close()

		readBody, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(readBody) == "new-value" && meta.ETag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_DeleteRemovesFromAllOrderedTiers(t *testing.T) {
	top := newMockStore()
	mid := newMockStore()
	lowest := newMockStore()
	meta := &daramjwee.Metadata{ETag: "v1", CachedAt: time.Now()}
	top.setData("delete-all", "top", meta)
	mid.setData("delete-all", "mid", meta)
	lowest.setData("delete-all", "lowest", meta)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lowest),
		daramjwee.WithDefaultTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	require.NoError(t, cache.Delete(context.Background(), "delete-all"))

	_, _, err = top.GetStream(context.Background(), "delete-all")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, _, err = mid.GetStream(context.Background(), "delete-all")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
	_, _, err = lowest.GetStream(context.Background(), "delete-all")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

type slowSetStore struct {
	*mockStore
	delay time.Duration
}

func newSlowSetStore(delay time.Duration) *slowSetStore {
	return &slowSetStore{
		mockStore: newMockStore(),
		delay:     delay,
	}
}

func (s *slowSetStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	sink, err := s.mockStore.BeginSet(ctx, key, metadata)
	if err != nil {
		return nil, err
	}
	return &slowSetSink{WriteSink: sink, delay: s.delay}, nil
}

type slowSetSink struct {
	daramjwee.WriteSink
	delay time.Duration
}

func (s *slowSetSink) Close() error {
	time.Sleep(s.delay)
	return s.WriteSink.Close()
}
