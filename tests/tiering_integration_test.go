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
	low.setData("tier-key", "tier-value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, low),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "tier-key", daramjwee.GetRequest{}, &mockFetcher{})
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
	lowest.setData("lowest-key", "lowest-value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lowest),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "lowest-key", daramjwee.GetRequest{}, &mockFetcher{})
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
		CacheTag: "old",
		CachedAt: time.Now().Add(-time.Hour),
	})
	fetcher := &mockFetcher{content: "fresh-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "stale-lower-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	_, _, err = top.GetStream(context.Background(), "stale-lower-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "stale-value", string(body))

	require.NoError(t, stream.Close())

	_, _, err = top.GetStream(context.Background(), "stale-lower-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

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
		return string(readBody) == "fresh-value" && meta.CacheTag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	lastOldMetadata := fetcher.getLastOldMetadata()
	require.NotNil(t, lastOldMetadata)
	assert.Equal(t, "old", lastOldMetadata.CacheTag)
}

func TestCache_LowerTierStaleHitNotModifiedPromotesToTopAndStopsRevalidation(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("stale-lower-304-key", "stale-value", &daramjwee.Metadata{
		CacheTag: "old",
		CachedAt: oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "stale-lower-304-key", daramjwee.GetRequest{}, fetcher)
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
		return string(persisted) == "stale-value" && meta.CacheTag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	lastOldMetadata := fetcher.getLastOldMetadata()
	require.NotNil(t, lastOldMetadata)
	assert.Equal(t, "old", lastOldMetadata.CacheTag)

	fetchCount := fetcher.getFetchCount()
	stream, err = cache.Get(context.Background(), "stale-lower-304-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "stale-value", string(body))

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, fetchCount, fetcher.getFetchCount())
}

func TestCache_ConditionalStaleHitReturnsNotModifiedAndRefreshesWithoutClose(t *testing.T) {
	top := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	top.setData("conditional-stale-key", "stale-value", &daramjwee.Metadata{
		CacheTag: "cache-v1",
		CachedAt: oldCachedAt,
	})
	fetcher := &mockFetcher{content: "fresh-value", etag: "cache-v2"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "conditional-stale-key", daramjwee.GetRequest{IfNoneMatch: "cache-v1"}, fetcher)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, daramjwee.GetStatusNotModified, resp.Status)
	assert.Nil(t, resp.Body)
	assert.Equal(t, "cache-v1", resp.Metadata.CacheTag)

	require.Eventually(t, func() bool {
		reader, meta, err := top.GetStream(context.Background(), "conditional-stale-key")
		if err != nil {
			return false
		}
		defer reader.Close()

		body, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return string(body) == "fresh-value" && meta.CacheTag == "cache-v2" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_ConditionalHitAcceptsHTTPIfNoneMatchForms(t *testing.T) {
	tests := []struct {
		name        string
		ifNoneMatch string
		cacheTag    string
	}{
		{name: "quoted", ifNoneMatch: "\"cache-v1\"", cacheTag: "cache-v1"},
		{name: "weak and csv", ifNoneMatch: "\"other\", W/\"cache-v1\"", cacheTag: "cache-v1"},
		{name: "wildcard", ifNoneMatch: "*", cacheTag: "cache-v1"},
		{name: "quoted comma", ifNoneMatch: "\"rev,42\"", cacheTag: "rev,42"},
		{name: "wildcard with empty cache tag", ifNoneMatch: "*", cacheTag: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			top := newMockStore()
			top.setData("conditional-header-key", "cached-value", &daramjwee.Metadata{
				CacheTag: tt.cacheTag,
				CachedAt: time.Now(),
			})

			cache, err := daramjwee.New(
				nil,
				daramjwee.WithTiers(top),
				daramjwee.WithFreshness(time.Hour, time.Hour),
				daramjwee.WithOpTimeout(2*time.Second),
			)
			require.NoError(t, err)
			defer cache.Close()

			fetcher := &mockFetcher{}
			resp, err := cache.Get(context.Background(), "conditional-header-key", daramjwee.GetRequest{IfNoneMatch: tt.ifNoneMatch}, fetcher)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, daramjwee.GetStatusNotModified, resp.Status)
			require.Nil(t, resp.Body)
			assert.Equal(t, tt.cacheTag, resp.Metadata.CacheTag)
			assert.Equal(t, 0, fetcher.getFetchCount())
		})
	}
}

func TestCache_LowerTierConditionalHitPromotesFreshEntryToTop(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	cachedAt := time.Now()
	lower.setData("conditional-lower-key", "cached-value", &daramjwee.Metadata{
		CacheTag: "cache-v1",
		CachedAt: cachedAt,
	})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "conditional-lower-key", daramjwee.GetRequest{IfNoneMatch: "cache-v1"}, &mockFetcher{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, daramjwee.GetStatusNotModified, resp.Status)
	require.Nil(t, resp.Body)

	reader, meta, err := top.GetStream(context.Background(), "conditional-lower-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "cached-value", string(body))
	assert.Equal(t, "cache-v1", meta.CacheTag)
	assert.Equal(t, cachedAt.UnixNano(), meta.CachedAt.UnixNano())
}

func TestCache_LowerTierNegativeStaleHitNotModifiedPromotesNegativeToTop(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("stale-negative-304-key", "", &daramjwee.Metadata{
		CacheTag:   "old-neg",
		IsNegative: true,
		CachedAt:   oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "stale-negative-304-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	require.True(t, resp.Metadata.IsNegative)

	require.Eventually(t, func() bool {
		meta, err := top.Stat(context.Background(), "stale-negative-304-key")
		if err != nil {
			return false
		}
		return meta.IsNegative && meta.CacheTag == "old-neg" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	fetchCount := fetcher.getFetchCount()
	resp, err = cache.Get(context.Background(), "stale-negative-304-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, fetchCount, fetcher.getFetchCount())
}

func TestCache_LowerTierHitWithZeroCachedAtSchedulesRefresh(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("legacy-key", "legacy-value", &daramjwee.Metadata{CacheTag: "legacy"})
	fetcher := &mockFetcher{content: "fresh-value", etag: "fresh"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "legacy-key", daramjwee.GetRequest{}, fetcher)
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
		return string(persisted) == "fresh-value" && meta.CacheTag == "fresh" && fetcher.getFetchCount() > 0
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
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "negative-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	require.True(t, resp.Metadata.IsNegative)
	require.Nil(t, resp.Body)

	reader, meta, err := top.GetStream(context.Background(), "negative-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.True(t, meta.IsNegative)
}

func TestCache_Get_LowerTierPositiveFreshnessOverride(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("override-positive-key", "lower-value", &daramjwee.Metadata{
		CacheTag: "lower-etag",
		CachedAt: time.Now(),
	})
	fetcher := &mockFetcher{content: "origin-value", etag: "origin-etag"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(0, 0),
		daramjwee.WithTierFreshness(1, time.Hour, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "override-positive-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	assert.Equal(t, "lower-value", string(body))

	reader, meta, err := top.GetStream(context.Background(), "override-positive-key")
	require.NoError(t, err)
	defer reader.Close()

	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(persisted))
	assert.Equal(t, "lower-etag", meta.CacheTag)
	assert.Zero(t, fetcher.getFetchCount())
}

func TestCache_Get_LowerTierNegativeFreshnessOverride(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("override-negative-key", "", &daramjwee.Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	})
	fetcher := &mockFetcher{content: "origin-value", etag: "origin-etag"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(0, 0),
		daramjwee.WithTierFreshness(1, 0, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "override-negative-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	require.True(t, resp.Metadata.IsNegative)
	require.Nil(t, resp.Body)

	reader, meta, err := top.GetStream(context.Background(), "override-negative-key")
	require.NoError(t, err)
	defer reader.Close()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.True(t, meta.IsNegative)
	assert.Zero(t, fetcher.getFetchCount())
}

func TestCache_Set_WritesToSingleTierConfig(t *testing.T) {
	tier := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "single-tier-key", &daramjwee.Metadata{CacheTag: "v1"})
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
	assert.Equal(t, "v1", meta.CacheTag)
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
				lower.setData("fanout-key", "fanout-value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})
				return daramjwee.New(
					nil,
					daramjwee.WithTiers(top, mid, lower),
					daramjwee.WithFreshness(time.Hour, time.Hour),
					daramjwee.WithOpTimeout(2*time.Second),
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

			stream, err := cache.Get(context.Background(), tc.expectedKey, daramjwee.GetRequest{}, &mockFetcher{})
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
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-fanout-key", daramjwee.GetRequest{}, fetcher)
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
	lower.setData("fanout-noncomparable", "value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	require.NotPanics(t, func() {
		stream, err := cache.Get(context.Background(), "fanout-noncomparable", daramjwee.GetRequest{}, &mockFetcher{})
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
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-miss", daramjwee.GetRequest{}, fetcher)
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
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestCache_SingleTierStaleHitRefreshesOnlyTier(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-stale", "old-value", &daramjwee.Metadata{
		CacheTag: "old",
		CachedAt: time.Now().Add(-time.Hour),
	})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithFreshness(time.Second, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-stale", daramjwee.GetRequest{}, fetcher)
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
		return string(readBody) == "new-value" && meta.CacheTag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierStaleNotModifiedRefreshesCachedAt(t *testing.T) {
	tier := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	tier.setData("single-tier-stale-304", "old-value", &daramjwee.Metadata{
		CacheTag: "old",
		CachedAt: oldCachedAt,
	})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithFreshness(time.Second, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-stale-304", daramjwee.GetRequest{}, fetcher)
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
		return meta.CacheTag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)

	fetchCount := fetcher.getFetchCount()
	stream, err = cache.Get(context.Background(), "single-tier-stale-304", daramjwee.GetRequest{}, fetcher)
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
		CacheTag: "old",
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
		daramjwee.WithFreshness(time.Second, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "filestore-stale-304", daramjwee.GetRequest{}, fetcher)
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
		return meta.CacheTag == "old" && meta.CachedAt.After(oldCachedAt)
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierZeroCachedAtHitRefreshesOnlyTier(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-legacy", "old-value", &daramjwee.Metadata{CacheTag: "old"})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithFreshness(time.Second, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-legacy", daramjwee.GetRequest{}, fetcher)
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
		return string(readBody) == "new-value" && meta.CacheTag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_SingleTierZeroCachedAtIsStaleWhenFreshnessIsZero(t *testing.T) {
	tier := newMockStore()
	tier.setData("single-tier-immediate-stale", "old-value", &daramjwee.Metadata{CacheTag: "old"})
	fetcher := &mockFetcher{content: "new-value", etag: "new"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(tier),
		daramjwee.WithFreshness(0, 0),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "single-tier-immediate-stale", daramjwee.GetRequest{}, fetcher)
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
		return string(readBody) == "new-value" && meta.CacheTag == "new" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_DeleteRemovesFromAllOrderedTiers(t *testing.T) {
	top := newMockStore()
	mid := newMockStore()
	lowest := newMockStore()
	meta := &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()}
	top.setData("delete-all", "top", meta)
	mid.setData("delete-all", "mid", meta)
	lowest.setData("delete-all", "lowest", meta)

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, mid, lowest),
		daramjwee.WithOpTimeout(2*time.Second),
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
