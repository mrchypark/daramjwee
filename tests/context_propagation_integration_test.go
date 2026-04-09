package daramjwee_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextValueKey string

type valueAwareFetcher struct {
	contextKey any
	expected   any
	content    string
	etag       string
	sawValue   atomic.Bool
}

func (f *valueAwareFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	if ctx.Value(f.contextKey) == f.expected {
		f.sawValue.Store(true)
	}
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(f.content))),
		Metadata: &daramjwee.Metadata{CacheTag: f.etag},
	}, nil
}

type contextAwarePersistStore struct {
	*mockStore
	contextKey any
	expected   any
	sawValue   atomic.Bool
}

func (s *contextAwarePersistStore) BeginSetUsesContext() bool { return true }

func (s *contextAwarePersistStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if ctx.Value(s.contextKey) == s.expected {
		s.sawValue.Store(true)
	}
	return s.mockStore.BeginSet(ctx, key, metadata)
}

func TestScheduleRefresh_PreservesCallerContextValuesAfterCancel(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := contextValueKey("trace-id")
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "refresh-trace"))
	fetcher := &valueAwareFetcher{
		contextKey: key,
		expected:   "refresh-trace",
		content:    "refresh-value",
		etag:       "refresh-v1",
	}

	require.NoError(t, cache.ScheduleRefresh(ctx, "refresh-context-key", fetcher))
	cancel()

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), "refresh-context-key")
		if err != nil {
			return false
		}
		defer reader.Close()
		body, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return fetcher.sawValue.Load() && string(body) == "refresh-value" && meta.CacheTag == "refresh-v1"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_StaleTopCloseRefreshPreservesCallerContextValuesAfterCancel(t *testing.T) {
	hot := newMockStore()
	hot.setData("stale-top-context-key", "stale-value", &daramjwee.Metadata{
		CacheTag: "stale-v1",
		CachedAt: time.Now().Add(-time.Hour),
	})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	key := contextValueKey("trace-id")
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "stale-close-trace"))
	fetcher := &valueAwareFetcher{
		contextKey: key,
		expected:   "stale-close-trace",
		content:    "fresh-value",
		etag:       "fresh-v2",
	}

	resp, err := cache.Get(ctx, "stale-top-context-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "stale-value", string(body))

	cancel()
	require.NoError(t, resp.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := hot.GetStream(context.Background(), "stale-top-context-key")
		if err != nil {
			return false
		}
		defer reader.Close()
		refreshed, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return fetcher.sawValue.Load() && string(refreshed) == "fresh-value" && meta.CacheTag == "fresh-v2"
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_MissPersistPreservesCallerContextValuesAfterCancel(t *testing.T) {
	hot := newMockStore()
	cold := &contextAwarePersistStore{
		mockStore:  newMockStore(),
		contextKey: contextValueKey("trace-id"),
		expected:   "persist-trace",
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), cold.contextKey, cold.expected))
	resp, err := cache.Get(ctx, "persist-context-key", daramjwee.GetRequest{}, &mockFetcher{content: "origin-value", etag: "origin-v1"})
	require.NoError(t, err)

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))

	cancel()
	require.NoError(t, resp.Close())

	require.Eventually(t, func() bool {
		reader, meta, err := cold.GetStream(context.Background(), "persist-context-key")
		if err != nil {
			return false
		}
		defer reader.Close()
		persisted, err := io.ReadAll(reader)
		if err != nil {
			return false
		}
		return cold.sawValue.Load() && string(persisted) == "origin-value" && meta.CacheTag == "origin-v1"
	}, 2*time.Second, 10*time.Millisecond)
}
