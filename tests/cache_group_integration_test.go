package daramjwee_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
)

type groupTestStore struct{}

func (groupTestStore) GetStream(context.Context, string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (groupTestStore) BeginSet(context.Context, string, *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return noopWriteSink{}, nil
}

func (groupTestStore) Delete(context.Context, string) error { return nil }

func (groupTestStore) Stat(context.Context, string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

type noopWriteSink struct{}

func (noopWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (noopWriteSink) Close() error                { return nil }
func (noopWriteSink) Abort() error                { return nil }

func TestCacheGroup_NewCacheAppliesGroupQueueDefault(t *testing.T) {
	group, err := daramjwee.NewGroup(nil,
		daramjwee.WithGroupWorkers(2),
		daramjwee.WithGroupWorkerQueueDefault(12),
	)
	require.NoError(t, err)
	t.Cleanup(group.Close)

	cache, err := group.NewCache("cache-a", daramjwee.WithTiers(groupTestStore{}))
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	typed := reflect.ValueOf(cache).Elem()
	require.Equal(t, 12, int(typed.FieldByName("runtimeQueueLimit").Int()))
}

func TestCacheGroup_CloseClosesCreatedCaches(t *testing.T) {
	group, err := daramjwee.NewGroup(nil, daramjwee.WithGroupWorkers(1))
	require.NoError(t, err)

	cache, err := group.NewCache("cache-b", daramjwee.WithTiers(groupTestStore{}))
	require.NoError(t, err)

	group.Close()

	_, err = cache.Get(context.Background(), "k", daramjwee.GetRequest{}, groupNoopFetcher{})
	require.ErrorIs(t, err, daramjwee.ErrCacheClosed)
}

type groupBlockingFetcher struct {
	started chan struct{}
	release chan struct{}
}

func (f *groupBlockingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	close(f.started)
	<-f.release
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader("value")),
		Metadata: &daramjwee.Metadata{CacheTag: fmt.Sprintf("tag-%d", time.Now().UnixNano())},
	}, nil
}

type groupRecordingFetcher struct {
	label string
	mu    *sync.Mutex
	order *[]string
}

func (f groupRecordingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.mu.Lock()
	*f.order = append(*f.order, f.label)
	f.mu.Unlock()
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.label)),
		Metadata: &daramjwee.Metadata{CacheTag: f.label},
	}, nil
}

type groupNoopFetcher struct{}

func (groupNoopFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader("noop")),
		Metadata: &daramjwee.Metadata{CacheTag: "noop"},
	}, nil
}

func TestCacheGroup_QueueIsolationThroughScheduleRefresh(t *testing.T) {
	group, err := daramjwee.NewGroup(nil,
		daramjwee.WithGroupWorkers(1),
		daramjwee.WithGroupWorkerQueueDefault(1),
	)
	require.NoError(t, err)
	t.Cleanup(group.Close)

	cacheA, err := group.NewCache("cache-a", daramjwee.WithTiers(groupTestStore{}))
	require.NoError(t, err)
	t.Cleanup(cacheA.Close)

	cacheB, err := group.NewCache("cache-b", daramjwee.WithTiers(groupTestStore{}))
	require.NoError(t, err)
	t.Cleanup(cacheB.Close)

	fetcher := &groupBlockingFetcher{started: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, cacheA.ScheduleRefresh(context.Background(), "key-a", fetcher))
	<-fetcher.started

	require.NoError(t, cacheA.ScheduleRefresh(context.Background(), "key-a-queued", groupNoopFetcher{}))
	require.ErrorIs(t, cacheA.ScheduleRefresh(context.Background(), "key-a-rejected", groupNoopFetcher{}), daramjwee.ErrBackgroundJobRejected)
	require.NoError(t, cacheB.ScheduleRefresh(context.Background(), "key-b", groupNoopFetcher{}))

	close(fetcher.release)
}

func TestCacheGroup_WeightedDequeueProgress(t *testing.T) {
	group, err := daramjwee.NewGroup(nil,
		daramjwee.WithGroupWorkers(1),
		daramjwee.WithGroupWorkerQueueDefault(4),
	)
	require.NoError(t, err)
	t.Cleanup(group.Close)

	cacheA, err := group.NewCache("cache-a", daramjwee.WithTiers(groupTestStore{}), daramjwee.WithWeight(2))
	require.NoError(t, err)
	t.Cleanup(cacheA.Close)

	cacheB, err := group.NewCache("cache-b", daramjwee.WithTiers(groupTestStore{}), daramjwee.WithWeight(1))
	require.NoError(t, err)
	t.Cleanup(cacheB.Close)

	var mu sync.Mutex
	order := make([]string, 0, 3)
	fetcherA := groupRecordingFetcher{label: "A", mu: &mu, order: &order}
	fetcherB := groupRecordingFetcher{label: "B", mu: &mu, order: &order}

	require.NoError(t, cacheA.ScheduleRefresh(context.Background(), "a1", fetcherA))
	require.NoError(t, cacheA.ScheduleRefresh(context.Background(), "a2", fetcherA))
	require.NoError(t, cacheB.ScheduleRefresh(context.Background(), "b1", fetcherB))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 3
	}, 3*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"A", "A", "B"}, order[:3])
}
