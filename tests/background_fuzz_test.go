package daramjwee_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/require"
)

func FuzzScheduleRefreshBackgroundStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 1, 0, 2, 3})
	f.Add([]byte{1, 1, 3, 0, 2, 1, 3, 2})

	f.Fuzz(func(t *testing.T, ops []byte) {
		hot := newMockStore()
		cold := newMockStore()
		cache, err := daramjwee.New(
			nil,
			daramjwee.WithTiers(hot, cold),
			daramjwee.WithOpTimeout(2*time.Second),
			daramjwee.WithFreshness(time.Hour, 0),
			daramjwee.WithWorkers(1),
			daramjwee.WithWorkerQueue(8),
			daramjwee.WithWorkerTimeout(2*time.Second),
		)
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		keys := []string{"bg-key-0", "bg-key-1", "bg-key-2"}
		hotState := map[string]entryExpectation{}
		coldState := map[string]entryExpectation{}

		for i, op := range ops {
			key := keys[int(op)%len(keys)]
			value := fuzzValue(i, op)

			switch op % 4 {
			case 0:
				writer, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: value})
				require.NoError(t, err)
				_, err = writer.Write([]byte(value))
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				hotState[key] = entryExpectation{present: true, value: value, cacheTag: value}
				eventuallyExpectStoreState(t, hot, key, hotState[key])
			case 1:
				require.NoError(t, cache.ScheduleRefresh(ctx, key, &mockFetcher{content: value, etag: value}))
				hotState[key] = entryExpectation{present: true, value: value, cacheTag: value}
				coldState[key] = entryExpectation{present: true, value: value, cacheTag: value}
				eventuallyExpectStoreState(t, hot, key, hotState[key])
				eventuallyExpectStoreState(t, cold, key, coldState[key])
			case 2:
				require.NoError(t, cache.ScheduleRefresh(ctx, key, cacheableNotFoundFetcher{}))
				hotState[key] = entryExpectation{present: true, negative: true}
				eventuallyExpectStoreState(t, hot, key, hotState[key])
				eventuallyExpectStoreState(t, cold, key, coldState[key])
			case 3:
				require.NoError(t, cache.Delete(ctx, key))
				delete(hotState, key)
				delete(coldState, key)
				eventuallyExpectStoreState(t, hot, key, entryExpectation{})
				eventuallyExpectStoreState(t, cold, key, entryExpectation{})
			}
		}
	})
}

func FuzzLowerTierFanoutStateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 1, 2})
	f.Add([]byte{0, 0, 1, 2, 1, 2})

	f.Fuzz(func(t *testing.T, ops []byte) {
		top := newMockStore()
		mid := newMockStore()
		low := newMockStore()
		cache, err := daramjwee.New(
			nil,
			daramjwee.WithTiers(top, mid, low),
			daramjwee.WithOpTimeout(2*time.Second),
			daramjwee.WithFreshness(time.Hour, 0),
			daramjwee.WithWorkers(1),
			daramjwee.WithWorkerQueue(8),
			daramjwee.WithWorkerTimeout(2*time.Second),
		)
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()
		keys := []string{"fanout-key-0", "fanout-key-1", "fanout-key-2"}
		state := map[string]entryExpectation{}

		for i, op := range ops {
			key := keys[int(op)%len(keys)]
			value := fuzzValue(i, op)

			switch op % 3 {
			case 0:
				require.NoError(t, top.Delete(ctx, key))
				require.NoError(t, mid.Delete(ctx, key))
				low.setData(key, value, &daramjwee.Metadata{CacheTag: value, CachedAt: time.Now()})
				state[key] = entryExpectation{present: true, value: value, cacheTag: value}
				eventuallyExpectStoreState(t, top, key, entryExpectation{})
				eventuallyExpectStoreState(t, mid, key, entryExpectation{})
				eventuallyExpectStoreState(t, low, key, state[key])
			case 1:
				resp, err := cache.Get(ctx, key, daramjwee.GetRequest{}, &mockFetcher{content: "unused", etag: "unused"})
				if expected, ok := state[key]; ok {
					require.NoError(t, err)
					body, readErr := io.ReadAll(resp)
					require.NoError(t, readErr)
					require.NoError(t, resp.Close())
					require.Equal(t, expected.value, string(body))
					eventuallyExpectStoreState(t, top, key, expected)
					eventuallyExpectStoreState(t, mid, key, expected)
					eventuallyExpectStoreState(t, low, key, expected)
				} else {
					require.NoError(t, err)
					body, readErr := io.ReadAll(resp)
					require.NoError(t, readErr)
					require.NoError(t, resp.Close())
					generated := string(body)
					state[key] = entryExpectation{present: true, value: generated, cacheTag: "unused"}
					eventuallyExpectStoreState(t, top, key, state[key])
					eventuallyExpectStoreState(t, mid, key, state[key])
				}
			case 2:
				require.NoError(t, cache.Delete(ctx, key))
				delete(state, key)
				eventuallyExpectStoreState(t, top, key, entryExpectation{})
				eventuallyExpectStoreState(t, mid, key, entryExpectation{})
				eventuallyExpectStoreState(t, low, key, entryExpectation{})
			}
		}
	})
}

type entryExpectation struct {
	present  bool
	negative bool
	value    string
	cacheTag string
}

func eventuallyExpectStoreState(t *testing.T, store *mockStore, key string, want entryExpectation) {
	t.Helper()
	require.Eventually(t, func() bool {
		got, err := currentMockStoreState(store, key)
		return err == nil && got == want
	}, 2*time.Second, 10*time.Millisecond, "store state for %q did not converge to %+v", key, want)
}

func currentMockStoreState(store *mockStore, key string) (entryExpectation, error) {
	reader, meta, err := store.GetStream(context.Background(), key)
	if err != nil {
		if err == daramjwee.ErrNotFound {
			return entryExpectation{}, nil
		}
		return entryExpectation{}, err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return entryExpectation{}, err
	}
	got := entryExpectation{
		present:  true,
		negative: meta.IsNegative,
		value:    string(body),
		cacheTag: meta.CacheTag,
	}
	return got, nil
}

type cacheableNotFoundFetcher struct{}

func (cacheableNotFoundFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return nil, daramjwee.ErrCacheableNotFound
}

func fuzzValue(i int, op byte) string {
	return string([]byte{
		'v',
		'a' + byte(i%26),
		'0' + (op % 10),
	})
}
