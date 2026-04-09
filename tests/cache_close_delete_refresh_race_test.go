package daramjwee_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/require"
)

func TestCache_ConcurrentCloseDeleteAndScheduleRefresh(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		hot := &signalingDeleteStore{mockStore: newMockStore(), started: make(chan struct{}, 1), blocker: make(chan struct{})}
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

		key := "race-key"
		writer, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: "seed"})
		require.NoError(t, err)
		_, err = writer.Write([]byte("seed"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		cold.setData(key, "seed", &daramjwee.Metadata{CacheTag: "seed", CachedAt: time.Now()})

		started := make(chan struct{}, 32)
		blocker := make(chan struct{})
		fetcher := blockingSuccessFetcher{
			started:  started,
			blocker:  blocker,
			content:  "refresh-value",
			cacheTag: "refresh-value",
		}

		require.NoError(t, cache.ScheduleRefresh(context.Background(), key, fetcher))
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: initial refresh did not start", iter)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 32)

		for range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := cache.ScheduleRefresh(context.Background(), key, fetcher)
				if !acceptedRefreshRaceError(err) {
					errs <- err
				}
			}()
		}

		for range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				err := cache.Delete(context.Background(), key)
				if !acceptedCloseDeleteRaceError(err) {
					errs <- err
				}
			}()
		}

		close(start)
		select {
		case <-hot.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: delete did not start", iter)
		}

		closeDone := make(chan struct{})
		go func() {
			cache.Close()
			close(closeDone)
		}()
		close(hot.blocker)

		close(blocker)
		wg.Wait()

		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Fatalf("iteration %d: cache close timed out", iter)
		}

		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}

		hotState, err := currentMockStoreState(hot.mockStore, key)
		require.NoError(t, err)
		coldState, err := currentMockStoreState(cold, key)
		require.NoError(t, err)
		require.True(t, isAllowedCloseRaceState(hotState), "iteration %d: unexpected hot state %+v", iter, hotState)
		require.True(t, isAllowedCloseRaceState(coldState), "iteration %d: unexpected cold state %+v", iter, coldState)
	}
}

func acceptedRefreshRaceError(err error) bool {
	return err == nil ||
		errors.Is(err, daramjwee.ErrBackgroundJobRejected) ||
		errors.Is(err, daramjwee.ErrCacheClosed)
}

func acceptedCloseDeleteRaceError(err error) bool {
	return err == nil || errors.Is(err, daramjwee.ErrCacheClosed)
}

func isAllowedCloseRaceState(state entryExpectation) bool {
	return state == (entryExpectation{}) ||
		state == (entryExpectation{present: true, value: "refresh-value", cacheTag: "refresh-value"})
}

type signalingDeleteStore struct {
	*mockStore
	started chan struct{}
	blocker chan struct{}
}

func (s *signalingDeleteStore) Delete(ctx context.Context, key string) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.blocker:
	}
	return s.mockStore.Delete(ctx, key)
}
