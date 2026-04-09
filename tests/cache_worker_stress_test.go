package daramjwee_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/require"
)

func TestCache_CloseDuringRefreshQueuePressureStress(t *testing.T) {
	for iter := 0; iter < 3; iter++ {
		hot := newMockStore()
		cold := newMockStore()
		cache, err := daramjwee.New(
			nil,
			daramjwee.WithTiers(hot, cold),
			daramjwee.WithOpTimeout(2*time.Second),
			daramjwee.WithWorkers(1),
			daramjwee.WithWorkerQueue(1),
			daramjwee.WithWorkerTimeout(2*time.Second),
		)
		require.NoError(t, err)

		blocker := make(chan struct{})
		started := make(chan struct{}, 32)
		fetcher := blockingSuccessFetcher{
			started:  started,
			blocker:  blocker,
			content:  "refresh-value",
			cacheTag: "refresh-value",
		}

		var accepted atomic.Int64
		var submitWG sync.WaitGroup
		for g := 0; g < 8; g++ {
			submitWG.Add(1)
			go func(id int) {
				defer submitWG.Done()
				key := "stress-key-" + string('a'+byte(id%3))
				if err := cache.ScheduleRefresh(context.Background(), key, fetcher); err == nil {
					accepted.Add(1)
				} else if err != daramjwee.ErrBackgroundJobRejected {
					t.Errorf("unexpected schedule refresh error: %v", err)
				}
			}(g)
		}

		submitWG.Wait()
		if accepted.Load() == 0 {
			close(blocker)
			cache.Close()
			t.Fatalf("iteration %d: expected at least one accepted refresh", iter)
		}

		closeDone := make(chan struct{})
		go func() {
			cache.Close()
			close(closeDone)
		}()

		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(blocker)
			t.Fatalf("iteration %d: refresh job did not start", iter)
		}

		close(blocker)

		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Fatalf("iteration %d: cache close timed out under queue pressure", iter)
		}
	}
}
