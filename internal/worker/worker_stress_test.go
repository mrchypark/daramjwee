package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestWorkerManager_ConcurrentSubmitAndShutdownStress(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		manager, err := NewManager("pool", log.NewNopLogger(), 2, 2, 2*time.Second)
		require.NoError(t, err)

		var accepted atomic.Int64
		var ran atomic.Int64
		var submitWG sync.WaitGroup

		for g := 0; g < 8; g++ {
			submitWG.Add(1)
			go func() {
				defer submitWG.Done()
				for i := 0; i < 100; i++ {
					if manager.Submit(func(ctx context.Context) {
						ran.Add(1)
						time.Sleep(2 * time.Millisecond)
					}) {
						accepted.Add(1)
					}
				}
			}()
		}

		time.Sleep(10 * time.Millisecond)
		require.NoError(t, manager.Shutdown(2*time.Second))
		submitWG.Wait()

		if accepted.Load() == 0 {
			t.Fatalf("iteration %d: expected at least one accepted job", iter)
		}
		if ran.Load() == 0 {
			t.Fatalf("iteration %d: expected at least one executed job", iter)
		}
		if ran.Load() > accepted.Load() {
			t.Fatalf("iteration %d: executed jobs exceeded accepted jobs: ran=%d accepted=%d", iter, ran.Load(), accepted.Load())
		}
	}
}
