package daramjwee

import (
	"testing"
	"time"
)

func TestPruneIdleTopWriteStatesRemovesExpiredEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state := cache.topWriteStateForKey("expired")
	state.lastTouched.Store(time.Now().Add(-10 * time.Second).UnixNano())

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("expired"); ok {
		t.Fatal("expected expired top write state to be pruned")
	}
}

func TestPruneIdleTopWriteStatesKeepsRecentEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	cache.topWriteStateForKey("recent")

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("recent"); !ok {
		t.Fatal("expected recent top write state to remain")
	}
}

func TestPruneIdleTopWriteStatesSkipsLockedEntries(t *testing.T) {
	cache := &DaramjweeCache{
		OpTimeout:     time.Second,
		WorkerTimeout: time.Second,
		CloseTimeout:  time.Second,
	}

	state := cache.topWriteStateForKey("locked")
	state.lastTouched.Store(time.Now().Add(-10 * time.Second).UnixNano())
	state.commitMu.Lock()
	defer state.commitMu.Unlock()

	cache.pruneIdleTopWriteStates(time.Now())

	if _, ok := cache.topWriteStates.Load("locked"); !ok {
		t.Fatal("expected locked top write state to remain")
	}
}
