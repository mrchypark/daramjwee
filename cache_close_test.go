package daramjwee

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

type cacheCloseStubRuntime struct {
	closeErr      error
	closeCalls    int
	removeCalls   int
	lastCacheID   string
	lastCloseWait time.Duration
}

func (s *cacheCloseStubRuntime) Register(cacheID string, cfg CacheRuntimeConfig) error { return nil }
func (s *cacheCloseStubRuntime) Submit(cacheID string, kind JobKind, job Job) error {
	job.Run(context.Background())
	return nil
}
func (s *cacheCloseStubRuntime) CloseCache(cacheID string, timeout time.Duration) error {
	s.closeCalls++
	s.lastCacheID = cacheID
	s.lastCloseWait = timeout
	return s.closeErr
}
func (s *cacheCloseStubRuntime) RemoveCache(cacheID string) {
	s.removeCalls++
	s.lastCacheID = cacheID
}
func (s *cacheCloseStubRuntime) Shutdown(timeout time.Duration) error { return nil }

func TestDaramjweeCache_Close_RemovesRuntimeStateAfterTimeout(t *testing.T) {
	errShutdownTimeout := errors.New("worker: shutdown timed out")
	rt := &cacheCloseStubRuntime{closeErr: errShutdownTimeout}
	hookCalls := 0

	cache := &DaramjweeCache{
		logger:  log.NewNopLogger(),
		runtime: rt,
		cacheID: "cache-a",
		config: cacheConfig{
			closeTimeout: 250 * time.Millisecond,
		},
		closeHook: func() {
			hookCalls++
		},
	}

	cache.Close()

	require.Equal(t, 1, rt.closeCalls)
	require.Equal(t, 1, rt.removeCalls)
	require.Equal(t, "cache-a", rt.lastCacheID)
	require.Equal(t, 250*time.Millisecond, rt.lastCloseWait)
	require.Equal(t, 1, hookCalls)
}
