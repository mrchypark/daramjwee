package runtime

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestGroup_RemoveCache_AdjustsNextIndex(t *testing.T) {
	rt := NewGroup(log.NewNopLogger(), 1, time.Second)

	require.NoError(t, rt.Register("cache-a", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-b", Config{Weight: 1, QueueLimit: 4}))
	require.NoError(t, rt.Register("cache-c", Config{Weight: 1, QueueLimit: 4}))

	group := rt.(*Group)

	group.mu.Lock()
	group.nextIdx = 2
	group.mu.Unlock()

	rt.RemoveCache("cache-a")

	group.mu.Lock()
	defer group.mu.Unlock()
	require.Equal(t, []string{"cache-b", "cache-c"}, group.order)
	require.Equal(t, 1, group.nextIdx)
}
