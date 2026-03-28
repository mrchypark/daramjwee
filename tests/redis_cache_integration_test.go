package daramjwee_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCache_Get_WithRedisHotStore_KeepsStreamUsable(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	cache, err := daramjwee.New(
		log.NewNopLogger(),
		daramjwee.WithTiers(redisstore.New(client, log.NewNopLogger())),
		daramjwee.WithDefaultTimeout(2*time.Second),
		daramjwee.WithCache(1*time.Minute),
	)
	require.NoError(t, err)
	defer cache.Close()

	w, err := cache.Set(context.Background(), "redis-key", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = w.Write([]byte("hello redis"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	stream, err := cache.Get(context.Background(), "redis-key", &mockFetcher{})
	require.NoError(t, err)
	defer stream.Close()

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "hello redis", string(got))
}
