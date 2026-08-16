package daramjwee_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// TestPromotionProbation_SecondHitPromotes verifies that the first lower-tier
// hit serves without promoting and only the second hit promotes to the top tier.
func TestPromotionProbation_SecondHitPromotes(t *testing.T) {
	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed lower tier
	sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
		CacheTag: "v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("lower-value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithPromotionProbation(1024),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// First hit: served from lower tier without promotion.
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "lower-value", string(body))
	require.NoError(t, resp.Close())

	_, _, err = top.GetStream(context.Background(), "key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound, "first hit must not promote")

	// Second hit: promoted to the top tier.
	resp, err = cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	body, err = io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "lower-value", string(body))
	require.NoError(t, resp.Close())

	reader, meta, err := top.GetStream(context.Background(), "key")
	require.NoError(t, err)
	_ = reader.Close()
	require.Equal(t, "v1", meta.CacheTag)
}

// TestPromotionProbation_DeleteResetsProbation verifies that deleting a key
// resets its probation state, so the next lower-tier hit does not promote.
func TestPromotionProbation_DeleteResetsProbation(t *testing.T) {
	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	seedLower := func() {
		sink, err := lower.BeginSet(context.Background(), "key", &daramjwee.Metadata{
			CacheTag: "v1",
			CachedAt: time.Now(),
		})
		require.NoError(t, err)
		_, err = sink.Write([]byte("lower-value"))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
	}

	seedLower()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithPromotionProbation(1024),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Two hits promote the key to the top tier.
	for i := 0; i < 2; i++ {
		resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
		require.NoError(t, err)
		_, _ = io.ReadAll(resp)
		require.NoError(t, resp.Close())
	}
	_, _, err = top.GetStream(context.Background(), "key")
	require.NoError(t, err)

	// Delete resets both tiers and the probation state.
	require.NoError(t, cache.Delete(context.Background(), "key"))
	seedLower()

	// First hit after delete must not promote again.
	resp, err := cache.Get(context.Background(), "key", daramjwee.GetRequest{}, silentFetcher{})
	require.NoError(t, err)
	_, _ = io.ReadAll(resp)
	require.NoError(t, resp.Close())

	_, _, err = top.GetStream(context.Background(), "key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound, "first hit after delete must not promote")
}
