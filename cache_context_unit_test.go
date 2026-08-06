package daramjwee

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneMetadata_Nil(t *testing.T) {
	assert.Nil(t, CloneMetadata(nil))
}

func TestCloneMetadata_DeepCopy(t *testing.T) {
	orig := &Metadata{
		CacheTag:   "abc",
		IsNegative: true,
		CachedAt:   time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
	}

	cloned := CloneMetadata(orig)
	require.NotNil(t, cloned)
	assert.Equal(t, *orig, *cloned)

	// Mutating the clone must not affect the original.
	cloned.CacheTag = "changed"
	cloned.IsNegative = false
	assert.Equal(t, "abc", orig.CacheTag)
	assert.True(t, orig.IsNegative)
}

func TestCloneMetadata_PointerIdentity(t *testing.T) {
	orig := &Metadata{CacheTag: "x"}
	cloned := CloneMetadata(orig)
	assert.NotSame(t, orig, cloned)
}

func TestValueOverlayContext_PrimaryFirst(t *testing.T) {
	type ctxKey string
	primary := context.WithValue(context.Background(), ctxKey("k"), "from-primary")
	values := context.WithValue(context.Background(), ctxKey("k"), "from-values")

	overlay := valueOverlayContext{primary: primary, values: values}
	assert.Equal(t, "from-primary", overlay.Value(ctxKey("k")))
}

func TestValueOverlayContext_FallbackToValues(t *testing.T) {
	type ctxKey string
	primary := context.Background()
	values := context.WithValue(context.Background(), ctxKey("k"), "from-values")

	overlay := valueOverlayContext{primary: primary, values: values}
	assert.Equal(t, "from-values", overlay.Value(ctxKey("k")))
}

func TestValueOverlayContext_MissingInBoth(t *testing.T) {
	type ctxKey string
	overlay := valueOverlayContext{
		primary: context.Background(),
		values:  context.Background(),
	}
	assert.Nil(t, overlay.Value(ctxKey("k")))
}

func TestValueOverlayContext_DeadlineAndDoneAndErr(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	primary, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	overlay := valueOverlayContext{primary: primary, values: context.Background()}

	got, ok := overlay.Deadline()
	assert.True(t, ok)
	assert.Equal(t, deadline, got)
	assert.NotNil(t, overlay.Done())
	assert.NoError(t, overlay.Err())
}

func TestDetachedValueContext_Nil(t *testing.T) {
	result := detachedValueContext(nil)
	assert.NotNil(t, result)
	// Should be a usable background context.
	assert.NoError(t, result.Err())
}

func TestDetachedValueContext_CancelDoesNotPropagate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	detached := detachedValueContext(ctx)

	// Cancel the original context.
	cancel()

	// The detached context must not be cancelled.
	select {
	case <-detached.Done():
		t.Fatal("detached context should not be cancelled when parent is cancelled")
	default:
		// OK – still open.
	}
}

func TestDetachedValueContext_WithDeadline(t *testing.T) {
	deadline := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	detached := detachedValueContext(ctx)

	// The detached context must not carry the parent's deadline.
	_, hasDeadline := detached.Deadline()
	assert.False(t, hasDeadline, "detached context should not inherit deadline")
}

func TestOverlayContextValues_NilValueCtx(t *testing.T) {
	runCtx := context.WithValue(context.Background(), "key", "run")
	result := overlayContextValues(runCtx, nil)
	// When valueCtx is nil, the runCtx is returned directly (no overlay wrapper).
	assert.Equal(t, runCtx, result)
}

func TestOverlayContextValues_WithOverlay(t *testing.T) {
	type ctxKey string
	runCtx := context.WithValue(context.Background(), ctxKey("rk"), "run")
	valueCtx := context.WithValue(context.Background(), ctxKey("vk"), "val")

	result := overlayContextValues(runCtx, valueCtx)
	require.NotNil(t, result)
	assert.Equal(t, "run", result.Value(ctxKey("rk")))
	assert.Equal(t, "val", result.Value(ctxKey("vk")))
}

func TestIsCachedStale_NilMeta(t *testing.T) {
	cache := &DaramjweeCache{}
	assert.True(t, cache.isCachedStale(nil, time.Hour, time.Hour))
}

func TestIsCachedStale_ZeroCachedAt(t *testing.T) {
	cache := &DaramjweeCache{}
	meta := &Metadata{CachedAt: time.Time{}}
	assert.True(t, cache.isCachedStale(meta, time.Hour, time.Hour))
}

func TestIsCachedStale_FreshPositive(t *testing.T) {
	cache := &DaramjweeCache{}
	meta := &Metadata{
		IsNegative: false,
		CachedAt:   time.Now(),
	}
	assert.False(t, cache.isCachedStale(meta, time.Hour, time.Hour))
}

func TestIsCachedStale_StalePositive(t *testing.T) {
	cache := &DaramjweeCache{}
	meta := &Metadata{
		IsNegative: false,
		CachedAt:   time.Now().Add(-2 * time.Hour),
	}
	assert.True(t, cache.isCachedStale(meta, time.Hour, time.Hour))
}

func TestIsCachedStale_FreshNegative(t *testing.T) {
	cache := &DaramjweeCache{}
	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}
	assert.False(t, cache.isCachedStale(meta, time.Hour, 30*time.Minute))
}

func TestIsCachedStale_StaleNegative(t *testing.T) {
	cache := &DaramjweeCache{}
	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now().Add(-1 * time.Hour),
	}
	assert.True(t, cache.isCachedStale(meta, time.Hour, 30*time.Minute))
}

func TestIsCachedStale_ExactBoundary(t *testing.T) {
	cache := &DaramjweeCache{}

	// When freshness is 0, the item should be immediately stale.
	// We use a time in the past to ensure Now() is always after CachedAt + 0.
	cachedAt := time.Now().Add(-time.Millisecond)
	meta := &Metadata{IsNegative: false, CachedAt: cachedAt}

	// At the exact boundary, Now() is slightly after CachedAt + freshness, so stale.
	assert.True(t, cache.isCachedStale(meta, 0, 0))
}

func TestTierFreshness_OverrideExists(t *testing.T) {
	cache := &DaramjweeCache{
		config: cacheConfig{
			positiveFreshness: 10 * time.Minute,
			negativeFreshness: 5 * time.Minute,
			tierFreshnessOverrides: map[int]TierFreshnessOverride{
				0: {Positive: 30 * time.Second, Negative: 10 * time.Second},
			},
		},
	}
	pos, neg := cache.tierFreshness(0)
	assert.Equal(t, 30*time.Second, pos)
	assert.Equal(t, 10*time.Second, neg)
}

func TestTierFreshness_NoOverride(t *testing.T) {
	cache := &DaramjweeCache{
		config: cacheConfig{
			positiveFreshness: 10 * time.Minute,
			negativeFreshness: 5 * time.Minute,
			tierFreshnessOverrides: map[int]TierFreshnessOverride{
				0: {Positive: 30 * time.Second, Negative: 10 * time.Second},
			},
		},
	}
	pos, neg := cache.tierFreshness(1)
	assert.Equal(t, 10*time.Minute, pos)
	assert.Equal(t, 5*time.Minute, neg)
}

func TestNewCtxWithTimeout_ExistingDeadline(t *testing.T) {
	cache := &DaramjweeCache{config: cacheConfig{opTimeout: 1 * time.Second}}
	deadline := time.Now().Add(5 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	result, resultCancel := cache.newCtxWithTimeout(ctx)
	defer resultCancel()

	// Should return the same context (no new timeout applied).
	gotDeadline, ok := result.Deadline()
	require.True(t, ok)
	assert.Equal(t, deadline, gotDeadline)
}

func TestNewCtxWithTimeout_NoExistingDeadline(t *testing.T) {
	cache := &DaramjweeCache{config: cacheConfig{opTimeout: 100 * time.Millisecond}}
	ctx := context.Background()

	result, resultCancel := cache.newCtxWithTimeout(ctx)
	defer resultCancel()

	gotDeadline, ok := result.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), gotDeadline, 50*time.Millisecond)
}

func TestHasRealStore_Nil(t *testing.T) {
	assert.False(t, hasRealStore(nil))
}

func TestHasRealStore_NullStore(t *testing.T) {
	assert.False(t, hasRealStore(newNullStore()))
}

func TestHasRealStore_RealStore(t *testing.T) {
	// memstore is a real store; we just need any non-nullStore implementation.
	// Use a minimal dummy that satisfies Store.
	store := &dummyStore{}
	assert.True(t, hasRealStore(store))
}

func TestNewGetResponse_NilMeta(t *testing.T) {
	body := io.NopCloser(strings.NewReader("data"))
	resp := newGetResponse(GetStatusOK, body, nil)

	assert.Equal(t, GetStatusOK, resp.Status)
	assert.Equal(t, body, resp.Body)
	assert.Equal(t, Metadata{}, resp.Metadata) // zero value
}

func TestNewGetResponse_NonNilMeta(t *testing.T) {
	body := io.NopCloser(strings.NewReader("data"))
	meta := &Metadata{
		CacheTag:   "tag-1",
		IsNegative: true,
		CachedAt:   time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
	}
	resp := newGetResponse(GetStatusNotFound, body, meta)

	assert.Equal(t, GetStatusNotFound, resp.Status)
	assert.Equal(t, body, resp.Body)
	assert.Equal(t, *meta, resp.Metadata)
}

// dummyStore is a minimal Store implementation for unit tests.
type dummyStore struct{}

func (d *dummyStore) GetStream(_ context.Context, _ string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}
func (d *dummyStore) BeginSet(_ context.Context, _ string, _ *Metadata) (WriteSink, error) {
	return nil, ErrNotFound
}
func (d *dummyStore) Delete(_ context.Context, _ string) error { return nil }
func (d *dummyStore) Stat(_ context.Context, _ string) (*Metadata, error) {
	return nil, ErrNotFound
}
