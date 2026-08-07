package daramjwee

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsConditionalRequestSatisfied(t *testing.T) {
	tests := []struct {
		name     string
		req      GetRequest
		meta     *Metadata
		expected bool
	}{
		{
			name:     "nil metadata",
			req:      GetRequest{IfNoneMatch: `"v1"`},
			meta:     nil,
			expected: false,
		},
		{
			name:     "empty if-none-match",
			req:      GetRequest{IfNoneMatch: ""},
			meta:     &Metadata{CacheTag: "v1"},
			expected: false,
		},
		{
			name:     "negative entry",
			req:      GetRequest{IfNoneMatch: `"v1"`},
			meta:     &Metadata{CacheTag: "v1", IsNegative: true},
			expected: false,
		},
		{
			name:     "matching etag",
			req:      GetRequest{IfNoneMatch: `"v1"`},
			meta:     &Metadata{CacheTag: "v1"},
			expected: true,
		},
		{
			name:     "non-matching etag",
			req:      GetRequest{IfNoneMatch: `"v2"`},
			meta:     &Metadata{CacheTag: "v1"},
			expected: false,
		},
		{
			name:     "wildcard if-none-match",
			req:      GetRequest{IfNoneMatch: "*"},
			meta:     &Metadata{CacheTag: "v1"},
			expected: true,
		},
		{
			name:     "wildcard if-none-match with negative entry",
			req:      GetRequest{IfNoneMatch: "*"},
			meta:     &Metadata{CacheTag: "v1", IsNegative: true},
			expected: false,
		},
		{
			name:     "multiple etags first matches",
			req:      GetRequest{IfNoneMatch: `"v2", "v1"`},
			meta:     &Metadata{CacheTag: "v1"},
			expected: true,
		},
		{
			name:     "multiple etags second matches",
			req:      GetRequest{IfNoneMatch: `"v2", "v1"`},
			meta:     &Metadata{CacheTag: "v2"},
			expected: true,
		},
		{
			name:     "whitespace if-none-match",
			req:      GetRequest{IfNoneMatch: "  "},
			meta:     &Metadata{CacheTag: "v1"},
			expected: false,
		},
		{
			name:     "weak etag matching strong",
			req:      GetRequest{IfNoneMatch: `W/"v1"`},
			meta:     &Metadata{CacheTag: `"v1"`},
			expected: true,
		},
		{
			name:     "empty cache tag",
			req:      GetRequest{IfNoneMatch: `"v1"`},
			meta:     &Metadata{CacheTag: ""},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &DaramjweeCache{logger: log.NewNopLogger()}
			got := cache.isConditionalRequestSatisfied(tt.req, tt.meta)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestTopTierCloseCallback(t *testing.T) {
	t.Run("not stale returns cancel handler", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		cache := &DaramjweeCache{logger: log.NewNopLogger()}

		handler := cache.topTierCloseCallback(context.Background(), "key", nil, cancel, &Metadata{}, false, nil)
		handler.handle()

		assert.True(t, cancelCalled)
	})

	t.Run("stale returns refresh callback", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		observedGen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &topWriteManager{},
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		observedGen.coord.committedGeneration.Store(1)
		cache := &DaramjweeCache{logger: log.NewNopLogger()}

		handler := cache.topTierCloseCallback(context.Background(), "key", nil, cancel, &Metadata{CacheTag: "v1"}, true, observedGen)
		require.NotNil(t, handler)

		handler.handle()
		assert.True(t, cancelCalled)
	})
}

func TestHandleConditionalLowerTierPromotionError(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus GetStatus
		cancelExpected bool
	}{
		{
			name:           "invalidation with preserveBody true",
			err:            lowerTierPromotionInvalidatedError{preserveBody: true},
			expectedStatus: GetStatusOK,
			cancelExpected: false,
		},
		{
			name:           "invalidation with preserveBody false",
			err:            lowerTierPromotionInvalidatedError{preserveBody: false},
			expectedStatus: GetStatusNotFound,
			cancelExpected: true,
		},
		{
			name:           "ErrTopWriteInvalidated",
			err:            ErrTopWriteInvalidated,
			expectedStatus: GetStatusOK,
			cancelExpected: false,
		},
		{
			name:           "other error",
			err:            errors.New("some error"),
			expectedStatus: GetStatusNotModified,
			cancelExpected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cancelCalled := false
			cancel := func() { cancelCalled = true }
			src := io.NopCloser(strings.NewReader("data"))
			cache := &DaramjweeCache{logger: log.NewNopLogger()}
			meta := &Metadata{CacheTag: "v1"}

			resp := cache.handleConditionalLowerTierPromotionError("key", 1, tt.err, src, meta, cancel)
			assert.Equal(t, tt.expectedStatus, resp.Status)
			assert.Equal(t, tt.cancelExpected, cancelCalled)
		})
	}
}

func TestDecideLowerTierHit(t *testing.T) {
	tests := []struct {
		name             string
		higherTiersClean bool
		ifNoneMatch      string
		cacheTag         string
		isNegative       bool
		isStale          bool
		canServeCond     bool
		expectedAction   lowerTierAction
	}{
		{
			name:             "higher tiers dirty + positive",
			higherTiersClean: false,
			cacheTag:         "v1",
			isStale:          false,
			expectedAction:   actionServeBodyDirect,
		},
		{
			name:             "higher tiers dirty + conditional",
			higherTiersClean: false,
			ifNoneMatch:      `"v1"`,
			cacheTag:         "v1",
			expectedAction:   actionServeConditionalDirect,
		},
		{
			name:             "higher tiers dirty + negative",
			higherTiersClean: false,
			isNegative:       true,
			expectedAction:   actionServeNotFoundDirect,
		},
		{
			name:             "higher tiers dirty + stale",
			higherTiersClean: false,
			isStale:          true,
			expectedAction:   actionServeStaleBodyRefresh,
		},
		{
			name:             "higher tiers clean + conditional + can serve",
			higherTiersClean: true,
			ifNoneMatch:      `"v1"`,
			cacheTag:         "v1",
			canServeCond:     true,
			expectedAction:   actionServeConditionalWithPromotion,
		},
		{
			name:             "higher tiers clean + conditional + cannot serve",
			higherTiersClean: true,
			ifNoneMatch:      `"v1"`,
			cacheTag:         "v1",
			canServeCond:     false,
			expectedAction:   actionServeBodyDirect,
		},
		{
			name:             "higher tiers clean + negative + not stale",
			higherTiersClean: true,
			isNegative:       true,
			isStale:          false,
			expectedAction:   actionServeNegativeNotFoundPromote,
		},
		{
			name:             "higher tiers clean + negative + stale",
			higherTiersClean: true,
			isNegative:       true,
			isStale:          true,
			expectedAction:   actionServeNegativeNotFoundStale,
		},
		{
			name:             "higher tiers clean + positive + stale",
			higherTiersClean: true,
			isStale:          true,
			expectedAction:   actionServeStaleBodyRefresh,
		},
		{
			name:             "higher tiers clean + positive + not stale",
			higherTiersClean: true,
			expectedAction:   actionPromotePositive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &DaramjweeCache{logger: log.NewNopLogger()}

			p := lowerTierHitParams{
				key:              "key",
				higherTiersClean: tt.higherTiersClean,
				req:              GetRequest{IfNoneMatch: tt.ifNoneMatch},
			}
			meta := &Metadata{
				CacheTag:   tt.cacheTag,
				IsNegative: tt.isNegative,
			}

			if tt.canServeCond {
				gen := &topWriteGeneration{
					coord: &writeCoordinator{
						manager:             &cache.topWrites,
						key:                 "key",
						committedGeneration: atomic.Uint64{},
					},
					generation: 1,
				}
				gen.coord.committedGeneration.Store(1)
				p.expectedGeneration = gen
			}

			decision := cache.decideLowerTierHit(p, meta, tt.isStale)
			assert.Equal(t, tt.expectedAction, decision.action)
			assert.Equal(t, tt.isStale, decision.isStale)
		})
	}
}

func TestServeLowerTierWithoutPromotion(t *testing.T) {
	t.Run("conditional request satisfied", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		src := io.NopCloser(strings.NewReader("data"))
		cache := &DaramjweeCache{logger: log.NewNopLogger()}

		p := lowerTierHitParams{
			key:    "key",
			req:    GetRequest{IfNoneMatch: `"v1"`},
			src:    src,
			cancel: cancel,
			meta:   &Metadata{CacheTag: "v1"},
		}

		resp, err := cache.serveLowerTierWithoutPromotion(p, false)
		require.NoError(t, err)
		assert.Equal(t, GetStatusOK, resp.Status)
		assert.False(t, cancelCalled)
	})

	t.Run("negative entry", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		src := io.NopCloser(strings.NewReader("data"))
		cache := &DaramjweeCache{logger: log.NewNopLogger()}

		p := lowerTierHitParams{
			key:    "key",
			req:    GetRequest{},
			src:    src,
			cancel: cancel,
			meta:   &Metadata{IsNegative: true},
		}

		resp, err := cache.serveLowerTierWithoutPromotion(p, false)
		require.NoError(t, err)
		assert.Equal(t, GetStatusNotFound, resp.Status)
		assert.True(t, cancelCalled)
	})

	t.Run("positive entry", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		src := io.NopCloser(strings.NewReader("data"))
		cache := &DaramjweeCache{logger: log.NewNopLogger()}

		p := lowerTierHitParams{
			key:    "key",
			req:    GetRequest{},
			src:    src,
			cancel: cancel,
			meta:   &Metadata{CacheTag: "v1"},
		}

		resp, err := cache.serveLowerTierWithoutPromotion(p, false)
		require.NoError(t, err)
		assert.Equal(t, GetStatusOK, resp.Status)
		assert.False(t, cancelCalled)
	})
}

func TestHandleStaleLowerTierHit(t *testing.T) {
	t.Run("negative entry returns not found", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		src := io.NopCloser(strings.NewReader("data"))
		observedGen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &topWriteManager{},
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		observedGen.coord.committedGeneration.Store(1)
		cache := &DaramjweeCache{
			tiers:  []Store{&nullStore{}},
			logger: log.NewNopLogger(),
		}

		resp, err := cache.handleStaleLowerTierHit(context.Background(), "key", 0, nil, src, &Metadata{IsNegative: true}, cancel, observedGen)
		require.NoError(t, err)
		assert.Equal(t, GetStatusNotFound, resp.Status)
		assert.True(t, cancelCalled)
	})

	t.Run("positive entry returns ok", func(t *testing.T) {
		cancelCalled := false
		cancel := func() { cancelCalled = true }
		src := io.NopCloser(strings.NewReader("data"))
		observedGen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &topWriteManager{},
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		observedGen.coord.committedGeneration.Store(1)
		cache := &DaramjweeCache{
			tiers:  []Store{&nullStore{}},
			logger: log.NewNopLogger(),
		}

		resp, err := cache.handleStaleLowerTierHit(context.Background(), "key", 0, nil, src, &Metadata{CacheTag: "v1"}, cancel, observedGen)
		require.NoError(t, err)
		assert.Equal(t, GetStatusOK, resp.Status)
		assert.False(t, cancelCalled)
	})
}

func TestCanAttemptExpectedTopWrite(t *testing.T) {
	t.Run("nil generation", func(t *testing.T) {
		cache := &DaramjweeCache{}
		assert.False(t, cache.canAttemptExpectedTopWrite("key", nil))
	})

	t.Run("wrong manager", func(t *testing.T) {
		cache := &DaramjweeCache{}
		gen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &topWriteManager{},
				key:                 "key",
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		gen.coord.committedGeneration.Store(1)
		assert.False(t, cache.canAttemptExpectedTopWrite("key", gen))
	})

	t.Run("wrong key", func(t *testing.T) {
		cache := &DaramjweeCache{}
		gen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &cache.topWrites,
				key:                 "other",
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		gen.coord.committedGeneration.Store(1)
		assert.False(t, cache.canAttemptExpectedTopWrite("key", gen))
	})

	t.Run("coord rejects (generation mismatch)", func(t *testing.T) {
		cache := &DaramjweeCache{}
		gen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &cache.topWrites,
				key:                 "key",
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		gen.coord.committedGeneration.Store(2)
		assert.False(t, cache.canAttemptExpectedTopWrite("key", gen))
	})

	t.Run("all conditions met", func(t *testing.T) {
		cache := &DaramjweeCache{}
		gen := &topWriteGeneration{
			coord: &writeCoordinator{
				manager:             &cache.topWrites,
				key:                 "key",
				committedGeneration: atomic.Uint64{},
			},
			generation: 1,
		}
		gen.coord.committedGeneration.Store(1)
		assert.True(t, cache.canAttemptExpectedTopWrite("key", gen))
	})
}

func TestDecideDirectServe(t *testing.T) {
	tests := []struct {
		name           string
		ifNoneMatch    string
		cacheTag       string
		isNegative     bool
		isStale        bool
		expectedAction lowerTierAction
	}{
		{
			name:           "conditional satisfied",
			ifNoneMatch:    `"v1"`,
			cacheTag:       "v1",
			expectedAction: actionServeConditionalDirect,
		},
		{
			name:           "negative entry",
			isNegative:     true,
			expectedAction: actionServeNotFoundDirect,
		},
		{
			name:           "stale entry",
			isStale:        true,
			expectedAction: actionServeStaleBodyRefresh,
		},
		{
			name:           "positive entry",
			expectedAction: actionServeBodyDirect,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &DaramjweeCache{logger: log.NewNopLogger()}
			p := lowerTierHitParams{
				key:  "key",
				req:  GetRequest{IfNoneMatch: tt.ifNoneMatch},
				meta: &Metadata{CacheTag: tt.cacheTag, IsNegative: tt.isNegative},
			}

			decision := cache.decideDirectServe(p, p.meta, tt.isStale)
			assert.Equal(t, tt.expectedAction, decision.action)
		})
	}
}

func TestHandleMissFetchError(t *testing.T) {
	t.Run("ErrCacheableNotFound higher tiers clean", func(t *testing.T) {
		cache := &DaramjweeCache{
			logger: log.NewNopLogger(),
			tiers:  []Store{&nullStore{}},
		}
		cancelCalled := false
		cancel := func() { cancelCalled = true }

		_, err := cache.handleMissFetchError(
			context.Background(), context.Background(), "key",
			GetRequest{}, cancel, nil,
			ErrCacheableNotFound, nil, true,
		)
		require.NoError(t, err)
		assert.True(t, cancelCalled)
	})

	t.Run("ErrCacheableNotFound higher tiers dirty", func(t *testing.T) {
		cache := &DaramjweeCache{
			logger: log.NewNopLogger(),
			tiers:  []Store{&nullStore{}},
		}
		cancelCalled := false
		cancel := func() { cancelCalled = true }

		resp, err := cache.handleMissFetchError(
			context.Background(), context.Background(), "key",
			GetRequest{}, cancel, nil,
			ErrCacheableNotFound, nil, false,
		)
		require.NoError(t, err)
		assert.Equal(t, GetStatusNotFound, resp.Status)
		assert.True(t, cancelCalled)
	})

	t.Run("ErrNotModified higher tiers dirty", func(t *testing.T) {
		cache := &DaramjweeCache{
			logger: log.NewNopLogger(),
			tiers:  []Store{&nullStore{}},
		}
		cancelCalled := false
		cancel := func() { cancelCalled = true }

		_, err := cache.handleMissFetchError(
			context.Background(), context.Background(), "key",
			GetRequest{}, cancel, nil,
			ErrNotModified, nil, false,
		)
		require.Error(t, err)
		assert.True(t, cancelCalled)
	})

	t.Run("other error", func(t *testing.T) {
		cache := &DaramjweeCache{
			logger: log.NewNopLogger(),
			tiers:  []Store{&nullStore{}},
		}
		cancelCalled := false
		cancel := func() { cancelCalled = true }

		fetchErr := errors.New("network error")
		_, err := cache.handleMissFetchError(
			context.Background(), context.Background(), "key",
			GetRequest{}, cancel, nil,
			fetchErr, nil, true,
		)
		assert.Equal(t, fetchErr, err)
		assert.False(t, cancelCalled)
	})
}
