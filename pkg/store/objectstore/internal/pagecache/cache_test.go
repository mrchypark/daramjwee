package pagecache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_NewReturnsNilForNonPositiveCapacity(t *testing.T) {
	assert.Nil(t, New(0))
	assert.Nil(t, New(-1))
}

func TestCache_SetGetAndEvictLeastRecentlyUsed(t *testing.T) {
	cache := New(4)
	require.NotNil(t, cache)

	cache.Set(Key{Version: "v1", Index: 1}, []byte("aa"))
	cache.Set(Key{Version: "v1", Index: 2}, []byte("bb"))

	got, ok := cache.Get(Key{Version: "v1", Index: 1})
	require.True(t, ok)
	assert.Equal(t, []byte("aa"), got)

	cache.Set(Key{Version: "v1", Index: 3}, []byte("cc"))

	_, ok = cache.Get(Key{Version: "v1", Index: 2})
	assert.False(t, ok, "least recently used page should be evicted")

	got, ok = cache.Get(Key{Version: "v1", Index: 1})
	require.True(t, ok)
	assert.Equal(t, []byte("aa"), got)

	got, ok = cache.Get(Key{Version: "v1", Index: 3})
	require.True(t, ok)
	assert.Equal(t, []byte("cc"), got)
}

func TestCache_IgnoresOversizedAndEmptyEntriesAndUpdatesExisting(t *testing.T) {
	cache := New(3)
	require.NotNil(t, cache)

	cache.Set(Key{Version: "v1", Index: 1}, []byte{})
	cache.Set(Key{Version: "v1", Index: 2}, []byte("toolarge"))
	_, ok := cache.Get(Key{Version: "v1", Index: 1})
	assert.False(t, ok)
	_, ok = cache.Get(Key{Version: "v1", Index: 2})
	assert.False(t, ok)

	key := Key{Version: "v1", Index: 3}
	cache.Set(key, []byte("ab"))
	cache.Set(key, []byte("xyz"))

	got, ok := cache.Get(key)
	require.True(t, ok)
	assert.Equal(t, []byte("xyz"), got)
}
