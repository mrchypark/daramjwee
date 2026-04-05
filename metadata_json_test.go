package daramjwee

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataMarshalJSONWritesCacheTag(t *testing.T) {
	meta := Metadata{
		CacheTag:   "cache-v1",
		IsNegative: true,
		CachedAt:   time.Date(2026, time.April, 5, 12, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.JSONEq(t, `{"CacheTag":"cache-v1","IsNegative":true,"CachedAt":"2026-04-05T12:00:00Z"}`, string(data))
}

func TestMetadataUnmarshalJSONAcceptsCacheTagAndLegacyETag(t *testing.T) {
	t.Run("cache tag", func(t *testing.T) {
		var meta Metadata
		err := json.Unmarshal([]byte(`{"CacheTag":"cache-v1","IsNegative":true,"CachedAt":"2026-04-05T12:00:00Z"}`), &meta)
		require.NoError(t, err)
		require.Equal(t, Metadata{
			CacheTag:   "cache-v1",
			IsNegative: true,
			CachedAt:   time.Date(2026, time.April, 5, 12, 0, 0, 0, time.UTC),
		}, meta)
	})

	t.Run("legacy etag", func(t *testing.T) {
		var meta Metadata
		err := json.Unmarshal([]byte(`{"ETag":"legacy-v1","IsNegative":false,"CachedAt":"0001-01-01T00:00:00Z"}`), &meta)
		require.NoError(t, err)
		require.Equal(t, Metadata{
			CacheTag: "legacy-v1",
			CachedAt: time.Time{},
		}, meta)
	})
}
