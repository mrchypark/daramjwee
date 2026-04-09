package daramjwee

import (
	"encoding/json"
	"testing"
	"time"
)

func FuzzMetadataJSONRoundTrip(f *testing.F) {
	f.Add("cache-v1", true, int64(0), uint32(0))
	f.Add("", false, int64(1712318400), uint32(123))
	f.Add("legacy,etag", false, int64(-3600), uint32(999999999))

	f.Fuzz(func(t *testing.T, cacheTag string, isNegative bool, secs int64, nanos uint32) {
		meta := Metadata{
			CacheTag:   cacheTag,
			IsNegative: isNegative,
			CachedAt:   fuzzTime(secs, nanos),
		}

		data, err := json.Marshal(meta)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		var roundTripped Metadata
		if err := json.Unmarshal(data, &roundTripped); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		want := meta
		want.CacheTag = normalizedJSONString(cacheTag)
		if roundTripped != want {
			t.Fatalf("round trip mismatch: got %+v want %+v", roundTripped, want)
		}
	})
}

func FuzzMetadataJSONLegacyAlias(f *testing.F) {
	f.Add("", "legacy-v1", false, int64(0), uint32(0))
	f.Add("cache-v2", "legacy-v2", true, int64(1712318400), uint32(42))

	f.Fuzz(func(t *testing.T, cacheTag string, legacyETag string, isNegative bool, secs int64, nanos uint32) {
		payload := metadataJSONPayload{
			CacheTag:   cacheTag,
			LegacyETag: legacyETag,
			IsNegative: isNegative,
			CachedAt:   fuzzTime(secs, nanos),
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		var meta Metadata
		if err := json.Unmarshal(data, &meta); err != nil {
			t.Fatalf("unmarshal metadata: %v", err)
		}

		wantTag := normalizedJSONString(cacheTag)
		if wantTag == "" {
			wantTag = normalizedJSONString(legacyETag)
		}
		if meta.CacheTag != wantTag || meta.IsNegative != isNegative || !meta.CachedAt.Equal(payload.CachedAt) {
			t.Fatalf("legacy alias mismatch: got %+v want tag=%q negative=%v cachedAt=%v", meta, wantTag, isNegative, payload.CachedAt)
		}
	})
}

func fuzzTime(secs int64, nanos uint32) time.Time {
	// Keep generated times in a broad but JSON-safe range.
	const maxSpan = int64(200 * 365 * 24 * 60 * 60)
	if secs > maxSpan || secs < -maxSpan {
		secs %= maxSpan
	}
	return time.Unix(secs, int64(nanos%1_000_000_000)).UTC()
}

func normalizedJSONString(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	var normalized string
	if err := json.Unmarshal(data, &normalized); err != nil {
		panic(err)
	}
	return normalized
}
