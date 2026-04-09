package daramjwee

import (
	"encoding/json"
	"testing"
	"time"
	"unicode/utf8"
)

func FuzzMetadataJSONRoundTrip(f *testing.F) {
	f.Add("cache-v1", "legacy-v1", false, int64(0))
	f.Add("", "legacy-only", true, int64(1712664000))
	f.Add("rev,42", "", false, int64(253402300799))

	f.Fuzz(func(t *testing.T, cacheTag, legacyETag string, isNegative bool, unixSeconds int64) {
		cachedAt := fuzzTime(unixSeconds)

		original := Metadata{
			CacheTag:   cacheTag,
			IsNegative: isNegative,
			CachedAt:   cachedAt,
		}

		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal metadata: %v", err)
		}

		var roundTrip Metadata
		if err := json.Unmarshal(data, &roundTrip); err != nil {
			t.Fatalf("unmarshal round-trip metadata: %v", err)
		}
		if roundTrip.IsNegative != original.IsNegative || !roundTrip.CachedAt.Equal(original.CachedAt) {
			t.Fatalf("round trip mismatch: got %+v want %+v", roundTrip, original)
		}
		if utf8.ValidString(original.CacheTag) && roundTrip.CacheTag != original.CacheTag {
			t.Fatalf("cache tag mismatch: got %q want %q", roundTrip.CacheTag, original.CacheTag)
		}

		payloadBytes, err := json.Marshal(metadataJSONPayload{
			CacheTag:   cacheTag,
			LegacyETag: legacyETag,
			IsNegative: isNegative,
			CachedAt:   cachedAt,
		})
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}

		var decoded Metadata
		if err := json.Unmarshal(payloadBytes, &decoded); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}

		expectedTag := cacheTag
		if expectedTag == "" {
			expectedTag = legacyETag
		}
		if decoded.IsNegative != isNegative || !decoded.CachedAt.Equal(cachedAt) {
			t.Fatalf("decoded mismatch: got %+v", decoded)
		}
		if utf8.ValidString(expectedTag) && decoded.CacheTag != expectedTag {
			t.Fatalf("cache tag mismatch: got %q want %q", decoded.CacheTag, expectedTag)
		}
	})
}

func fuzzTime(unixSeconds int64) time.Time {
	const minUnix = -62135596800
	const span = 315537897599

	normalized := minUnix + int64(uint64(unixSeconds)%uint64(span+1))
	return time.Unix(normalized, 0).UTC()
}
