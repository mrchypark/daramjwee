package daramjwee

import (
	"strconv"
	"testing"
)

func FuzzIfNoneMatchMatchesCacheTag(f *testing.F) {
	seeds := [][2]string{
		{`"abc"`, "abc"},
		{`W/"abc"`, "abc"},
		{`"abc", "def"`, "def"},
		{`*`, "anything"},
		{"", "abc"},
		{`" spaced "`, " spaced "},
		{`W/"a,b"`, "a,b"},
	}
	for _, seed := range seeds {
		f.Add(seed[0], seed[1])
	}

	f.Fuzz(func(t *testing.T, ifNoneMatch string, cacheTag string) {
		match := ifNoneMatchMatchesCacheTag(ifNoneMatch, cacheTag)
		normalized := normalizeEntityTag(cacheTag)

		if normalized != "" && isSimpleEntityTag(normalized) {
			quoted := strconv.Quote(normalized)
			if !ifNoneMatchMatchesCacheTag(quoted, cacheTag) {
				t.Fatalf("exact quoted tag should match normalized cache tag %q", cacheTag)
			}
			if !ifNoneMatchMatchesCacheTag(`W/`+quoted, cacheTag) {
				t.Fatalf("weak quoted tag should match normalized cache tag %q", cacheTag)
			}
		}
		if ifNoneMatchMatchesCacheTag("", cacheTag) {
			t.Fatalf("empty If-None-Match must not match cache tag %q", cacheTag)
		}
		if !ifNoneMatchMatchesCacheTag("*", cacheTag) {
			t.Fatalf("wildcard If-None-Match must match cache tag %q", cacheTag)
		}
		if ifNoneMatchMatchesCacheTag(ifNoneMatch, "") && match {
			if !containsWildcardCandidate(ifNoneMatch) {
				t.Fatalf("empty cache tag should only match wildcard, got header %q", ifNoneMatch)
			}
		}
	})
}

func isSimpleEntityTag(tag string) bool {
	for _, r := range tag {
		if r < 0x20 || r > 0x7e || r == '"' || r == '\\' {
			return false
		}
	}
	return true
}

func containsWildcardCandidate(header string) bool {
	for _, candidate := range splitEntityTagList(header) {
		if normalizeEntityTag(candidate) == "*" {
			return true
		}
	}
	return false
}
