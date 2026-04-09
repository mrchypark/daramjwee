package daramjwee

import (
	"strings"
	"testing"
)

func FuzzIfNoneMatchMatchesCacheTag(f *testing.F) {
	seeds := []struct {
		header   string
		cacheTag string
	}{
		{header: `W/"cache-v1"`, cacheTag: "cache-v1"},
		{header: `"other", "cache-v1"`, cacheTag: `"cache-v1"`},
		{header: `"rev,42"`, cacheTag: "rev,42"},
		{header: "*", cacheTag: ""},
		{header: "", cacheTag: "cache-v1"},
	}
	for _, seed := range seeds {
		f.Add(seed.header, seed.cacheTag)
	}

	f.Fuzz(func(t *testing.T, header, cacheTag string) {
		_ = ifNoneMatchMatchesCacheTag(header, cacheTag)

		if ifNoneMatchMatchesCacheTag("*", cacheTag) != true {
			t.Fatalf("wildcard should always match cacheTag %q", cacheTag)
		}

		if strings.TrimSpace(header) == "" && ifNoneMatchMatchesCacheTag(header, cacheTag) {
			t.Fatalf("empty header unexpectedly matched cacheTag %q", cacheTag)
		}

		normalizedTag := normalizeEntityTag(cacheTag)
		if normalizedTag == "" {
			return
		}
		if strings.ContainsAny(normalizedTag, `"\`) {
			return
		}

		quoted := `"` + escapeEntityTag(normalizedTag) + `"`
		var variants []string
		if !strings.Contains(normalizedTag, ",") && normalizedTag == strings.TrimSpace(normalizedTag) && normalizedTag != "" {
			variants = append(variants, normalizedTag)
		}
		variants = append(variants,
			quoted,
			"W/"+quoted,
			`"other", W/`+quoted,
		)
		for _, variant := range variants {
			if !ifNoneMatchMatchesCacheTag(variant, cacheTag) {
				t.Fatalf("variant %q did not match cacheTag %q", variant, cacheTag)
			}
		}
	})
}

func escapeEntityTag(tag string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(tag)
}
