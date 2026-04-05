package daramjwee

import "testing"

func TestIfNoneMatchMatchesCacheTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ifNoneMatch string
		cacheTag    string
		want        bool
	}{
		{name: "exact bare match", ifNoneMatch: "cache-v1", cacheTag: "cache-v1", want: true},
		{name: "quoted match", ifNoneMatch: "\"cache-v1\"", cacheTag: "cache-v1", want: true},
		{name: "weak match", ifNoneMatch: "W/\"cache-v1\"", cacheTag: "cache-v1", want: true},
		{name: "comma separated match", ifNoneMatch: "\"other\", W/\"cache-v1\"", cacheTag: "cache-v1", want: true},
		{name: "wildcard", ifNoneMatch: "*", cacheTag: "cache-v1", want: true},
		{name: "no match", ifNoneMatch: "\"other\"", cacheTag: "cache-v1", want: false},
		{name: "empty header", ifNoneMatch: "", cacheTag: "cache-v1", want: false},
		{name: "empty cache tag", ifNoneMatch: "\"cache-v1\"", cacheTag: "", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ifNoneMatchMatchesCacheTag(tt.ifNoneMatch, tt.cacheTag); got != tt.want {
				t.Fatalf("ifNoneMatchMatchesCacheTag(%q, %q) = %v, want %v", tt.ifNoneMatch, tt.cacheTag, got, tt.want)
			}
		})
	}
}
