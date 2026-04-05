package storetest

import "testing"

func TestBuildProdLikeWorkloadPreservesLegacyETagAlias(t *testing.T) {
	items, _, _ := BuildProdLikeWorkload()
	if len(items) == 0 {
		t.Fatal("expected prodlike workload items")
	}

	for _, item := range items {
		if item.CacheTag == "" {
			t.Fatalf("expected CacheTag for %q", item.Key)
		}
		if item.ETag != item.CacheTag {
			t.Fatalf("expected legacy ETag alias to match CacheTag for %q: etag=%q cacheTag=%q", item.Key, item.ETag, item.CacheTag)
		}
	}
}
