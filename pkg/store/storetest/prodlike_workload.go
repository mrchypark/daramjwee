package storetest

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
)

type ProdLikeWorkloadItem struct {
	Key      string
	Category string
	Body     []byte
	ETag     string
	CacheTag string
}

func BuildProdLikeWorkload() ([]ProdLikeWorkloadItem, map[string]int, int64) {
	type spec struct {
		category string
		count    int
		size     int
		prefix   string
	}

	specs := []spec{
		{category: "metrics_negative_15m", count: 240, size: 77, prefix: "metrics/device/panel/15m/metrics_negative_15m"},
		{category: "plant_small", count: 16, size: 800, prefix: "plant/plant/metadata/plant_small"},
		{category: "registry_medium", count: 8, size: 32 << 10, prefix: "registry/registry/registry_medium"},
		{category: "blueprint_medium", count: 8, size: 96 << 10, prefix: "blueprint/blueprint/manager/blueprint_medium"},
		{category: "metrics_positive_15m_medium", count: 12, size: 128 << 10, prefix: "metrics/device/panel/15m/metrics_positive_15m_medium"},
		{category: "metrics_positive_5m_large", count: 6, size: 768 << 10, prefix: "metrics/device/panel/5m/metrics_positive_5m_large"},
		{category: "metrics_positive_5m_direct", count: 2, size: 2 << 20, prefix: "metrics/device/panel/5m/metrics_positive_5m_direct"},
	}

	items := make([]ProdLikeWorkloadItem, 0, 292)
	counts := make(map[string]int, len(specs))
	var totalBytes int64

	for _, spec := range specs {
		for i := 0; i < spec.count; i++ {
			key := fmt.Sprintf("%s/%03d/%s", spec.prefix, i, workloadDate(i))
			body := workloadBody(spec.category, i, spec.size)
			cacheTag := fmt.Sprintf("%s-%03d", spec.category, i)
			items = append(items, ProdLikeWorkloadItem{
				Key:      key,
				Category: spec.category,
				Body:     body,
				ETag:     cacheTag,
				CacheTag: cacheTag,
			})
			counts[spec.category]++
			totalBytes += int64(len(body))
		}
	}

	return items, counts, totalBytes
}

func workloadDate(i int) string {
	if i%2 == 0 {
		return fmt.Sprintf("2026-03-%02d", (i%28)+1)
	}
	return fmt.Sprintf("2025-03-%02d", (i%28)+1)
}

func workloadBody(category string, index, size int) []byte {
	sum := sha1.Sum([]byte(fmt.Sprintf("%s-%03d", category, index)))
	pattern := []byte(hex.EncodeToString(sum[:]))
	body := make([]byte, size)
	for i := range body {
		body[i] = pattern[i%len(pattern)]
	}
	return body
}
