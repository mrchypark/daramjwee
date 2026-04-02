package shard

import (
	"fmt"
	"path"

	"github.com/zeebo/xxh3"
)

func ForKey(key string) string {
	return fmt.Sprintf("%02x", xxh3.HashString(key)%256)
}

func SegmentObjectPath(prefix, shardID, segmentID string) string {
	return join(prefix, "segments", shardID, segmentID+".seg")
}

func CheckpointObjectPath(prefix, shardID string) string {
	return join(prefix, "checkpoints", shardID, "latest.json")
}

func join(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	return path.Join(filtered...)
}
