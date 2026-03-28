package objectstore

import (
	"context"
	"fmt"
	"io"

	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	internalshard "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/shard"
)

func (s *Store) loadCheckpointSnapshot(ctx context.Context, shardID string) (*checkpoint, error) {
	reader, err := s.bucket.Get(ctx, internalshard.CheckpointObjectPath(s.prefix, shardID))
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("objectstore: decode checkpoint for shard %q: %w", shardID, err)
	}
	return &cp, nil
}

func (s *Store) loadRemoteEntry(ctx context.Context, key string) (*checkpointEntry, error) {
	cp, err := s.loadCheckpointSnapshot(ctx, shardForKey(key))
	if err != nil {
		return nil, err
	}
	entry, ok := cp.Entries[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}
	return &entry, nil
}

func (s *Store) openRemoteEntry(ctx context.Context, entry checkpointEntry) (io.ReadCloser, error) {
	reader, err := s.bucket.GetRange(ctx, entry.SegmentPath, entry.Offset, entry.Length)
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	return reader, nil
}
