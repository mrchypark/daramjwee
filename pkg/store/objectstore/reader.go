package objectstore

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/blockcache"
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

func decodeCheckpoint(reader io.Reader, cp *checkpoint) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, cp); err != nil {
		return err
	}
	return nil
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
	if s.isPackedRemotePath(entry.SegmentPath) {
		packedCtx, cancel := context.WithCancel(ctx)
		return &packedRemoteReader{
			ctx:       packedCtx,
			cancel:    cancel,
			store:     s,
			entry:     entry,
			blockSize: s.pageSize,
			blockIdx:  -1,
		}, nil
	}

	reader, err := s.bucket.GetRange(ctx, entry.SegmentPath, entry.Offset, entry.Length)
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	return reader, nil
}

func (s *Store) isPackedRemotePath(remotePath string) bool {
	return strings.HasPrefix(remotePath, ensureDir(joinPath(s.prefix, "segments")))
}

func (s *Store) loadPackedBlock(ctx context.Context, remotePath string, blockIndex int64) ([]byte, error) {
	key := blockcache.Key{Path: remotePath, Index: blockIndex}
	if block, ok := s.blockCache.Get(key); ok {
		return block, nil
	}

	value, err, _ := s.blockLoads.Do(key.String(), func() (any, error) {
		if block, ok := s.blockCache.Get(key); ok {
			return block, nil
		}
		reader, err := s.bucket.GetRange(ctx, remotePath, blockIndex*s.pageSize, s.pageSize)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil, daramjwee.ErrNotFound
			}
			return nil, err
		}
		defer reader.Close()

		block, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		s.blockCache.Set(key, block)
		return block, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

type packedRemoteReader struct {
	ctx       context.Context
	cancel    context.CancelFunc
	store     *Store
	entry     checkpointEntry
	blockSize int64
	offset    int64
	blockIdx  int64
	block     []byte
}

func (r *packedRemoteReader) Read(p []byte) (int, error) {
	if r.offset >= r.entry.Length {
		return 0, io.EOF
	}

	total := 0
	for total < len(p) && r.offset < r.entry.Length {
		absoluteOffset := r.entry.Offset + r.offset
		blockIdx := absoluteOffset / r.blockSize
		if err := r.ensureBlock(blockIdx); err != nil {
			if total > 0 {
				return total, nil
			}
			return 0, err
		}

		blockOffset := absoluteOffset % r.blockSize
		remaining := r.entry.Length - r.offset
		available := int64(len(r.block)) - blockOffset
		if available > remaining {
			available = remaining
		}
		copied := copy(p[total:], r.block[blockOffset:blockOffset+available])
		total += copied
		r.offset += int64(copied)
	}

	if r.offset >= r.entry.Length {
		return total, io.EOF
	}
	return total, nil
}

func (r *packedRemoteReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (r *packedRemoteReader) ensureBlock(blockIdx int64) error {
	if r.blockIdx == blockIdx {
		return nil
	}
	block, err := r.store.loadPackedBlock(r.ctx, r.entry.SegmentPath, blockIdx)
	if err != nil {
		return err
	}
	r.blockIdx = blockIdx
	r.block = block
	return nil
}
