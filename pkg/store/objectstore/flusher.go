package objectstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	internalshard "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/shard"
)

const flushDebounce = 10 * time.Millisecond

type checkpoint struct {
	UpdatedAt time.Time                  `json:"updated_at"`
	Entries   map[string]checkpointEntry `json:"entries"`
}

type checkpointEntry struct {
	SegmentPath string             `json:"segment_path"`
	Offset      int64              `json:"offset"`
	Length      int64              `json:"length"`
	Metadata    daramjwee.Metadata `json:"metadata"`
}

type pendingFlushRecord struct {
	key   string
	entry localCatalogEntry
}

func (s *Store) enqueueFlush(key string) {
	shardID := shardForKey(key)
	s.flushMu.Lock()
	s.pendingShards[shardID] = struct{}{}
	if s.autoFlush {
		s.scheduleFlushLocked()
	}
	s.flushMu.Unlock()
}

func (s *Store) scheduleFlushLocked() {
	if s.flushTimer != nil {
		return
	}
	s.flushTimer = time.AfterFunc(flushDebounce, func() {
		if err := s.flushPending(context.Background()); err != nil {
			level.Warn(s.logger).Log("msg", "objectstore flush failed", "err", err)
		}
		s.flushMu.Lock()
		s.flushTimer = nil
		if len(s.pendingShards) > 0 {
			s.scheduleFlushLocked()
		}
		s.flushMu.Unlock()
	})
}

func (s *Store) flushPending(ctx context.Context) error {
	s.flushRunMu.Lock()
	defer s.flushRunMu.Unlock()

	for {
		shards := s.takePendingShards()
		if len(shards) == 0 {
			return nil
		}
		for idx, shardID := range shards {
			if err := s.flushShard(ctx, shardID); err != nil {
				s.requeueShards(shards[idx:])
				return err
			}
		}
	}
}

func (s *Store) takePendingShards() []string {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	if len(s.pendingShards) == 0 {
		return nil
	}
	shards := make([]string, 0, len(s.pendingShards))
	for shardID := range s.pendingShards {
		shards = append(shards, shardID)
	}
	clear(s.pendingShards)
	slices.Sort(shards)
	return shards
}

func (s *Store) requeueShards(shards []string) {
	if len(shards) == 0 {
		return
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	for _, shardID := range shards {
		s.pendingShards[shardID] = struct{}{}
	}
}

func (s *Store) flushShard(ctx context.Context, shardID string) error {
	currentEntries := s.catalog.Entries()
	records, err := s.pendingRecordsForShard(shardID, currentEntries)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return s.publishCheckpoint(ctx, shardID, currentEntries)
	}

	segmentID := s.nextVersion()
	remotePath := internalshard.SegmentObjectPath(s.prefix, shardID, segmentID)

	payload := bytes.NewBuffer(nil)
	offsets := make(map[string]int64, len(records))
	for _, record := range records {
		offsets[record.key] = int64(payload.Len())
		file, err := os.Open(record.entry.SegmentPath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(payload, io.NewSectionReader(file, record.entry.Offset, record.entry.Length)); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}

	if err := s.bucket.Upload(ctx, remotePath, bytes.NewReader(payload.Bytes())); err != nil {
		return err
	}

	updates := make(map[string]localCatalogEntry, len(records))
	mergedEntries := mapsClone(currentEntries)
	for _, record := range records {
		current, ok := currentEntries[record.key]
		if !ok || current.Missing || current.SegmentPath != record.entry.SegmentPath {
			continue
		}
		current.RemotePath = remotePath
		current.RemoteOffset = offsets[record.key]
		updates[record.key] = current
		mergedEntries[record.key] = current
	}

	if err := s.publishCheckpoint(ctx, shardID, mergedEntries); err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	return s.updateLocalEntries(updates)
}

func (s *Store) pendingRecordsForShard(shardID string, entries map[string]localCatalogEntry) ([]pendingFlushRecord, error) {
	records := make([]pendingFlushRecord, 0)
	for key, entry := range entries {
		if shardForKey(key) != shardID || entry.Missing || entry.SegmentPath == "" || entry.RemotePath != "" {
			continue
		}
		if _, err := os.Stat(entry.SegmentPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		records = append(records, pendingFlushRecord{key: key, entry: entry})
	}
	slices.SortFunc(records, func(a, b pendingFlushRecord) int {
		return strings.Compare(a.key, b.key)
	})
	return records, nil
}

func (s *Store) publishCheckpoint(ctx context.Context, shardID string, entries map[string]localCatalogEntry) error {
	payload := checkpoint{
		UpdatedAt: s.now(),
		Entries:   make(map[string]checkpointEntry),
	}
	for key, entry := range entries {
		if shardForKey(key) != shardID || entry.Missing || entry.RemotePath == "" {
			continue
		}
		payload.Entries[key] = checkpointEntry{
			SegmentPath: entry.RemotePath,
			Offset:      entry.RemoteOffset,
			Length:      entry.Length,
			Metadata:    entry.Metadata,
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.bucket.Upload(ctx, internalshard.CheckpointObjectPath(s.prefix, shardID), bytes.NewReader(data))
}

func mapsClone(entries map[string]localCatalogEntry) map[string]localCatalogEntry {
	cloned := make(map[string]localCatalogEntry, len(entries))
	for key, entry := range entries {
		cloned[key] = entry
	}
	return cloned
}
