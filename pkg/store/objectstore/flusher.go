package objectstore

import (
	"bytes"
	"context"
	"errors"
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
	baseEntries, err := s.loadCheckpointEntries(ctx, shardID)
	if err != nil {
		return err
	}
	records, err := s.pendingRecordsForShard(shardID, currentEntries)
	if err != nil {
		return err
	}
	mergedEntries := mergeCheckpointEntries(baseEntries, currentEntries, shardID)
	if len(records) == 0 {
		return s.publishCheckpoint(ctx, shardID, mergedEntries)
	}

	updates := make(map[string]localCatalogEntry, len(records))
	packedRecords := make([]pendingFlushRecord, 0, len(records))
	for _, record := range records {
		if s.shouldUploadDirect(record.entry) {
			if err := s.flushDirectRecord(ctx, record, currentEntries, updates, mergedEntries); err != nil {
				return err
			}
			continue
		}
		packedRecords = append(packedRecords, record)
	}

	if len(packedRecords) > 0 {
		if err := s.flushPackedRecords(ctx, shardID, packedRecords, currentEntries, updates, mergedEntries); err != nil {
			return err
		}
	}

	if err := s.publishCheckpoint(ctx, shardID, mergedEntries); err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	return s.commitFlushUpdates(currentEntries, updates)
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

func (s *Store) shouldUploadDirect(entry localCatalogEntry) bool {
	return s.packedThreshold > 0 && entry.Length > s.packedThreshold
}

func (s *Store) flushPackedRecords(
	ctx context.Context,
	shardID string,
	records []pendingFlushRecord,
	currentEntries map[string]localCatalogEntry,
	updates map[string]localCatalogEntry,
	mergedEntries map[string]checkpointEntry,
) error {
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

	for _, record := range records {
		current, ok := currentEntries[record.key]
		if !ok || current.Missing || current.SegmentPath != record.entry.SegmentPath {
			continue
		}
		current.RemotePath = remotePath
		current.RemoteOffset = offsets[record.key]
		updates[record.key] = current
		mergedEntries[record.key] = checkpointEntry{
			SegmentPath: current.RemotePath,
			Offset:      current.RemoteOffset,
			Length:      current.Length,
			Metadata:    current.Metadata,
		}
	}
	return nil
}

func (s *Store) flushDirectRecord(
	ctx context.Context,
	record pendingFlushRecord,
	currentEntries map[string]localCatalogEntry,
	updates map[string]localCatalogEntry,
	mergedEntries map[string]checkpointEntry,
) error {
	current, ok := currentEntries[record.key]
	if !ok || current.Missing || current.SegmentPath != record.entry.SegmentPath {
		return nil
	}

	remotePath := s.blobPath(record.key, s.nextVersion())
	file, err := os.Open(record.entry.SegmentPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := s.bucket.Upload(ctx, remotePath, io.NewSectionReader(file, record.entry.Offset, record.entry.Length)); err != nil {
		return err
	}

	current.RemotePath = remotePath
	current.RemoteOffset = 0
	updates[record.key] = current
	mergedEntries[record.key] = checkpointEntry{
		SegmentPath: current.RemotePath,
		Offset:      0,
		Length:      current.Length,
		Metadata:    current.Metadata,
	}
	return nil
}

func (s *Store) loadCheckpointEntries(ctx context.Context, shardID string) (map[string]checkpointEntry, error) {
	cp, err := s.loadCheckpointSnapshot(ctx, shardID)
	if err != nil {
		if errors.Is(err, daramjwee.ErrNotFound) {
			return make(map[string]checkpointEntry), nil
		}
		return nil, err
	}
	entries := make(map[string]checkpointEntry, len(cp.Entries))
	for key, entry := range cp.Entries {
		entries[key] = entry
	}
	return entries, nil
}

func mergeCheckpointEntries(base map[string]checkpointEntry, locals map[string]localCatalogEntry, shardID string) map[string]checkpointEntry {
	merged := make(map[string]checkpointEntry, len(base)+len(locals))
	for key, entry := range base {
		merged[key] = entry
	}
	for key, entry := range locals {
		if shardForKey(key) != shardID {
			continue
		}
		if entry.Missing {
			delete(merged, key)
			continue
		}
		if entry.RemotePath == "" {
			continue
		}
		merged[key] = checkpointEntry{
			SegmentPath: entry.RemotePath,
			Offset:      entry.RemoteOffset,
			Length:      entry.Length,
			Metadata:    entry.Metadata,
		}
	}
	return merged
}

func (s *Store) publishCheckpoint(ctx context.Context, shardID string, entries map[string]checkpointEntry) error {
	payload := checkpoint{
		UpdatedAt: s.now(),
		Entries:   make(map[string]checkpointEntry, len(entries)),
	}
	for key, entry := range entries {
		payload.Entries[key] = entry
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.bucket.Upload(ctx, internalshard.CheckpointObjectPath(s.prefix, shardID), bytes.NewReader(data))
}
