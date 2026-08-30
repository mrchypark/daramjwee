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

const (
	flushDebounce = 10 * time.Millisecond
	flushRetryMin = 20 * time.Millisecond
	flushRetryMax = time.Second
)

type checkpoint struct {
	UpdatedAt time.Time                  `json:"updated_at"`
	Entries   map[string]checkpointEntry `json:"entries"`
}

type checkpointEntry struct {
	SegmentPath string             `json:"segment_path"`
	Offset      int64              `json:"offset"`
	Length      int64              `json:"length"`
	Generation  uint64             `json:"generation,omitempty"`
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
	if !s.autoFlush || s.flushScheduled {
		return
	}
	delay := flushDebounce
	if s.flushRetryDelay > delay {
		delay = s.flushRetryDelay
	}
	s.flushScheduled = true
	s.scheduleFlushAfter(delay, func() {
		if err := s.flushRun.acquire(context.Background()); err != nil {
			return
		}
		defer s.flushRun.release()

		s.flushMu.Lock()
		if !s.autoFlush {
			s.flushScheduled = false
			s.flushMu.Unlock()
			return
		}
		s.flushMu.Unlock()
		if s.afterAutoFlushCheck != nil {
			s.afterAutoFlushCheck()
		}

		err := s.flushPendingAcquired(context.Background())
		if err != nil {
			_ = level.Warn(s.logger).Log("msg", "objectstore flush failed", "err", err)
		}
		s.flushMu.Lock()
		s.flushScheduled = false
		if err != nil {
			s.flushRetryDelay = nextFlushRetryDelay(s.flushRetryDelay)
		} else {
			s.flushRetryDelay = 0
		}
		if len(s.pendingShards) > 0 {
			s.scheduleFlushLocked()
		}
		s.flushMu.Unlock()
	})
}

func nextFlushRetryDelay(current time.Duration) time.Duration {
	if current < flushRetryMin {
		return flushRetryMin
	}
	if current >= flushRetryMax/2 {
		return flushRetryMax
	}
	return current * 2
}

func (s *Store) flushPending(ctx context.Context) error {
	if s.beforeFlushAcquire != nil {
		s.beforeFlushAcquire()
	}
	if err := s.flushRun.acquire(ctx); err != nil {
		return err
	}
	defer s.flushRun.release()
	return s.flushPendingAcquired(ctx)
}

func (s *Store) flushPendingAcquired(ctx context.Context) error {
	for {
		if s.syncCatalog != nil {
			if err := s.syncCatalog(); err != nil {
				return err
			}
		}
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

	// Compaction must see the upload and its checkpoint/catalog publication as
	// one transition, or it can sweep the newly uploaded object in between.
	if err := s.remoteState.acquire(ctx); err != nil {
		return err
	}
	defer s.remoteState.release()

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
	return s.packThreshold > 0 && entry.Length > s.packThreshold
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

	payload, offsets, err := newPackedRecordReader(records, nil)
	if err != nil {
		return err
	}
	releasePins, err := s.pinPackedSegments(records)
	if err != nil {
		return err
	}
	defer releasePins()
	if err := s.uploadPackedBody(ctx, remotePath, payload); err != nil {
		return err
	}

	for _, record := range records {
		current, ok := currentEntries[record.key]
		if !ok || current.Missing || current.SegmentPath != record.entry.SegmentPath {
			continue
		}
		current.RemotePath = remotePath
		current.RemoteOffset = offsets[record.key]
		current.SegmentPath = ""
		current.Offset = 0
		updates[record.key] = current
		mergedEntries[record.key] = checkpointEntry{
			SegmentPath: remotePath,
			Offset:      offsets[record.key],
			Length:      current.Length,
			Generation:  current.Generation,
			Metadata:    current.Metadata,
		}
	}
	return nil
}

func (s *Store) uploadPackedBody(ctx context.Context, remotePath string, payload io.ReadCloser) error {
	uploadErr := s.bucket.Upload(ctx, remotePath, payload)
	return errors.Join(uploadErr, payload.Close())
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
	current.SegmentPath = ""
	current.Offset = 0
	updates[record.key] = current
	mergedEntries[record.key] = checkpointEntry{
		SegmentPath: remotePath,
		Offset:      0,
		Length:      current.Length,
		Generation:  current.Generation,
		Metadata:    current.Metadata,
	}
	return nil
}

func (s *Store) loadCheckpointEntries(ctx context.Context, shardID string) (map[string]checkpointEntry, error) {
	cp, err := s.loadCheckpointSnapshotFresh(ctx, shardID)
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
			Generation:  entry.Generation,
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
	if err := s.bucket.Upload(ctx, internalshard.CheckpointObjectPath(s.prefix, shardID), bytes.NewReader(data)); err != nil {
		return err
	}
	s.checkpointCache.Set(shardID, &payload, int64(len(data)))
	return nil
}
