package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
	internalshard "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/shard"
)

const (
	flushDebounce = 10 * time.Millisecond
	flushRetryMin = 20 * time.Millisecond
	flushRetryMax = time.Second
)

var intentFinalizeTimeout = 5 * time.Second

var errPendingUploadPlanChanged = errors.New("objectstore: pending upload plan changed")

type checkpoint struct {
	UpdatedAt time.Time                  `json:"updated_at"`
	Entries   map[string]checkpointEntry `json:"entries"`
}

type checkpointEntry struct {
	SegmentPath      string             `json:"segment_path"`
	Offset           int64              `json:"offset"`
	Length           int64              `json:"length"`
	Generation       uint64             `json:"generation,omitempty"`
	Missing          bool               `json:"missing,omitempty"`
	PublicationToken string             `json:"publication_token,omitempty"`
	Metadata         daramjwee.Metadata `json:"metadata"`
}

type uploadIntent struct {
	RemotePath string `json:"remote_path"`
	Completed  bool   `json:"completed,omitempty"`
	Abandoned  bool   `json:"abandoned,omitempty"`
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
	if err := s.ensureReady(); err != nil {
		return err
	}
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
			s.reclaimDurableLocalSegments()
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

	updates := make(map[string]localCatalogEntry, len(records))
	packedRecords := make([]pendingFlushRecord, 0, len(records))
	pendingGroups := make(map[string][]pendingFlushRecord)
	for _, record := range records {
		if record.entry.PendingRemotePath != "" {
			pendingGroups[record.entry.PendingRemotePath] = append(pendingGroups[record.entry.PendingRemotePath], record)
			continue
		}
		if s.shouldUploadDirect(record.entry) {
			if err := s.flushDirectRecord(ctx, record, currentEntries, updates, mergedEntries); err != nil {
				return err
			}
			continue
		}
		packedRecords = append(packedRecords, record)
	}
	pendingPaths := make([]string, 0, len(pendingGroups))
	for remotePath := range pendingGroups {
		pendingPaths = append(pendingPaths, remotePath)
	}
	slices.Sort(pendingPaths)
	for _, remotePath := range pendingPaths {
		if err := s.flushPendingRemoteRecords(ctx, pendingGroups[remotePath], currentEntries, updates, mergedEntries); err != nil {
			return err
		}
	}

	if len(packedRecords) > 0 {
		if err := s.flushPackedRecords(ctx, shardID, packedRecords, currentEntries, updates, mergedEntries); err != nil {
			return err
		}
	}
	remoteEntries := make(map[string]localCatalogEntry, len(updates))
	cleanupEntries := make(map[string]localCatalogEntry)
	for key, entry := range currentEntries {
		if shardForKey(key) == shardID && entry.Missing && !entry.RemotePublished {
			remoteEntries[key] = entry
		}
		if shardForKey(key) == shardID && entry.Missing && entry.CleanupPending {
			cleanupEntries[key] = entry
		}
	}
	for key, entry := range updates {
		remoteEntries[key] = entry
	}
	conflicts, err := s.publishRemoteEntries(ctx, currentEntries, updates, remoteEntries)
	if err != nil {
		return err
	}
	for key, winner := range conflicts {
		if winner.Missing {
			delete(mergedEntries, key)
		} else {
			mergedEntries[key] = winner
		}
	}
	for key, entry := range remoteEntries {
		if merged, ok := mergedEntries[key]; ok {
			merged.PublicationToken = entry.PublicationToken
			mergedEntries[key] = merged
		}
	}
	for key, entry := range remoteEntries {
		cleanupEntries[key] = entry
	}

	if err := s.publishCheckpoint(ctx, shardID, mergedEntries); err != nil {
		return err
	}
	if err := s.commitFlushUpdates(currentEntries, updates); err != nil {
		return err
	}
	intentEntries := make(map[string]localCatalogEntry, len(updates))
	for key, entry := range currentEntries {
		if shardForKey(key) == shardID && entry.IntentCleanupPending {
			intentEntries[key] = entry
		}
	}
	for key, entry := range updates {
		intentEntries[key] = entry
	}
	if err := s.completeUploadIntents(ctx, intentEntries); err != nil {
		return err
	}
	if err := s.commitCleanedUploadIntents(intentEntries); err != nil {
		return err
	}
	if err := s.clearLegacyManifests(ctx, cleanupEntries); err != nil {
		return err
	}
	return s.commitCleanedTombstones(cleanupEntries)
}

func (s *Store) clearLegacyManifests(ctx context.Context, entries map[string]localCatalogEntry) error {
	for key, entry := range entries {
		if !entry.Missing {
			continue
		}
		if err := s.bucket.Delete(ctx, s.manifestPath(key)); err != nil && !s.bucket.IsObjNotFoundErr(err) {
			return err
		}
	}
	return nil
}

func (s *Store) completeUploadIntents(ctx context.Context, entries map[string]localCatalogEntry) error {
	cleared := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.RemotePath == "" {
			continue
		}
		if _, ok := cleared[entry.RemotePath]; ok {
			continue
		}
		cleared[entry.RemotePath] = struct{}{}
		data, err := json.Marshal(uploadIntent{RemotePath: entry.RemotePath, Completed: true})
		if err != nil {
			return err
		}
		if err := s.bucket.Upload(ctx, s.uploadIntentPath(entry.RemotePath), bytes.NewReader(data)); err != nil {
			return err
		}
		if err := s.bucket.Delete(ctx, s.uploadIntentPath(entry.RemotePath)); ignoreNotFound(err, s.bucket) != nil {
			_ = level.Warn(s.logger).Log("msg", "failed to clear remote upload intent", "path", entry.RemotePath, "err", err)
		}
	}
	return nil
}

func (s *Store) publishRemoteEntries(
	ctx context.Context,
	currentEntries map[string]localCatalogEntry,
	updates map[string]localCatalogEntry,
	entries map[string]localCatalogEntry,
) (map[string]checkpointEntry, error) {
	conflicts := make(map[string]checkpointEntry)
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		entry := entries[key]
		expected, ok := currentEntries[key]
		if !ok {
			delete(entries, key)
			delete(updates, key)
			continue
		}
		prepared, applied, err := s.prepareRemotePublication(ctx, key, expected)
		if err != nil {
			return nil, err
		}
		if !applied {
			delete(entries, key)
			delete(updates, key)
			continue
		}
		currentEntries[key] = prepared
		entry.PublicationToken = prepared.PublicationToken
		entry.RemoteVersionSet = prepared.RemoteVersionSet
		entry.RemoteVersionAbsent = prepared.RemoteVersionAbsent
		entry.RemoteVersionType = prepared.RemoteVersionType
		entry.RemoteVersionValue = prepared.RemoteVersionValue
		entries[key] = entry
		if update, ok := updates[key]; ok {
			update.PublicationToken = prepared.PublicationToken
			update.RemoteVersionSet = prepared.RemoteVersionSet
			update.RemoteVersionAbsent = prepared.RemoteVersionAbsent
			update.RemoteVersionType = prepared.RemoteVersionType
			update.RemoteVersionValue = prepared.RemoteVersionValue
			updates[key] = update
		}
		remoteEntry := checkpointEntry{
			SegmentPath:      entry.RemotePath,
			Offset:           entry.RemoteOffset,
			Length:           entry.Length,
			Generation:       entry.Generation,
			Missing:          entry.Missing,
			PublicationToken: entry.PublicationToken,
			Metadata:         entry.Metadata,
		}
		data, err := json.Marshal(remoteEntry)
		if err != nil {
			return nil, err
		}
		var uploadOpt objstore.ObjectUploadOption
		if prepared.RemoteVersionAbsent {
			uploadOpt = objstore.WithIfNotExists()
		} else {
			uploadOpt = objstore.WithIfMatch(&objstore.ObjectVersion{
				Type:  objstore.ObjectVersionType(prepared.RemoteVersionType),
				Value: prepared.RemoteVersionValue,
			})
		}
		if err := s.bucket.Upload(ctx, s.remoteEntryPath(key), bytes.NewReader(data), uploadOpt); err != nil {
			if !s.bucket.IsConditionNotMetErr(err) {
				return nil, err
			}
			winner, err := s.loadRemoteEntryFreshRaw(ctx, key)
			if err != nil {
				return nil, err
			}
			if winner.PublicationToken != entry.PublicationToken {
				if err := s.retireConflictedPublication(key, prepared, entry); err != nil {
					return nil, err
				}
				if latest, exists := s.catalog.Get(key); exists {
					currentEntries[key] = latest
				} else {
					delete(currentEntries, key)
				}
				delete(entries, key)
				delete(updates, key)
				conflicts[key] = winner
				s.checkpointCache.SetEntry(key, &winner, 0)
				continue
			}
		}
		s.checkpointCache.SetEntry(key, &remoteEntry, int64(len(data)))
		if entry.Missing {
			if err := s.commitPublishedTombstones(map[string]localCatalogEntry{key: entry}); err != nil {
				return nil, err
			}
		}
	}
	return conflicts, nil
}

func (s *Store) prepareRemotePublication(ctx context.Context, key string, expected localCatalogEntry) (localCatalogEntry, bool, error) {
	if expected.RemoteVersionSet {
		return expected, true, nil
	}
	prepared := expected
	if prepared.PublicationToken == "" {
		prepared.PublicationToken = s.nextVersion()
	}
	attrs, err := s.bucket.Attributes(ctx, s.remoteEntryPath(key))
	if err != nil {
		if !s.bucket.IsObjNotFoundErr(err) {
			return localCatalogEntry{}, false, err
		}
		prepared.RemoteVersionAbsent = true
	} else {
		if attrs.Version == nil {
			return localCatalogEntry{}, false, errors.New("objectstore: bucket did not return a version for conditional entry upload")
		}
		prepared.RemoteVersionType = int(attrs.Version.Type)
		prepared.RemoteVersionValue = attrs.Version.Value
	}
	prepared.RemoteVersionSet = true
	applied := false
	_, err = s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		if !exists || current != expected {
			return current, exists
		}
		applied = true
		return prepared, true
	})
	return prepared, applied, err
}

func (s *Store) loadRemoteEntryFreshRaw(ctx context.Context, key string) (checkpointEntry, error) {
	reader, err := s.bucket.Get(ctx, s.remoteEntryPath(key))
	if err != nil {
		return checkpointEntry{}, err
	}
	defer reader.Close()
	var entry checkpointEntry
	if _, err := decodeCheckpointEntry(reader, &entry); err != nil {
		return checkpointEntry{}, err
	}
	return entry, nil
}

func (s *Store) retireConflictedPublication(key string, expected, publication localCatalogEntry) error {
	var reclaim string
	_, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		if !exists || current != expected {
			return current, exists
		}
		if publication.Missing {
			reclaim = current.SegmentPath
			current.SegmentPath = ""
			current.Offset = 0
			current.RemotePublished = true
			current.CleanupPending = true
			current.Superseded = true
			return current, true
		}
		reclaim = current.SegmentPath
		current.SegmentPath = ""
		current.Offset = 0
		current.RemotePath = publication.RemotePath
		current.RemoteOffset = publication.RemoteOffset
		current.PendingRemotePath = ""
		current.PendingRemoteOffset = 0
		current.PendingRemoteSize = 0
		current.IntentCleanupPending = true
		current.Superseded = true
		return current, true
	})
	if err == nil && reclaim != "" {
		s.markLocalSegmentReclaimable(reclaim)
	}
	return err
}

func (s *Store) pendingRecordsForShard(shardID string, entries map[string]localCatalogEntry) ([]pendingFlushRecord, error) {
	records := make([]pendingFlushRecord, 0)
	for key, entry := range entries {
		if shardForKey(key) != shardID || entry.Missing || entry.RemotePath != "" || (entry.SegmentPath == "" && entry.PendingRemotePath == "") {
			continue
		}
		if entry.PendingRemotePath == "" {
			if _, err := os.Stat(entry.SegmentPath); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
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
	planned, applied, err := s.planPendingRemoteRecords(records, currentEntries, remotePath, offsets)
	if err != nil {
		_ = payload.Close()
		return err
	}
	if !applied {
		_ = payload.Close()
		return nil
	}
	if err := s.publishUploadIntent(ctx, remotePath); err != nil {
		_ = payload.Close()
		return err
	}
	if err := s.uploadPackedBody(ctx, remotePath, payload); err != nil {
		return errors.Join(err, s.abandonUploadIntent(ctx, remotePath))
	}

	for _, record := range planned {
		s.resumePendingRemote(record, currentEntries, updates, mergedEntries)
	}
	return nil
}

func (s *Store) uploadPackedBody(ctx context.Context, remotePath string, payload io.ReadCloser) error {
	uploadErr := s.bucket.Upload(ctx, remotePath, payload)
	return errors.Join(uploadErr, payload.Close())
}

func (s *Store) flushPendingRemoteRecords(
	ctx context.Context,
	records []pendingFlushRecord,
	currentEntries map[string]localCatalogEntry,
	updates map[string]localCatalogEntry,
	mergedEntries map[string]checkpointEntry,
) error {
	if len(records) == 0 {
		return nil
	}
	remotePath := records[0].entry.PendingRemotePath
	attrs, err := s.bucket.Attributes(ctx, remotePath)
	if err != nil && !s.bucket.IsObjNotFoundErr(err) {
		return err
	}
	bodyReady := err == nil && attrs.Size == records[0].entry.PendingRemoteSize
	if !bodyReady {
		if s.isPackedRemotePath(remotePath) {
			payload, offsets, err := newPackedRecordReader(records, nil)
			if err != nil {
				return err
			}
			defer payload.Close()
			var size int64
			for _, record := range records {
				if offsets[record.key] != record.entry.PendingRemoteOffset {
					return s.resetPendingRemoteRecords(ctx, records)
				}
				end := offsets[record.key] + record.entry.Length
				if end > size {
					size = end
				}
			}
			if size != records[0].entry.PendingRemoteSize {
				return s.resetPendingRemoteRecords(ctx, records)
			}
			releasePins, err := s.pinPackedSegments(records)
			if err != nil {
				return err
			}
			defer releasePins()
			if err := s.publishUploadIntent(ctx, remotePath); err != nil {
				return err
			}
			if err := s.uploadPackedBody(ctx, remotePath, payload); err != nil {
				return errors.Join(err, s.abandonUploadIntent(ctx, remotePath))
			}
		} else {
			if len(records) != 1 {
				return fmt.Errorf("objectstore: direct upload plan %q has %d records", remotePath, len(records))
			}
			record := records[0]
			file, err := os.Open(record.entry.SegmentPath)
			if err != nil {
				if os.IsNotExist(err) {
					return errMissingLocalEntry
				}
				return err
			}
			defer file.Close()
			if err := s.publishUploadIntent(ctx, remotePath); err != nil {
				return err
			}
			if err := s.bucket.Upload(ctx, remotePath, io.NewSectionReader(file, record.entry.Offset, record.entry.Length)); err != nil {
				return errors.Join(err, s.abandonUploadIntent(ctx, remotePath))
			}
		}
	}
	for _, record := range records {
		s.resumePendingRemote(record, currentEntries, updates, mergedEntries)
	}
	return nil
}

func (s *Store) resetPendingRemoteRecords(ctx context.Context, records []pendingFlushRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := s.abandonUploadIntent(ctx, records[0].entry.PendingRemotePath); err != nil {
		return err
	}
	expected := make(map[string]localCatalogEntry, len(records))
	cleared := make(map[string]localCatalogEntry, len(records))
	for _, record := range records {
		next := record.entry
		next.PendingRemotePath = ""
		next.PendingRemoteOffset = 0
		next.PendingRemoteSize = 0
		expected[record.key] = record.entry
		cleared[record.key] = next
	}
	if _, err := s.updateLocalEntriesIf(expected, cleared); err != nil {
		return err
	}
	return errPendingUploadPlanChanged
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

	pending, applied, err := s.persistPendingRemote(record.key, current, remotePath, 0, record.entry.Length)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	currentEntries[record.key] = pending
	current = pending
	if err := s.publishUploadIntent(ctx, remotePath); err != nil {
		return err
	}
	if err := s.bucket.Upload(ctx, remotePath, io.NewSectionReader(file, record.entry.Offset, record.entry.Length)); err != nil {
		return errors.Join(err, s.abandonUploadIntent(ctx, remotePath))
	}

	current.RemotePath = remotePath
	current.RemoteOffset = 0
	current.PendingRemotePath = ""
	current.PendingRemoteOffset = 0
	current.PendingRemoteSize = 0
	current.IntentCleanupPending = true
	current.SegmentPath = ""
	current.Offset = 0
	updates[record.key] = current
	mergedEntries[record.key] = checkpointEntry{
		SegmentPath:      remotePath,
		Offset:           0,
		Length:           current.Length,
		Generation:       current.Generation,
		PublicationToken: current.PublicationToken,
		Metadata:         current.Metadata,
	}
	return nil
}

func (s *Store) persistPendingRemote(key string, expected localCatalogEntry, remotePath string, remoteOffset, remoteSize int64) (localCatalogEntry, bool, error) {
	pending := expected
	pending.PendingRemotePath = remotePath
	pending.PendingRemoteOffset = remoteOffset
	pending.PendingRemoteSize = remoteSize
	if s.catalog == nil {
		return pending, true, nil
	}
	applied := false
	_, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		if !exists || current != expected {
			return current, exists
		}
		applied = true
		return pending, true
	})
	return pending, applied, err
}

func (s *Store) planPendingRemoteRecords(
	records []pendingFlushRecord,
	currentEntries map[string]localCatalogEntry,
	remotePath string,
	offsets map[string]int64,
) ([]pendingFlushRecord, bool, error) {
	expected := make(map[string]localCatalogEntry, len(records))
	plannedEntries := make(map[string]localCatalogEntry, len(records))
	planned := make([]pendingFlushRecord, 0, len(records))
	var size int64
	for _, record := range records {
		current, ok := currentEntries[record.key]
		if !ok || current != record.entry || current.Missing {
			return nil, false, nil
		}
		end := offsets[record.key] + current.Length
		if end > size {
			size = end
		}
	}
	for _, record := range records {
		current := currentEntries[record.key]
		pending := current
		pending.PendingRemotePath = remotePath
		pending.PendingRemoteOffset = offsets[record.key]
		pending.PendingRemoteSize = size
		expected[record.key] = current
		plannedEntries[record.key] = pending
		planned = append(planned, pendingFlushRecord{key: record.key, entry: pending})
	}
	applied, err := s.updateLocalEntriesIf(expected, plannedEntries)
	if err != nil || !applied {
		return nil, applied, err
	}
	for key, entry := range plannedEntries {
		currentEntries[key] = entry
	}
	return planned, true, nil
}

func (s *Store) resumePendingRemote(
	record pendingFlushRecord,
	currentEntries map[string]localCatalogEntry,
	updates map[string]localCatalogEntry,
	mergedEntries map[string]checkpointEntry,
) {
	current, ok := currentEntries[record.key]
	if !ok || current != record.entry || current.PendingRemotePath == "" {
		return
	}
	current.RemotePath = current.PendingRemotePath
	current.RemoteOffset = current.PendingRemoteOffset
	current.PendingRemotePath = ""
	current.PendingRemoteOffset = 0
	current.PendingRemoteSize = 0
	current.IntentCleanupPending = true
	current.SegmentPath = ""
	current.Offset = 0
	updates[record.key] = current
	mergedEntries[record.key] = checkpointEntry{
		SegmentPath:      current.RemotePath,
		Offset:           current.RemoteOffset,
		Length:           current.Length,
		Generation:       current.Generation,
		PublicationToken: current.PublicationToken,
		Metadata:         current.Metadata,
	}
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
		if entry.Superseded {
			continue
		}
		if entry.Missing && (!entry.RemotePublished || entry.CleanupPending) {
			delete(merged, key)
			continue
		}
		if entry.RemotePath == "" {
			continue
		}
		merged[key] = checkpointEntry{
			SegmentPath:      entry.RemotePath,
			Offset:           entry.RemoteOffset,
			Length:           entry.Length,
			Generation:       entry.Generation,
			PublicationToken: entry.PublicationToken,
			Metadata:         entry.Metadata,
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
