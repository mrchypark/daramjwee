package objectstore

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/thanos-io/objstore"
)

func (s *Store) Compact(ctx context.Context, olderThan time.Duration) (SweepStats, error) {
	var stats SweepStats
	if err := s.ensureReady(); err != nil {
		return stats, err
	}
	if err := s.remoteState.acquire(ctx); err != nil {
		return stats, err
	}
	defer s.remoteState.release()

	reachable, err := s.collectReachableRemotePaths(ctx, &stats)
	if err != nil {
		return stats, err
	}

	cutoff := s.now().Add(-olderThan)
	immediate := olderThan <= 0
	candidates, err := s.collectRemoteCandidates(ctx, ensureDir(joinPath(s.prefix, "segments")), cutoff)
	if err != nil {
		return stats, err
	}
	blobCandidates, err := s.collectRemoteCandidates(ctx, s.blobRoot(), cutoff)
	if err != nil {
		return stats, err
	}
	candidates = append(candidates, blobCandidates...)

	// Re-read references after fixing the candidate set. An uploader either
	// appears in this snapshot or uploaded too late to be a candidate.
	latest := make(map[string]struct{})
	if err := s.collectUploadIntentPaths(ctx, latest, &SweepStats{}); err != nil {
		return stats, err
	}
	latestSnapshot, err := s.collectReachableRemotePaths(ctx, &SweepStats{})
	if err != nil {
		return stats, err
	}
	for name := range latestSnapshot {
		latest[name] = struct{}{}
	}
	for name := range latest {
		reachable[name] = struct{}{}
	}
	s.reclaimRemoteObjects(ctx, candidates, reachable, &stats)
	if err := s.pruneStaleCheckpoints(ctx, cutoff, immediate, &stats); err != nil {
		return stats, err
	}

	_ = level.Debug(s.logger).Log("msg", "objectstore compaction complete", "scanned", stats.Scanned, "reachable", stats.Reachable, "deleted", stats.Deleted, "failed", stats.Failed)
	return stats, nil
}

func (s *Store) CompactStale(ctx context.Context) (SweepStats, error) {
	return s.Compact(ctx, s.gcGrace)
}

func (s *Store) collectReachableRemotePaths(ctx context.Context, stats *SweepStats) (map[string]struct{}, error) {
	reachable := make(map[string]struct{})
	remoteEntryKeys := make(map[string]struct{})
	legacyManifests := make(map[string]string)

	if s.catalog != nil {
		for _, entry := range s.catalog.Entries() {
			if entry.Missing || entry.RemotePath == "" {
				continue
			}
			reachable[entry.RemotePath] = struct{}{}
		}
	}

	err := s.bucket.Iter(ctx, ensureDir(joinPath(s.prefix, "manifests")), func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		stats.Scanned++
		reader, err := s.bucket.Get(ctx, name) //nolint:govet // shadow: iterator callback error handling
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			return err
		}
		defer reader.Close()

		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		var m manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		if m.BlobPath == "" {
			return fmt.Errorf("objectstore: compact: manifest %q is missing blob_path", name)
		}
		base := path.Base(name)
		if !strings.HasSuffix(base, ".json") {
			return fmt.Errorf("objectstore: manifest %q has an invalid name", name)
		}
		key, err := decodeKey(strings.TrimSuffix(base, ".json"))
		if err != nil {
			return fmt.Errorf("objectstore: decode manifest key %q: %w", name, err)
		}
		legacyManifests[key] = m.BlobPath
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, err
	}

	err = s.bucket.Iter(ctx, ensureDir(joinPath(s.prefix, "entries")), func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		stats.Scanned++
		reader, err := s.bucket.Get(ctx, name)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			return err
		}
		defer reader.Close()
		var entry checkpointEntry
		if _, err := decodeCheckpointEntry(reader, &entry); err != nil {
			return err
		}
		base := path.Base(name)
		if !strings.HasSuffix(base, ".json") {
			return fmt.Errorf("objectstore: remote entry %q has an invalid name", name)
		}
		key, err := decodeKey(strings.TrimSuffix(base, ".json"))
		if err != nil {
			return fmt.Errorf("objectstore: decode remote entry key %q: %w", name, err)
		}
		remoteEntryKeys[key] = struct{}{}
		if entry.Missing {
			return nil
		}
		if entry.SegmentPath == "" {
			return fmt.Errorf("objectstore: compact: remote entry %q is missing segment_path", name)
		}
		reachable[entry.SegmentPath] = struct{}{}
		stats.Reachable++
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, err
	}
	for key, blobPath := range legacyManifests {
		if _, authoritative := remoteEntryKeys[key]; authoritative {
			continue
		}
		reachable[blobPath] = struct{}{}
		stats.Reachable++
	}

	if err := s.collectUploadIntentPaths(ctx, reachable, stats); err != nil {
		return nil, err
	}

	err = s.bucket.Iter(ctx, ensureDir(joinPath(s.prefix, "checkpoints")), func(name string) error {
		if path.Base(name) != "latest.json" {
			return nil
		}
		stats.Scanned++
		reader, err := s.bucket.Get(ctx, name)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			return err
		}
		defer reader.Close()

		var cp checkpoint
		if err := decodeCheckpoint(reader, &cp); err != nil {
			return err
		}
		if cp.Entries == nil {
			return fmt.Errorf("objectstore: compact: checkpoint %q is missing entries", name)
		}
		for key, entry := range cp.Entries {
			if _, authoritative := remoteEntryKeys[key]; authoritative {
				continue
			}
			if entry.SegmentPath == "" {
				return fmt.Errorf("objectstore: compact: checkpoint %q entry %q is missing segment_path", name, key)
			}
			reachable[entry.SegmentPath] = struct{}{}
			stats.Reachable++
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, err
	}
	return reachable, nil
}

func (s *Store) collectUploadIntentPaths(ctx context.Context, reachable map[string]struct{}, stats *SweepStats) error {
	return s.bucket.Iter(ctx, ensureDir(joinPath(s.prefix, "uploads")), func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		stats.Scanned++
		reader, err := s.bucket.Get(ctx, name)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			return err
		}
		defer reader.Close()
		var intent uploadIntent
		if _, err := decodeUploadIntent(reader, &intent); err != nil {
			return fmt.Errorf("objectstore: decode upload intent %q: %w", name, err)
		}
		if !s.isPackedRemotePath(intent.RemotePath) && !strings.HasPrefix(intent.RemotePath, s.blobRoot()) {
			return fmt.Errorf("objectstore: upload intent %q has invalid remote_path", name)
		}
		reachable[intent.RemotePath] = struct{}{}
		stats.Reachable++
		return nil
	}, objstore.WithRecursiveIter())
}

func decodeUploadIntent(reader io.Reader, intent *uploadIntent) (int64, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), json.Unmarshal(data, intent)
}

func (s *Store) collectRemoteCandidates(ctx context.Context, root string, cutoff time.Time) ([]string, error) {
	if root == "" {
		return nil, nil
	}
	var candidates []string
	err := s.bucket.Iter(ctx, root, func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		createdAt, ok := objectTimestampFromPath(name)
		if !ok || createdAt.After(cutoff) {
			return nil
		}
		candidates = append(candidates, name)
		return nil
	}, objstore.WithRecursiveIter())
	return candidates, err
}

func (s *Store) reclaimRemoteObjects(ctx context.Context, candidates []string, reachable map[string]struct{}, stats *SweepStats) {
	for _, name := range candidates {
		if _, ok := reachable[name]; ok {
			continue
		}
		if err := s.bucket.Delete(ctx, name); err != nil {
			if ignoreNotFound(err, s.bucket) != nil {
				stats.Failed++
				_ = level.Warn(s.logger).Log("msg", "failed to reclaim remote object", "path", name, "err", err)
			}
			continue
		}
		stats.Deleted++
	}
}

func (s *Store) pruneStaleCheckpoints(ctx context.Context, cutoff time.Time, immediate bool, stats *SweepStats) error {
	root := ensureDir(joinPath(s.prefix, "checkpoints"))
	if root == "" {
		return nil
	}
	return s.bucket.Iter(ctx, root, func(name string) error {
		if strings.HasSuffix(name, "/") || path.Base(name) == "latest.json" {
			return nil
		}
		if !immediate {
			createdAt, ok := objectTimestampFromPath(name)
			if !ok || createdAt.After(cutoff) {
				return nil
			}
		}
		if err := s.bucket.Delete(ctx, name); err != nil {
			if ignoreNotFound(err, s.bucket) != nil {
				stats.Failed++
				_ = level.Warn(s.logger).Log("msg", "failed to prune stale checkpoint", "path", name, "err", err)
			}
			return nil
		}
		stats.Deleted++
		return nil
	}, objstore.WithRecursiveIter())
}
