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
	if err := s.reclaimRemoteObjects(ctx, ensureDir(joinPath(s.prefix, "segments")), reachable, cutoff, &stats); err != nil {
		return stats, err
	}
	if err := s.reclaimRemoteObjects(ctx, s.blobRoot(), reachable, cutoff, &stats); err != nil {
		return stats, err
	}
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
		reachable[m.BlobPath] = struct{}{}
		stats.Reachable++
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

func (s *Store) reclaimRemoteObjects(ctx context.Context, root string, reachable map[string]struct{}, cutoff time.Time, stats *SweepStats) error {
	if root == "" {
		return nil
	}
	return s.bucket.Iter(ctx, root, func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		if _, ok := reachable[name]; ok {
			return nil
		}
		createdAt, ok := objectTimestampFromPath(name)
		if !ok || createdAt.After(cutoff) {
			return nil
		}
		if err := s.bucket.Delete(ctx, name); err != nil {
			if ignoreNotFound(err, s.bucket) != nil {
				stats.Failed++
				_ = level.Warn(s.logger).Log("msg", "failed to reclaim remote object", "path", name, "err", err)
			}
			return nil
		}
		stats.Deleted++
		return nil
	}, objstore.WithRecursiveIter())
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
