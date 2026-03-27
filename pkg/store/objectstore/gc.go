package objectstore

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/thanos-io/objstore"
)

type SweepStats struct {
	Scanned   int
	Deleted   int
	Failed    int
	Reachable int
}

// Sweep deletes unreachable blob generations older than the provided grace period.
// A zero grace period allows immediate reclamation of unreachable blobs.
func (s *Store) Sweep(ctx context.Context, olderThan time.Duration) (SweepStats, error) {
	var stats SweepStats

	reachable := make(map[string]struct{})
	err := s.bucket.Iter(ctx, s.manifestRoot(), func(name string) error {
		stats.Scanned++
		reader, err := s.bucket.Get(ctx, name)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			return err
		}
		defer reader.Close()

		var m manifest
		if err := json.NewDecoder(reader).Decode(&m); err != nil {
			return nil
		}
		if m.BlobPath != "" {
			reachable[m.BlobPath] = struct{}{}
			stats.Reachable++
		}
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return stats, err
	}

	cutoff := s.now().Add(-olderThan)
	err = s.bucket.Iter(ctx, s.blobRoot(), func(name string) error {
		if _, ok := reachable[name]; ok {
			return nil
		}
		createdAt, ok := blobTimestampFromPath(name)
		if !ok || createdAt.After(cutoff) {
			return nil
		}
		if err := s.bucket.Delete(ctx, name); err != nil {
			if ignoreNotFound(err, s.bucket) != nil {
				stats.Failed++
				level.Warn(s.logger).Log("msg", "gc failed to delete orphan blob", "blob_path", name, "err", err)
			}
			return nil
		}
		stats.Deleted++
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return stats, err
	}

	level.Debug(s.logger).Log("msg", "objectstore sweep complete", "scanned", stats.Scanned, "reachable", stats.Reachable, "deleted", stats.Deleted, "failed", stats.Failed)
	return stats, nil
}

// SweepStale deletes unreachable blobs using the store default grace period.
func (s *Store) SweepStale(ctx context.Context) (SweepStats, error) {
	return s.Sweep(ctx, s.defaultGCGrace)
}
