package objectstore

import (
	"context"
	"time"
)

type SweepStats struct {
	Scanned   int
	Deleted   int
	Failed    int
	Reachable int
}

// Sweep deletes unreachable remote objects older than the provided grace period.
// A zero grace period allows immediate reclamation of unreachable objects and
// stale non-latest checkpoints.
func (s *Store) Sweep(ctx context.Context, olderThan time.Duration) (SweepStats, error) {
	return s.Compact(ctx, olderThan)
}

// SweepStale deletes unreachable remote objects using the store default grace period.
func (s *Store) SweepStale(ctx context.Context) (SweepStats, error) {
	return s.Sweep(ctx, s.defaultGCGrace)
}
