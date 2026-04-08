package objectstore

import "os"

func (s *Store) recoverLocalState() error {
	if err := s.sweepOrphanedLocalSegments(); err != nil {
		return err
	}
	if s.catalog == nil {
		return nil
	}

	for key, entry := range s.catalog.Entries() {
		s.observeGeneration(entry.Generation)
		if entry.Missing || entry.SegmentPath == "" {
			continue
		}
		if _, err := os.Stat(entry.SegmentPath); err == nil {
			if entry.RemotePath == "" {
				s.pendingShards[shardForKey(key)] = struct{}{}
			}
			continue
		} else if !os.IsNotExist(err) {
			return err
		}

		if _, err := s.publishLocalEntry(key, repairedEntryWithoutLocalSegment(entry)); err != nil {
			return err
		}
	}
	return nil
}
