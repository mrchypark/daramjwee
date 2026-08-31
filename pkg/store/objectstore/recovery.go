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
		if entry.Superseded {
			s.pendingShards[shardForKey(key)] = struct{}{}
			continue
		}
		if entry.Missing && (!entry.RemotePublished || entry.CleanupPending) {
			s.pendingShards[shardForKey(key)] = struct{}{}
			continue
		}
		if entry.Missing {
			continue
		}
		if entry.PendingRemotePath != "" {
			s.pendingShards[shardForKey(key)] = struct{}{}
		}
		if entry.IntentCleanupPending {
			s.pendingShards[shardForKey(key)] = struct{}{}
		}
		if entry.SegmentPath == "" {
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

		repaired := repairedEntryWithoutLocalSegment(entry)
		if _, err := s.publishLocalEntry(key, repaired); err != nil {
			return err
		}
		if repaired.PendingRemotePath != "" {
			s.pendingShards[shardForKey(key)] = struct{}{}
		}
	}
	for _, plan := range s.catalog.UploadPlans() {
		for _, member := range plan.Members {
			s.pendingShards[shardForKey(member.Key)] = struct{}{}
			break
		}
	}
	return nil
}
