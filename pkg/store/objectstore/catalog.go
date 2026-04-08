package objectstore

import (
	"errors"
	"os"
	"path/filepath"

	internalcatalog "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/catalog"
)

type localCatalogEntry = internalcatalog.Entry

var errMissingLocalEntry = errors.New("objectstore: missing local entry data")

func (s *Store) loadLocalEntry(key string) (localCatalogEntry, bool, error) {
	if s.catalog == nil {
		return localCatalogEntry{}, false, nil
	}
	entry, ok := s.catalog.Get(key)
	return entry, ok, nil
}

func (s *Store) loadLiveLocalEntry(key string) (localCatalogEntry, bool, error) {
	entry, ok, err := s.loadLocalEntry(key)
	if err != nil || !ok {
		return localCatalogEntry{}, ok, err
	}
	resolved, live, needsRepair, err := resolveLocalEntry(entry)
	if err != nil || live || !needsRepair {
		return resolved, live, err
	}

	s.lockManager.Lock(key)
	defer s.lockManager.Unlock(key)

	latest, ok, err := s.loadLocalEntry(key)
	if err != nil || !ok {
		return localCatalogEntry{}, false, err
	}
	resolved, live, needsRepair, err = resolveLocalEntry(latest)
	if err != nil || live || !needsRepair {
		return resolved, live, err
	}

	repaired := repairedEntryWithoutLocalSegment(latest)
	published, err := s.publishLocalEntry(key, repaired)
	if err != nil {
		return localCatalogEntry{}, false, err
	}
	if !published {
		current, ok, err := s.loadLocalEntry(key)
		if err != nil || !ok {
			return localCatalogEntry{}, ok, err
		}
		resolved, live, needsRepair, err := resolveLocalEntry(current)
		if needsRepair {
			repairedCurrent := repairedEntryWithoutLocalSegment(current)
			if repairedCurrent.Missing {
				return localCatalogEntry{}, false, errMissingLocalEntry
			}
			return localCatalogEntry{}, false, nil
		}
		return resolved, live, err
	}
	resolved, live, _, err = resolveLocalEntry(repaired)
	return resolved, live, err
}

func resolveLocalEntry(entry localCatalogEntry) (localCatalogEntry, bool, bool, error) {
	if entry.Missing {
		return localCatalogEntry{}, false, false, errMissingLocalEntry
	}
	if entry.SegmentPath == "" {
		return localCatalogEntry{}, false, false, nil
	}
	if _, err := os.Stat(entry.SegmentPath); err == nil {
		return entry, true, false, nil
	} else if !os.IsNotExist(err) {
		return localCatalogEntry{}, false, false, err
	}
	return localCatalogEntry{}, false, true, nil
}

func repairedEntryWithoutLocalSegment(entry localCatalogEntry) localCatalogEntry {
	entry.SegmentPath = ""
	entry.Offset = 0
	if entry.RemotePath != "" {
		return entry
	}
	entry.Missing = true
	return entry
}

func (s *Store) publishLocalEntry(key string, entry localCatalogEntry) (bool, error) {
	if s.catalog == nil {
		return true, nil
	}
	s.observeGeneration(entry.Generation)
	var (
		prev      localCatalogEntry
		ok        bool
		applied   bool
		staleSeen bool
	)
	if err := s.catalog.Update(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		prev, ok = current, exists
		if exists && current.Generation > entry.Generation {
			staleSeen = true
			return current, true
		}
		applied = true
		return entry, true
	}); err != nil {
		return false, err
	}
	if staleSeen || !applied {
		return false, nil
	}
	if ok && prev.SegmentPath != "" && prev.SegmentPath != entry.SegmentPath {
		s.markLocalSegmentReclaimable(prev.SegmentPath)
	}
	return true, nil
}

func (s *Store) updateLocalEntry(key string, fn func(localCatalogEntry, bool) (localCatalogEntry, bool)) error {
	if s.catalog == nil {
		return nil
	}
	return s.catalog.Update(key, fn)
}

func (s *Store) updateLocalEntries(entries map[string]localCatalogEntry) error {
	if s.catalog == nil {
		return nil
	}
	return s.catalog.UpdateMany(entries)
}

func (s *Store) commitFlushUpdates(expectedEntries, updates map[string]localCatalogEntry) error {
	if s.catalog == nil || len(updates) == 0 {
		return nil
	}
	for key, next := range updates {
		expected, ok := expectedEntries[key]
		if !ok {
			continue
		}
		applied := false
		if err := s.catalog.Update(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
			if !exists || current != expected {
				return current, exists
			}
			applied = true
			return next, true
		}); err != nil {
			return err
		}
		if applied && expected.SegmentPath != "" && expected.SegmentPath != next.SegmentPath {
			s.markLocalSegmentReclaimable(expected.SegmentPath)
		}
	}
	return nil
}

func (s *Store) deleteLocalEntry(key string) error {
	if s.catalog == nil {
		return nil
	}
	prev, ok := s.catalog.Get(key)
	if err := s.catalog.Delete(key); err != nil {
		return err
	}
	if ok && prev.SegmentPath != "" {
		s.markLocalSegmentReclaimable(prev.SegmentPath)
	}
	return nil
}

func (s *Store) publishDeleteTombstone(key string, generation uint64) (bool, error) {
	if s.catalog == nil {
		return true, nil
	}
	s.observeGeneration(generation)
	var (
		previousSegment string
		applied         bool
		staleSeen       bool
	)
	if err := s.catalog.Update(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		if exists && current.Generation > generation {
			staleSeen = true
			return current, true
		}
		applied = true
		if current.SegmentPath != "" {
			previousSegment = current.SegmentPath
		}
		tombstone := localCatalogEntry{
			Generation: generation,
			Missing:    true,
			Metadata:   current.Metadata,
		}
		return tombstone, true
	}); err != nil {
		return false, err
	}
	if staleSeen || !applied {
		return false, nil
	}
	if previousSegment != "" {
		s.markLocalSegmentReclaimable(previousSegment)
	}
	return true, nil
}

func removeLocalSegment(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Store) sweepOrphanedLocalSegments() error {
	if s.catalog == nil || s.dataDir == "" {
		return nil
	}

	referenced := make(map[string]struct{})
	for _, entry := range s.catalog.Entries() {
		if entry.SegmentPath != "" {
			referenced[entry.SegmentPath] = struct{}{}
		}
	}

	sealedRoot := filepath.Join(s.dataDir, "ingest", "sealed")
	return filepath.WalkDir(sealedRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".seg" {
			return nil
		}
		if _, ok := referenced[path]; ok {
			return nil
		}
		if err := removeLocalSegment(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	})
}
