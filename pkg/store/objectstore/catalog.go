package objectstore

import (
	"errors"
	"os"
	"path/filepath"

	internalcatalog "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/catalog"
)

type localCatalogEntry = internalcatalog.Entry

var (
	// ErrAmbiguousCommit means a catalog snapshot was renamed but its directory
	// durability could not be confirmed. The store remains fail-closed until
	// the catalog is explicitly reconciled.
	ErrAmbiguousCommit   = internalcatalog.ErrAmbiguousCommit
	errMissingLocalEntry = errors.New("objectstore: missing local entry data")
)

func (s *Store) loadLocalEntry(key string) (localCatalogEntry, bool, error) {
	if s.catalog == nil {
		return localCatalogEntry{}, false, nil
	}
	entry, ok := s.catalog.Get(key)
	return entry, ok, nil //nolint:unparam // error always nil; kept for interface consistency
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
		current, ok, err := s.loadLocalEntry(key) //nolint:govet // shadow: retry after lock acquisition
		if err != nil || !ok {
			return localCatalogEntry{}, ok, err
		}
		resolved, live, needsRepair, err = resolveLocalEntry(current)
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
		if entry.RemotePublished {
			return localCatalogEntry{}, false, false, nil
		}
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
	committed, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
		prev, ok = current, exists
		if exists && current.Generation > entry.Generation {
			staleSeen = true
			return current, true
		}
		applied = true
		return entry, true
	})
	published := committed && applied && !staleSeen
	if published && ok && prev.SegmentPath != "" && prev.SegmentPath != entry.SegmentPath {
		if err == nil {
			s.markLocalSegmentReclaimable(prev.SegmentPath)
		} else {
			s.deferLocalSegmentReclaim(prev.SegmentPath)
		}
	}
	return published, err
}

func (s *Store) updateLocalEntry(key string, fn func(localCatalogEntry, bool) (localCatalogEntry, bool)) (bool, error) {
	if s.updateCatalog == nil {
		return true, nil
	}
	return s.updateCatalog(key, fn)
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
		committed, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
			if !exists || current != expected {
				return current, exists
			}
			applied = true
			return next, true
		})
		if committed && applied && expected.SegmentPath != "" && expected.SegmentPath != next.SegmentPath {
			if err == nil {
				s.markLocalSegmentReclaimable(expected.SegmentPath)
			} else {
				s.deferLocalSegmentReclaim(expected.SegmentPath)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) commitPublishedTombstones(expectedEntries map[string]localCatalogEntry) error {
	if s.catalog == nil {
		return nil
	}
	for key, expected := range expectedEntries {
		if !expected.Missing || expected.RemotePublished {
			continue
		}
		_, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
			if !exists || current != expected {
				return current, exists
			}
			current.RemotePublished = true
			return current, true
		})
		if err != nil {
			return err
		}
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
	committed, err := s.updateLocalEntry(key, func(current localCatalogEntry, exists bool) (localCatalogEntry, bool) {
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
	})
	published := committed && applied && !staleSeen
	if published && previousSegment != "" {
		if err == nil {
			s.markLocalSegmentReclaimable(previousSegment)
		} else {
			s.deferLocalSegmentReclaim(previousSegment)
		}
	}
	return published, err
}

func removeLocalSegment(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func sweepLocalSegmentFiles(root string, keep func(string) bool) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".seg" {
			return nil
		}
		if keep != nil && keep(path) {
			return nil
		}
		return removeLocalSegment(path)
	})
}

func (s *Store) sweepOrphanedLocalSegments() error {
	if s.dataDir == "" {
		return nil
	}

	activeRoot := filepath.Join(s.dataDir, "ingest", "active")
	if err := sweepLocalSegmentFiles(activeRoot, nil); err != nil {
		return err
	}
	if s.catalog == nil {
		return nil
	}

	referenced := make(map[string]struct{})
	for _, entry := range s.catalog.Entries() {
		if entry.SegmentPath != "" {
			referenced[entry.SegmentPath] = struct{}{}
		}
	}

	sealedRoot := filepath.Join(s.dataDir, "ingest", "sealed")
	return sweepLocalSegmentFiles(sealedRoot, func(path string) bool {
		if _, ok := referenced[path]; ok {
			return true
		}
		return false
	})
}
