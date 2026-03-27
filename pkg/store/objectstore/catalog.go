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
	if entry.Missing {
		return localCatalogEntry{}, false, errMissingLocalEntry
	}
	if _, statErr := os.Stat(entry.SegmentPath); statErr == nil {
		return entry, true, nil
	} else if !os.IsNotExist(statErr) {
		return localCatalogEntry{}, false, statErr
	}

	s.lockManager.Lock(key)
	defer s.lockManager.Unlock(key)

	latest, ok, err := s.loadLocalEntry(key)
	if err != nil || !ok {
		return localCatalogEntry{}, false, err
	}
	if latest.SegmentPath == entry.SegmentPath {
		if err := s.publishLocalEntry(key, localCatalogEntry{
			Missing:  true,
			Metadata: latest.Metadata,
		}); err != nil {
			return localCatalogEntry{}, false, err
		}
		return localCatalogEntry{}, false, errMissingLocalEntry
	}
	return localCatalogEntry{}, false, nil
}

func (s *Store) publishLocalEntry(key string, entry localCatalogEntry) error {
	if s.catalog == nil {
		return nil
	}
	prev, ok := s.catalog.Get(key)
	if err := s.catalog.Set(key, entry); err != nil {
		return err
	}
	if ok && prev.SegmentPath != "" && prev.SegmentPath != entry.SegmentPath {
		_ = removeLocalSegment(prev.SegmentPath)
	}
	return nil
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

func (s *Store) deleteLocalEntry(key string) error {
	if s.catalog == nil {
		return nil
	}
	prev, ok := s.catalog.Get(key)
	if err := s.catalog.Delete(key); err != nil {
		return err
	}
	if ok && prev.SegmentPath != "" {
		_ = removeLocalSegment(prev.SegmentPath)
	}
	return nil
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
