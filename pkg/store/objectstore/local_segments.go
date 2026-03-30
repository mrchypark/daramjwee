package objectstore

import (
	"io"
	"os"
)

func (s *Store) openLocalEntry(entry localCatalogEntry) (io.ReadCloser, error) {
	file, err := os.Open(entry.SegmentPath)
	if err != nil {
		return nil, err
	}
	s.acquireLocalSegment(entry.SegmentPath)
	section := io.NewSectionReader(file, entry.Offset, entry.Length)
	return &fileSectionReadCloser{
		Reader: section,
		closeFn: func() error {
			err := file.Close()
			s.releaseLocalSegment(entry.SegmentPath)
			return err
		},
	}, nil
}

func (s *Store) acquireLocalSegment(segmentPath string) {
	if segmentPath == "" {
		return
	}
	s.segmentRefsMu.Lock()
	defer s.segmentRefsMu.Unlock()
	s.segmentRefs[segmentPath]++
}

func (s *Store) releaseLocalSegment(segmentPath string) {
	if segmentPath == "" {
		return
	}

	var shouldReclaim bool
	s.segmentRefsMu.Lock()
	if refs := s.segmentRefs[segmentPath]; refs <= 1 {
		delete(s.segmentRefs, segmentPath)
		_, shouldReclaim = s.reclaimableSegs[segmentPath]
		if shouldReclaim {
			delete(s.reclaimableSegs, segmentPath)
		}
	} else {
		s.segmentRefs[segmentPath] = refs - 1
	}
	s.segmentRefsMu.Unlock()

	if shouldReclaim {
		_ = removeLocalSegment(segmentPath)
	}
}

func (s *Store) markLocalSegmentReclaimable(segmentPath string) {
	if segmentPath == "" {
		return
	}

	var shouldReclaim bool
	s.segmentRefsMu.Lock()
	if s.segmentRefs[segmentPath] == 0 {
		shouldReclaim = true
	} else {
		s.reclaimableSegs[segmentPath] = struct{}{}
	}
	s.segmentRefsMu.Unlock()

	if shouldReclaim {
		_ = removeLocalSegment(segmentPath)
	}
}
