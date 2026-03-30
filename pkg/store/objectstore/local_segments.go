package objectstore

import (
	"io"
	"os"

	"github.com/go-kit/log/level"
)

var openLocalSegmentFile = os.Open

func (s *Store) openLocalEntry(entry localCatalogEntry) (io.ReadCloser, error) {
	file, err := openLocalSegmentFile(entry.SegmentPath)
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
	if refs := s.segmentRefs[segmentPath]; refs == 1 {
		delete(s.segmentRefs, segmentPath)
		_, shouldReclaim = s.reclaimableSegs[segmentPath]
		if shouldReclaim {
			delete(s.reclaimableSegs, segmentPath)
		}
	} else if refs > 1 {
		s.segmentRefs[segmentPath] = refs - 1
	}
	s.segmentRefsMu.Unlock()

	if shouldReclaim {
		s.reclaimLocalSegmentNow(segmentPath)
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
		s.reclaimLocalSegmentNow(segmentPath)
	}
}

func (s *Store) reclaimLocalSegmentNow(segmentPath string) {
	if err := removeLocalSegment(segmentPath); err != nil {
		level.Warn(s.logger).Log("msg", "failed to reclaim local segment file", "path", segmentPath, "err", err)
	}
}
