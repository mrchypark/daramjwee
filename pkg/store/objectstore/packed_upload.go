package objectstore

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

type packedSourceFile interface {
	io.ReaderAt
	io.Closer
}

type packedSourceOpener func(string) (packedSourceFile, error)

type packedRecordReader struct {
	records   []pendingFlushRecord
	open      packedSourceOpener
	next      int
	current   packedSourceFile
	section   *io.SectionReader
	remaining int64
	closed    bool
	done      bool
}

func newPackedRecordReader(
	records []pendingFlushRecord,
	open packedSourceOpener,
) (*packedRecordReader, map[string]int64, error) {
	if open == nil {
		open = openPackedSourceFile
	}

	offsets := make(map[string]int64, len(records))
	var offset int64
	for _, record := range records {
		if record.entry.Offset < 0 || record.entry.Length < 0 {
			return nil, nil, fmt.Errorf("objectstore: invalid packed record bounds for %q", record.key)
		}
		if record.entry.Offset > math.MaxInt64-record.entry.Length {
			return nil, nil, fmt.Errorf("objectstore: packed record bounds overflow for %q", record.key)
		}
		if record.entry.Length > math.MaxInt64-offset {
			return nil, nil, fmt.Errorf("objectstore: packed upload size overflow")
		}
		offsets[record.key] = offset
		offset += record.entry.Length
	}

	return &packedRecordReader{
		records: records,
		open:    open,
	}, offsets, nil
}

func (s *Store) pinPackedSegments(records []pendingFlushRecord) (func(), error) {
	seen := make(map[string]struct{}, len(records))
	pinned := make([]string, 0, len(records))
	release := func() {
		for index := len(pinned) - 1; index >= 0; index-- {
			s.releaseLocalSegment(pinned[index])
		}
	}

	for _, record := range records {
		segmentPath := record.entry.SegmentPath
		if _, ok := seen[segmentPath]; ok {
			continue
		}
		seen[segmentPath] = struct{}{}
		s.acquireLocalSegment(segmentPath)
		pinned = append(pinned, segmentPath)
		if _, err := os.Stat(segmentPath); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

func openPackedSourceFile(path string) (packedSourceFile, error) {
	return os.Open(path)
}

func (r *packedRecordReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.done {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	read := 0
	for len(p) > 0 {
		if r.current == nil {
			if r.next == len(r.records) {
				r.done = true
				if read > 0 {
					return read, nil
				}
				return 0, io.EOF
			}
			if err := r.openCurrent(); err != nil {
				r.closed = true
				return read, err
			}
			if r.remaining == 0 {
				if err := r.finishCurrent(); err != nil {
					r.closed = true
					return read, err
				}
				continue
			}
		}

		n, readErr := r.section.Read(p)
		read += n
		p = p[n:]
		r.remaining -= int64(n)

		if r.remaining == 0 {
			if errors.Is(readErr, io.EOF) {
				readErr = nil
			}
			closeErr := r.finishCurrent()
			if readErr != nil || closeErr != nil {
				r.closed = true
				return read, errors.Join(readErr, closeErr)
			}
			continue
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				readErr = io.ErrUnexpectedEOF
			}
			closeErr := r.finishCurrent()
			r.closed = true
			return read, errors.Join(readErr, closeErr)
		}
		if n == 0 {
			closeErr := r.finishCurrent()
			r.closed = true
			return read, errors.Join(io.ErrNoProgress, closeErr)
		}
	}
	return read, nil
}

func (r *packedRecordReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.current == nil {
		return nil
	}
	return r.closeCurrent()
}

func (r *packedRecordReader) openCurrent() error {
	record := r.records[r.next]
	file, err := r.open(record.entry.SegmentPath)
	if err != nil {
		return err
	}
	r.current = file
	r.section = io.NewSectionReader(file, record.entry.Offset, record.entry.Length)
	r.remaining = record.entry.Length
	return nil
}

func (r *packedRecordReader) finishCurrent() error {
	err := r.closeCurrent()
	r.next++
	return err
}

func (r *packedRecordReader) closeCurrent() error {
	err := r.current.Close()
	r.current = nil
	r.section = nil
	r.remaining = 0
	return err
}
