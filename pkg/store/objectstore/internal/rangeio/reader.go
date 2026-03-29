package rangeio

import (
	"io"
	"sync"
)

type FetchPageFunc func(pageIndex int64) ([]byte, error)

type Reader struct {
	size      int64
	pageSize  int64
	fetchPage FetchPageFunc
	closeFn   func() error
	closeOnce sync.Once
	closeErr  error
	offset    int64
	pageIndex int64
	page      []byte
}

func New(size, pageSize int64, fetchPage FetchPageFunc, closeFn func() error) *Reader {
	return &Reader{
		size:      size,
		pageSize:  pageSize,
		fetchPage: fetchPage,
		closeFn:   closeFn,
		pageIndex: -1,
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	total := 0
	for total < len(p) && r.offset < r.size {
		index := r.offset / r.pageSize
		if err := r.ensurePage(index); err != nil {
			if total > 0 {
				return total, nil
			}
			return 0, err
		}

		pageOffset := r.offset % r.pageSize
		copied := copy(p[total:], r.page[pageOffset:])
		if copied == 0 {
			if total > 0 {
				return total, nil
			}
			return 0, io.ErrUnexpectedEOF
		}
		total += copied
		r.offset += int64(copied)
	}

	if r.offset >= r.size {
		return total, io.EOF
	}
	return total, nil
}

func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		if r.closeFn != nil {
			r.closeErr = r.closeFn()
		}
	})
	return r.closeErr
}

func (r *Reader) ensurePage(index int64) error {
	if r.pageIndex == index {
		return nil
	}
	page, err := r.fetchPage(index)
	if err != nil {
		return err
	}
	r.pageIndex = index
	r.page = page
	return nil
}
