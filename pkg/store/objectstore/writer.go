package objectstore

import (
	"context"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee"
)

type writer struct {
	ctx     context.Context
	store   *Store
	key     string
	segment interface {
		Write([]byte) (int, error)
		Seal() (string, int64, error)
		Abort() error
	}
	metadata *daramjwee.Metadata

	mu   sync.Mutex
	done bool
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	done := w.done
	w.mu.Unlock()
	if done {
		return 0, io.ErrClosedPipe
	}

	return w.segment.Write(p)
}

func (w *writer) Close() error {
	if !w.markDone() {
		return nil
	}
	defer w.store.lockManager.Unlock(w.key)

	sealedPath, size, err := w.segment.Seal()
	if err != nil {
		return err
	}
	metadata := daramjwee.Metadata{}
	if w.metadata != nil {
		metadata = *w.metadata
	}
	err = w.store.publishLocalEntry(w.key, localCatalogEntry{
		SegmentPath: sealedPath,
		Offset:      0,
		Length:      size,
		Metadata:    metadata,
	})
	if err != nil {
		_ = removeLocalSegment(sealedPath)
		return err
	}
	w.store.enqueueFlush(w.key)
	return nil
}

func (w *writer) Abort() error {
	if !w.markDone() {
		return nil
	}
	defer w.store.lockManager.Unlock(w.key)
	return w.segment.Abort()
}

func (w *writer) markDone() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	return true
}
