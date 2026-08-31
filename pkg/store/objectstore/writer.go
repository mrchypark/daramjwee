package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/mrchypark/daramjwee"
)

type segmentWriter interface {
	Write([]byte) (int, error)
	Seal() (string, int64, error)
	Abort() error
}

type writer struct {
	ctx        context.Context
	store      *Store
	key        string
	segment    segmentWriter
	metadata   *daramjwee.Metadata
	generation uint64

	mu      sync.Mutex
	done    bool
	doneCh  chan struct{}
	result  error
	aborted bool
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	done := w.done
	w.mu.Unlock()
	if done {
		return 0, io.ErrClosedPipe
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	return w.segment.Write(p)
}

func (w *writer) Close() error {
	return w.Commit(w.ctx)
}

func (w *writer) Commit(ctx context.Context) (result error) {
	if ctx == nil {
		ctx = context.Background()
	}
	started, previous := w.beginFinalize(false)
	if !started {
		return previous
	}
	defer func() { w.finish(result) }()
	defer w.store.writers.Done()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("objectstore: commit: %w", w.abortWith(err))
	}
	if err := w.ctx.Err(); err != nil {
		return fmt.Errorf("objectstore: commit: %w", w.abortWith(err))
	}

	sealedPath, size, err := w.segment.Seal()
	if err != nil {
		return fmt.Errorf("objectstore: commit: seal segment: %w", w.abortWith(err))
	}
	metadata := daramjwee.Metadata{}
	if w.metadata != nil {
		metadata = *w.metadata
	}
	// ponytail: process-wide publication fence; replace with per-key context locks if write latency becomes measurable.
	if err := w.store.flushRun.acquire(ctx); err != nil {
		_ = removeLocalSegment(sealedPath)
		return fmt.Errorf("objectstore: commit: publication fence: %w", err)
	}
	defer w.store.flushRun.release()
	published, err := w.store.publishLocalEntry(w.key, localCatalogEntry{
		SegmentPath:      sealedPath,
		Offset:           0,
		Length:           size,
		Generation:       w.generation,
		PublicationToken: w.store.nextVersion(),
		Metadata:         metadata,
	})
	if err != nil {
		if published {
			w.store.enqueueFlush(w.key)
		} else {
			_ = removeLocalSegment(sealedPath)
		}
		return err
	}
	if !published {
		_ = removeLocalSegment(sealedPath)
		return nil
	}
	w.store.enqueueFlush(w.key)
	return nil
}

func (w *writer) Abort() (result error) {
	started, previous := w.beginFinalize(true)
	if !started {
		return previous
	}
	defer func() { w.finish(result) }()
	defer w.store.writers.Done()
	return w.segment.Abort()
}

func (w *writer) abortWith(err error) error {
	if abortErr := w.segment.Abort(); abortErr != nil {
		return errors.Join(err, abortErr)
	}
	return err
}

func (w *writer) beginFinalize(abort bool) (bool, error) {
	w.mu.Lock()
	if w.done {
		doneCh := w.doneCh
		w.mu.Unlock()
		<-doneCh
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.aborted != abort {
			if abort {
				return false, nil
			}
			return false, io.ErrClosedPipe
		}
		return false, w.result
	}
	w.done = true
	w.aborted = abort
	w.mu.Unlock()
	return true, nil
}

func (w *writer) finish(err error) {
	w.mu.Lock()
	w.result = err
	close(w.doneCh)
	w.mu.Unlock()
}
