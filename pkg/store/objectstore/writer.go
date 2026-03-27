package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
)

type writer struct {
	ctx      context.Context
	store    *Store
	key      string
	blobPath string
	metadata *daramjwee.Metadata
	pw       *io.PipeWriter
	wg       sync.WaitGroup

	mu        sync.Mutex
	done      bool
	size      int64
	uploadErr error
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	done := w.done
	w.mu.Unlock()
	if done {
		return 0, io.ErrClosedPipe
	}

	n, err := w.pw.Write(p)
	w.mu.Lock()
	w.size += int64(n)
	w.mu.Unlock()
	return n, err
}

func (w *writer) Close() error {
	if !w.markDone() {
		return nil
	}
	defer w.store.lockManager.Unlock(w.key)

	if err := w.pw.Close(); err != nil {
		return err
	}
	w.wg.Wait()

	if w.uploadErr != nil {
		level.Error(w.store.logger).Log("msg", "blob upload failed", "key", w.key, "blob_path", w.blobPath, "err", w.uploadErr)
		return fmt.Errorf("objectstore: blob upload for %q failed: %w", w.key, w.uploadErr)
	}

	if err := w.store.publishManifest(w.ctx, w.key, w.blobPath, w.currentSize(), w.metadata); err != nil {
		cleanupErr := ignoreNotFound(w.store.bucket.Delete(context.Background(), w.blobPath), w.store.bucket)
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("cleanup failed: %w", cleanupErr))
		}
		level.Error(w.store.logger).Log("msg", "manifest publish failed", "key", w.key, "blob_path", w.blobPath, "err", err)
		return fmt.Errorf("objectstore: manifest publish for %q failed: %w", w.key, err)
	}

	return nil
}

func (w *writer) Abort() error {
	if !w.markDone() {
		return nil
	}
	defer w.store.lockManager.Unlock(w.key)

	closeErr := w.pw.CloseWithError(context.Canceled)
	w.wg.Wait()

	deleteErr := ignoreNotFound(w.store.bucket.Delete(context.Background(), w.blobPath), w.store.bucket)
	return errors.Join(closeErr, deleteErr)
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

func (w *writer) currentSize() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}
