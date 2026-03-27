package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/goccy/go-json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
	"github.com/zeebo/xxh3"
	"golang.org/x/sync/errgroup"
)

// objstoreAdapter wraps an objstore.Bucket to implement the daramjwee.Store interface.
// This implementation manages metadata (like ETags) by storing a separate '.meta.json'
// object for each data object. It supports true streaming for uploads.
type objstoreAdapter struct {
	bucket      objstore.Bucket
	logger      log.Logger
	lockManager *keyLockManager
}

// NewObjstoreAdapter creates a new adapter.
func NewObjstoreAdapter(bucket objstore.Bucket, logger log.Logger) daramjwee.Store {
	return &objstoreAdapter{
		bucket:      bucket,
		logger:      logger,
		lockManager: newKeyLockManager(2048),
	}
}

// GetStream retrieves an object as a stream from the object storage.
// It first fetches the metadata and then the actual data object.
func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	meta, err := a.Stat(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	dataPath := a.toDataPath(key)
	r, err := a.bucket.Get(ctx, dataPath)
	if err != nil {
		level.Warn(a.logger).Log("msg", "failed to get data object even though metadata exists", "key", key, "err", err)
		return nil, nil, fmt.Errorf("failed to get object for key '%s' after meta check: %w", key, err)
	}

	return r, meta, nil
}

// BeginSet returns a WriteSink that enables true streaming uploads.
// It uses an io.Pipe, allowing the caller to write data chunk by chunk,
// which is concurrently uploaded to the object store in a separate goroutine.
// This is highly memory-efficient as the entire object does not need to be
// buffered in memory before the upload begins.
func (a *objstoreAdapter) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	a.lockManager.Lock(key)
	pr, pw := io.Pipe()

	writer := &streamingObjstoreWriter{
		ctx:      ctx,
		adapter:  a,
		key:      key,
		metadata: metadata,
		pw:       pw,
		wg:       &sync.WaitGroup{},
	}

	writer.wg.Add(1)
	go func() {
		defer writer.wg.Done()
		dataPath := a.toDataPath(key)
		writer.uploadErr = a.bucket.Upload(ctx, dataPath, pr)
	}()

	return writer, nil
}

func (a *objstoreAdapter) ValidateHotStore() error {
	return &daramjwee.ConfigError{Message: "unsupported hot store: objstore adapter"}
}

// Delete removes an object and its associated metadata from the object storage.
// It attempts to delete both the data object and the metadata object concurrently.
func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	a.lockManager.Lock(key)
	defer a.lockManager.Unlock(key)

	g, gCtx := errgroup.WithContext(ctx)

	dataPath := a.toDataPath(key)
	metaPath := a.toMetaPath(key)

	// Delete data object
	g.Go(func() error {
		if err := a.bucket.Delete(gCtx, dataPath); err != nil {
			if a.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			level.Error(a.logger).Log("msg", "failed to delete data object", "key", dataPath, "err", err)
			return err
		}
		return nil
	})

	// Delete meta object
	g.Go(func() error {
		if err := a.bucket.Delete(gCtx, metaPath); err != nil {
			if a.bucket.IsObjNotFoundErr(err) {
				return nil
			}
			level.Error(a.logger).Log("msg", "failed to delete meta object", "key", metaPath, "err", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

// Stat retrieves metadata for an object from the object storage without fetching the data.
func (a *objstoreAdapter) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	metaPath := a.toMetaPath(key)

	r, err := a.bucket.Get(ctx, metaPath)
	if err != nil {
		if a.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get metadata object for key '%s': %w", key, err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			level.Warn(a.logger).Log("msg", "failed to close reader in Stat", "key", key, "err", err)
		}
	}()

	metaBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata for key '%s': %w", key, err)
	}

	var metadata daramjwee.Metadata
	if err := json.Unmarshal(metaBytes, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata for key '%s': %w", key, err)
	}

	return &metadata, nil
}

// toDataPath returns the path for the data object.
func (a *objstoreAdapter) toDataPath(key string) string {
	return key
}

// toMetaPath returns the path for the metadata object.
func (a *objstoreAdapter) toMetaPath(key string) string {
	return key + ".meta.json"
}

// streamingObjstoreWriter handles the true streaming upload process.
type streamingObjstoreWriter struct {
	ctx       context.Context
	adapter   *objstoreAdapter
	key       string
	metadata  *daramjwee.Metadata
	pw        *io.PipeWriter
	wg        *sync.WaitGroup
	uploadErr error
	mu        sync.Mutex
	done      bool
}

// Write writes data to the pipe, which is immediately streamed to the object store.
func (w *streamingObjstoreWriter) Write(p []byte) (n int, err error) {
	if w.isDone() {
		return 0, io.ErrClosedPipe
	}
	return w.pw.Write(p)
}

// Close finalizes the write operation. It closes the pipe, waits for the data
// upload to finish, and then uploads the metadata object.
func (w *streamingObjstoreWriter) Close() error {
	if !w.markDone() {
		return nil
	}
	defer w.adapter.lockManager.Unlock(w.key)

	// 1. Close the pipe writer. This signals the end of the stream to the
	//    background upload goroutine and causes it to complete.
	if err := w.pw.Close(); err != nil {
		return err
	}

	// 2. Wait for the background upload to finish.
	w.wg.Wait()

	// 3. Check if the data upload failed.
	if w.uploadErr != nil {
		level.Error(w.adapter.logger).Log("msg", "data object upload failed", "key", w.key, "err", w.uploadErr)
		return fmt.Errorf("data upload for key '%s' failed: %w", w.key, w.uploadErr)
	}

	// 4. If data upload was successful, upload the metadata object.
	metaPath := w.adapter.toMetaPath(w.key)
	metaBytes, err := json.Marshal(w.metadata)
	if err != nil {
		if deleteErr := w.adapter.bucket.Delete(context.Background(), w.adapter.toDataPath(w.key)); deleteErr != nil {
			level.Error(w.adapter.logger).Log("msg", "failed to clean up data object after metadata marshal failure", "key", w.key, "err", deleteErr)
			err = errors.Join(err, fmt.Errorf("cleanup failed: %w", deleteErr))
		}
		level.Error(w.adapter.logger).Log("msg", "failed to marshal metadata", "key", metaPath, "err", err)
		return fmt.Errorf("failed to marshal metadata for key '%s': %w", w.key, err)
	}

	err = w.adapter.bucket.Upload(w.ctx, metaPath, bytes.NewReader(metaBytes))
	if err != nil {
		if deleteErr := w.adapter.bucket.Delete(context.Background(), w.adapter.toDataPath(w.key)); deleteErr != nil {
			level.Error(w.adapter.logger).Log("msg", "failed to clean up data object after metadata upload failure", "key", w.key, "err", deleteErr)
			err = errors.Join(err, fmt.Errorf("cleanup failed: %w", deleteErr))
		}
		level.Error(w.adapter.logger).Log("msg", "failed to upload metadata object", "key", metaPath, "err", err)
		return fmt.Errorf("failed to upload metadata for key '%s': %w", w.key, err)
	}

	level.Debug(w.adapter.logger).Log("msg", "successfully uploaded data and metadata", "key", w.key)
	return nil
}

func (w *streamingObjstoreWriter) Abort() error {
	if !w.markDone() {
		return nil
	}
	defer w.adapter.lockManager.Unlock(w.key)

	closeErr := w.pw.CloseWithError(context.Canceled)
	w.wg.Wait()

	dataKey := w.adapter.toDataPath(w.key)
	metaKey := w.adapter.toMetaPath(w.key)
	deleteErr := errors.Join(
		ignoreNotFound(w.adapter.bucket.Delete(context.Background(), dataKey), w.adapter.bucket),
		ignoreNotFound(w.adapter.bucket.Delete(context.Background(), metaKey), w.adapter.bucket),
	)
	return errors.Join(closeErr, deleteErr)
}

func (w *streamingObjstoreWriter) markDone() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return false
	}
	w.done = true
	return true
}

func (w *streamingObjstoreWriter) isDone() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

type keyLockManager struct {
	locks []sync.Mutex
	slots uint64
}

func newKeyLockManager(slots int) *keyLockManager {
	if slots <= 0 {
		slots = 2048
	}
	return &keyLockManager{
		locks: make([]sync.Mutex, slots),
		slots: uint64(slots),
	}
}

func (m *keyLockManager) Lock(key string) {
	m.locks[xxh3.HashString(key)%m.slots].Lock()
}

func (m *keyLockManager) Unlock(key string) {
	m.locks[xxh3.HashString(key)%m.slots].Unlock()
}

func ignoreNotFound(err error, bucket objstore.Bucket) error {
	if err == nil || bucket.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}
