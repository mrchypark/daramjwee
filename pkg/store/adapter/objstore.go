// Package adapter provides adapters to make external libraries compatible with daramjwee's interfaces.
package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/goccy/go-json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

// objstoreAdapter wraps an objstore.Bucket to implement the daramjwee.Store interface.
// This implementation manages metadata (like ETags) by storing a separate '.meta.json'
// object for each data object. It supports true streaming for uploads.
type objstoreAdapter struct {
	bucket objstore.Bucket
	logger log.Logger
}

// NewObjstoreAdapter creates a new adapter.
func NewObjstoreAdapter(bucket objstore.Bucket, logger log.Logger) daramjwee.Store {
	return &objstoreAdapter{
		bucket: bucket,
		logger: logger,
	}
}

// 컴파일 타임에 인터페이스 만족 확인
var _ daramjwee.Store = (*objstoreAdapter)(nil)

// metaFilePayload defines the structure for storing metadata in a .meta.json file.
type metaFilePayload struct {
	ETag string `json:"etag"`
}

func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	// First, get the metadata. This also serves as an existence check.
	meta, err := a.Stat(ctx, key)
	if err != nil {
		return nil, nil, err // daramjwee.ErrNotFound or other errors
	}

	// Now, get the actual object data stream.
	dataPath := a.toDataPath(key)
	r, err := a.bucket.Get(ctx, dataPath)
	if err != nil {
		// "Not Found"를 확인하는 대신, Stat이 통과했으므로 다른 종류의 에러로 간주합니다.
		// 만약 메타는 있는데 데이터가 없는 불일치 상황이라면, Get에서 오류가 발생할 것입니다.
		level.Warn(a.logger).Log("msg", "failed to get data object even though metadata exists", "key", key, "err", err)
		return nil, nil, fmt.Errorf("failed to get object for key '%s' after meta check: %w", key, err)
	}

	return r, meta, nil
}

// SetWithWriter returns a WriteCloser that enables true streaming uploads.
// It uses an io.Pipe, allowing the caller to write data chunk by chunk,
// which is concurrently uploaded to the object store in a separate goroutine.
// This is highly memory-efficient as the entire object does not need to be
// buffered in memory before the upload begins.
func (a *objstoreAdapter) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	writer := &streamingObjstoreWriter{
		ctx:     ctx,
		adapter: a,
		key:     key,
		etag:    etag,
		pw:      pw,
		wg:      &sync.WaitGroup{},
	}

	writer.wg.Add(1)
	go func() {
		defer writer.wg.Done()
		dataPath := a.toDataPath(key)
		// This call blocks until the pipe writer is closed.
		writer.uploadErr = a.bucket.Upload(ctx, dataPath, pr)
	}()

	return writer, nil
}

func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	g, gCtx := errgroup.WithContext(ctx)

	dataPath := a.toDataPath(key)
	metaPath := a.toMetaPath(key)

	// Delete data object
	g.Go(func() error {
		// 오류 타입을 확인하는 대신, Delete를 직접 호출합니다.
		// 대부분의 객체 저장소에서 Delete는 멱등성을 가집니다.
		if err := a.bucket.Delete(gCtx, dataPath); err != nil {
			level.Error(a.logger).Log("msg", "failed to delete data object", "key", dataPath, "err", err)
			return err
		}
		return nil
	})

	// Delete meta object
	g.Go(func() error {
		if err := a.bucket.Delete(gCtx, metaPath); err != nil {
			level.Error(a.logger).Log("msg", "failed to delete meta object", "key", metaPath, "err", err)
			return err
		}
		return nil
	})

	// Wait()는 nil이 아닌 첫 번째 오류를 반환합니다.
	return g.Wait()
}

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

	var payload metaFilePayload
	if err := json.Unmarshal(metaBytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata for key '%s': %w", key, err)
	}

	return &daramjwee.Metadata{ETag: payload.ETag}, nil
}

// --- Helper methods and types ---

func (a *objstoreAdapter) toDataPath(key string) string {
	return key
}

func (a *objstoreAdapter) toMetaPath(key string) string {
	return key + ".meta.json"
}

// streamingObjstoreWriter handles the true streaming upload process.
type streamingObjstoreWriter struct {
	ctx       context.Context
	adapter   *objstoreAdapter
	key       string
	etag      string
	pw        *io.PipeWriter
	wg        *sync.WaitGroup
	uploadErr error
}

// Write writes data to the pipe, which is immediately streamed to the object store.
func (w *streamingObjstoreWriter) Write(p []byte) (n int, err error) {
	return w.pw.Write(p)
}

// Close finalizes the write operation. It closes the pipe, waits for the data
// upload to finish, and then uploads the metadata object.
func (w *streamingObjstoreWriter) Close() error {
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
	payload := metaFilePayload{ETag: w.etag}
	metaBytes, err := json.Marshal(payload)
	if err != nil {
		level.Error(w.adapter.logger).Log("msg", "failed to marshal metadata", "key", metaPath, "err", err)
		return fmt.Errorf("failed to marshal metadata for key '%s': %w", w.key, err)
	}

	err = w.adapter.bucket.Upload(w.ctx, metaPath, bytes.NewReader(metaBytes))
	if err != nil {
		level.Error(w.adapter.logger).Log("msg", "failed to upload metadata object", "key", metaPath, "err", err)
		return fmt.Errorf("failed to upload metadata for key '%s': %w", w.key, err)
	}

	level.Debug(w.adapter.logger).Log("msg", "successfully uploaded data and metadata", "key", w.key)
	return nil
}
