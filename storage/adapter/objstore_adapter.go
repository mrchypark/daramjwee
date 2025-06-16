// Package adapter provides adapters to make external libraries compatible with daramjwee's interfaces.
package adapter

import (
	"context"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/thanos-io/objstore"
)

// objstoreAdapter wraps an objstore.Bucket to implement the daramjwee.Store interface.
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

func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	exists, err := a.bucket.Exists(ctx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to check existence in objstore for key '%s': %w", key, err)
	}
	if !exists {
		return nil, nil, daramjwee.ErrNotFound
	}

	r, err := a.bucket.Get(ctx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get object for key '%s' after existence check: %w", err)
	}

	// objstore.Bucket은 Get에서 ETag를 직접 반환하지 않으므로 빈 메타데이터를 반환합니다.
	return r, &daramjwee.Metadata{ETag: ""}, nil
}

func (a *objstoreAdapter) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		err := a.bucket.Upload(ctx, key, pr)
		if err != nil {
			level.Error(a.logger).Log("msg", "failed to upload to objstore bucket", "key", key, "err", err)
			pr.CloseWithError(err)
			pw.CloseWithError(err)
		}
	}()

	return pw, nil
}

func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	return a.bucket.Delete(ctx, key)
}

func (a *objstoreAdapter) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	exists, err := a.bucket.Exists(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, daramjwee.ErrNotFound
	}
	// Exists만으로는 상세 메타데이터를 알 수 없으므로, 빈 메타데이터를 반환합니다.
	return &daramjwee.Metadata{ETag: ""}, nil
}
