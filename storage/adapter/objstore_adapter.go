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

// --- Context-Aware Methods ---

func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, string, error) {
	// 1. 먼저 Exists를 호출하여 객체 존재 여부만 확인합니다.
	// 이 방식은 특정 에러 타입이나 헬퍼 함수에 의존하지 않아 매우 안정적입니다.
	exists, err := a.bucket.Exists(ctx, key)
	if err != nil {
		// Exists 호출 자체에서 에러가 발생한 경우 (네트워크 문제, 권한 문제 등)
		return nil, "", fmt.Errorf("failed to check existence in objstore for key '%s': %w", key, err)
	}
	if !exists {
		// 객체가 존재하지 않는 것이 명확하게 확인되면, daramjwee의 표준 에러를 반환합니다.
		return nil, "", daramjwee.ErrNotFound
	}

	// 2. 존재가 확인된 후에만 Get을 호출합니다.
	// 이제 Get에서 발생하는 에러는 'Not Found'가 아닌 다른 문제(예: 다운로드 중 연결 끊김)일 가능성이 높습니다.
	r, err := a.bucket.Get(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get object for key '%s' after existence check: %w", err)
	}

	return r, "", nil
}

func (a *objstoreAdapter) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	// io.Pipe를 사용하여 Writer와 Reader를 연결하는 파이프를 생성합니다.
	// WriteCloser(pw)에 쓴 내용이 Reader(pr)로 전달됩니다.
	pr, pw := io.Pipe()

	// 별도의 고루틴에서, 파이프의 Reader(pr)로부터 데이터를 읽어 Thanos Bucket에 업로드합니다.
	// 이 Upload 메서드는 데이터 읽기가 완료될 때까지 블로킹됩니다.
	go func() {
		err := a.bucket.Upload(ctx, key, pr)
		if err != nil {
			level.Error(a.logger).Log("msg", "failed to upload to objstore bucket", "key", key, "err", err)
			// 업로드 중 에러가 발생하면, 파이프의 읽기/쓰기 쪽 모두에 에러를 전파합니다.
			pr.CloseWithError(err)
			pw.CloseWithError(err)
		}
	}()

	// 파이프의 Writer(pw)를 즉시 반환합니다.
	// 호출자는 여기에 데이터를 쓰면, 위 고루틴의 Upload 메서드가 데이터를 읽어갑니다.
	return pw, nil
}

func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	return a.bucket.Delete(ctx, key)
}

func (a *objstoreAdapter) Stat(ctx context.Context, key string) (string, error) {
	exists, err := a.bucket.Exists(ctx, key)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", daramjwee.ErrNotFound
	}
	// Exists만으로는 상세 메타데이터를 알 수 없으므로, 키 정보만 반환합니다.
	return "", nil
}
