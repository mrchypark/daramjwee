package transport

import (
	"context"
	"io"

	"github.com/mrchypark/daramjwee"
)

type Client interface {
	Do(ctx context.Context, key string) (Response, error)
}

// Response는 원격 피어로부터 받은 응답의 추상화입니다.
// Get 요청에 대한 응답에 필요한 정보를 포함합니다.
type Response interface {
	// Body는 응답 본문 데이터를 위한 io.ReadCloser를 반환합니다.
	Body() io.ReadCloser
	// Metadata는 응답에 포함된 메타데이터를 반환합니다.
	Metadata() *daramjwee.Metadata
	// Err는 요청 처리 중 발생한 오류를 반환합니다.
	Err() error
	// IsNotFound는 Err()가 daramjwee.ErrNotFound인지 확인하는 헬퍼 메서드입니다.
	IsNotFound() bool
}
