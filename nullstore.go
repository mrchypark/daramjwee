// package daramjwee provides a simple, "Good Enough" hybrid caching middleware.
package daramjwee

import (
	"context"
	"io"
)

// nullStore는 아무 작업도 수행하지 않는 Store 인터페이스의 Null Object 구현입니다.
// ColdStore가 설정되지 않았을 때 nil 대신 사용되어 nil 체크를 없애줍니다.
type nullStore struct{}

// newNullStore는 nullStore의 새 인스턴스를 생성합니다.
func newNullStore() Store {
	return &nullStore{}
}

// 컴파일 타임에 인터페이스 만족 확인
var _ Store = (*nullStore)(nil)

// GetStream은 항상 ErrNotFound를 반환합니다.
func (ns *nullStore) GetStream(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return nil, "", ErrNotFound
}

// SetWithWriter는 모든 데이터를 버리는 io.WriteCloser를 반환합니다.
func (ns *nullStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	return &nullWriteCloser{}, nil
}

// Delete는 아무것도 하지 않고 성공을 반환합니다.
func (ns *nullStore) Delete(ctx context.Context, key string) error {
	return nil
}

// Stat은 항상 ErrNotFound를 반환합니다.
func (ns *nullStore) Stat(ctx context.Context, key string) (string, error) {
	return "", ErrNotFound
}

// nullWriteCloser는 io.WriteCloser 인터페이스를 만족하며 모든 쓰기 작업을 무시합니다.
type nullWriteCloser struct{}

func (nwc *nullWriteCloser) Write(p []byte) (n int, err error) {
	return len(p), nil // 받은 데이터를 성공적으로 "썼다"고 보고하지만 실제로는 버립니다.
}

func (nwc *nullWriteCloser) Close() error {
	return nil
}
