package distributed

import (
	"context"
	"io"

	"github.com/go-kit/log"

	"github.com/mrchypark/daramjwee" // daramjwee의 인터페이스를 가져옵니다.
	"github.com/mrchypark/daramjwee/pkg/transport"
)

type Info struct {
	Address string
	IsSelf  bool
}

type Picker interface {
	GetPeer(key string) (Info, error)
}

type DistributedStore struct {
	localStore daramjwee.Store // 로컬 저장소 (e.g., FileStore) - "Hot Tier의 Shard"
	picker     Picker
	request    transport.Request
	client     transport.Client

	logger log.Logger
}

// 생성자 함수
func New(
	localStore daramjwee.Store,
	picker Picker,
	request transport.Request,
	client transport.Client,
	logger log.Logger,
) (*DistributedStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &DistributedStore{
		localStore: localStore,
		logger:     logger,
	}, nil
}

// 컴파일 타임에 daramjwee.Store 인터페이스 만족 확인
var _ daramjwee.Store = (*DistributedStore)(nil)

func (d *DistributedStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, nil
}

func (d *DistributedStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	return nil, nil
}

func (d *DistributedStore) Delete(ctx context.Context, key string) error {
	return nil
}
func (d *DistributedStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, nil
}
