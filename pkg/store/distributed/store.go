package distributed

import (
	"github.com/go-kit/log"

	"github.com/mrchypark/daramjwee" // daramjwee의 인터페이스를 가져옵니다.
)

type Info struct {
	Address string
	IsSelf  bool
}

type KeyRouter interface {
	GetPeer(key string) (Info, error)
}

type DistributedStore struct {
	localStore daramjwee.Store // 로컬 저장소 (e.g., FileStore) - "Hot Tier의 Shard"
	router     KeyRouter       // ★★★ 'hashring' 대신 'KeyRouter' 인터페이스에 의존
	logger     log.Logger
}

// 생성자 함수
func New(
	localStore daramjwee.Store,
	router KeyRouter,
	logger log.Logger,
) (*DistributedStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &DistributedStore{
		localStore: localStore,
		router:     router,
		logger:     logger,
	}, nil
}

// 컴파일 타임에 daramjwee.Store 인터페이스 만족 확인
var _ daramjwee.Store = (*DistributedStore)(nil)
