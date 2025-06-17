package router

import (
	"fmt"
	"hash/fnv"

	"github.com/edwingeng/doublejump/v2"
	"github.com/mrchypark/daramjwee/pkg/store/distributed"
)

// DynamicKeyRouter는 KeyRouter 인터페이스의 동적 구현체입니다.
type StaticKeyRouter struct {
	self   string
	hasher *doublejump.Hash[string]
}

var _ distributed.KeyRouter = (*StaticKeyRouter)(nil)

func NewStaticKeyRouter(self string, peers []string) (*StaticKeyRouter, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("peer list cannot be empty for StaticKeyRouter")
	}

	h := doublejump.NewHash[string]()
	for _, peer := range peers {
		h.Add(peer) // Use the 'peer' variable from the loop
	}
	r := &StaticKeyRouter{
		self:   self,
		hasher: h,
	}
	return r, nil
}

// KeyRouter 인터페이스 구현
func (r *StaticKeyRouter) GetPeer(key string) (distributed.Info, error) {
	peer, ok := r.hasher.Get(stringToUint64FNV(key))
	if !ok {
		return distributed.Info{}, fmt.Errorf("no peer found for key: %s", key)
	}
	i := distributed.Info{Address: peer, IsSelf: false}
	if peer == r.self {
		i.IsSelf = true
	}

	return i, nil
}

func stringToUint64FNV(s string) uint64 {
	// 64비트 FNV-1a 해셔를 생성합니다.
	h := fnv.New64a()
	// 문자열 바이트를 해셔에 씁니다.
	h.Write([]byte(s))
	// 해시 값을 uint64로 반환합니다.
	return h.Sum64()
}
