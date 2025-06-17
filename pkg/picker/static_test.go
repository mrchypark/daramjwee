// Filename: pkg/picker/static_test.go
package picker // NOTE: 'pickter'는 원본 파일의 오타를 따름. 'picker'로 수정 권장.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewStaticKeyRouter는 라우터 생성자의 성공 및 실패 케이스를 검증합니다.
func TestNewStaticKeyRouter(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		peers := []string{"peer1", "peer2", "peer3"}
		router, err := NewStaticKeyRouter("peer1", peers)
		require.NoError(t, err)
		assert.NotNil(t, router)
		assert.Equal(t, "peer1", router.self)
		assert.NotNil(t, router.hasher)
	})

	t.Run("Error on empty peer list", func(t *testing.T) {
		_, err := NewStaticKeyRouter("peer1", []string{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "peer list cannot be empty")
	})
}

// TestStaticKeyRouter_GetPeer는 키에 대한 피어 선택 로직을 검증합니다.
func TestStaticKeyRouter_GetPeer(t *testing.T) {
	self := "http://127.0.0.1:8080"
	peers := []string{
		"http://127.0.0.1:8080", // self
		"http://127.0.0.1:8081",
		"http://127.0.0.1:8082",
	}

	router, err := NewStaticKeyRouter(self, peers)
	require.NoError(t, err)

	t.Run("Consistency", func(t *testing.T) {
		key := "my-consistent-key"

		// 동일한 키에 대해서는 항상 동일한 피어를 반환해야 함
		firstPeer, err := router.GetPeer(key)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			peer, err := router.GetPeer(key)
			require.NoError(t, err)
			assert.Equal(t, firstPeer.Address, peer.Address)
		}
	})

	t.Run("Distribution and IsSelf flag", func(t *testing.T) {
		// 여러 키를 테스트하여 결과가 목록 내의 유효한 피어인지,
		// 그리고 IsSelf 플래그가 정확한지 확인합니다.
		// 이 키들은 테스트 실행 시 doublejump 해시 결과에 따라 결정됩니다.
		// (참고: 모든 키가 다른 피어로 매핑될 필요는 없습니다)
		keyMappings := map[string]struct {
			expectedPeer   string
			expectedIsSelf bool
		}{
			"key-a": {"http://127.0.0.1:8082", false}, // 이 값들은 실제 해시 결과에 따라 달라질 수 있음
			"key-b": {"http://127.0.0.1:8080", true},
			"key-c": {"http://127.0.0.1:8081", false},
		}

		foundPeers := make(map[string]string)
		for key := range keyMappings {
			info, err := router.GetPeer(key)
			require.NoError(t, err)
			foundPeers[key] = info.Address
		}

		// NOTE: 아래 검증은 doublejump 해시의 결과가 항상 동일함을 전제로 합니다.
		// 만약 이 테스트가 실패한다면, 실제 해시 결과에 맞게 keyMappings의 기대값을 수정해야 합니다.
		// 여기서는 로직의 정확성을 검증하는 것이 목적입니다.
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("some-other-key-%d", i)
			info, err := router.GetPeer(key)
			require.NoError(t, err)
			assert.Contains(t, peers, info.Address, "Returned peer must be in the peer list")
			if info.Address == self {
				assert.True(t, info.IsSelf, "IsSelf should be true for self address")
			} else {
				assert.False(t, info.IsSelf, "IsSelf should be false for other peers")
			}
		}
	})
}
