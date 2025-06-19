//go:build stress

// Filename: stress_test.go
package daramjwee

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowMockStore는 의도적인 지연을 추가하여 느린 스토리지를 시뮬레이션합니다.
type slowMockStore struct {
	mockStore
	delay time.Duration
}

func newSlowMockStore(delay time.Duration) *slowMockStore {
	return &slowMockStore{
		mockStore: *newMockStore(),
		delay:     delay,
	}
}

// GetStream은 데이터를 반환하기 전에 설정된 시간만큼 지연됩니다.
func (s *slowMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	time.Sleep(s.delay)
	return s.mockStore.GetStream(ctx, key)
}

// TestCache_WithSlowColdStore는 Cold Store가 느릴 때 Cold Hit 시나리오가
// 타임아웃 내에 정상적으로 완료되는지 검증합니다.
func TestCache_WithSlowColdStore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	hot := newMockStore()
	// Cold Store의 응답이 100ms 지연되도록 설정
	slowCold := newSlowMockStore(100 * time.Millisecond)

	// 캐시의 기본 타임아웃은 지연 시간보다 길게 설정 (예: 1초)
	cache, err := New(nil, WithHotStore(hot), WithColdStore(slowCold), WithDefaultTimeout(1*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	key := "slow-item"
	content := "content from slow store"
	slowCold.setData(key, content, &Metadata{ETag: "v-slow"})

	// Get 호출 (내부적으로 Cold Hit -> Promotion 발생)
	stream, err := cache.Get(context.Background(), key, &mockFetcher{})
	require.NoError(t, err)

	readBytes, err := io.ReadAll(stream)
	require.NoError(t, err)
	err = stream.Close()
	require.NoError(t, err)

	// 1. 반환된 데이터가 올바른지 확인
	assert.Equal(t, content, string(readBytes))

	// 2. 데이터가 Hot 캐시로 정상적으로 승격되었는지 확인
	hot.mu.RLock()
	promotedData, ok := hot.data[key]
	hot.mu.RUnlock()
	assert.True(t, ok, "Data should be promoted to hot cache")
	assert.Equal(t, content, string(promotedData))
}
