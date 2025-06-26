// Filename: options_test.go
package daramjwee

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- 테스트를 위한 Mock Store ---
// 이 테스트 파일 내에서만 사용할 간단한 Mock Store입니다.
// cache_test.go의 mockStore를 가져오지 않고, 의존성을 최소화합니다.
type optionsTestMockStore struct{}

func (s *optionsTestMockStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}
func (s *optionsTestMockStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	return nil, nil
}
func (s *optionsTestMockStore) Delete(ctx context.Context, key string) error {
	return nil
}
func (s *optionsTestMockStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, nil
}

// --- 기본 유효성 검사 테스트 ---

func TestNew_OptionValidation(t *testing.T) {
	// 유효성 검사 테스트에 사용할 기본 HotStore
	validHotStore := &optionsTestMockStore{}

	testCases := []struct {
		name        string
		options     []Option
		expectErr   bool
		expectedMsg string // 에러 발생 시 기대하는 메시지 일부
	}{
		// --- 해피 패스 (성공 케이스) ---
		{
			name:      "Success with only mandatory hot store",
			options:   []Option{WithHotStore(validHotStore)},
			expectErr: false,
		},
		{
			name: "Success with all options valid",
			options: []Option{
				WithHotStore(validHotStore),
				WithColdStore(&optionsTestMockStore{}),
				WithWorker("pool", 10, 100, 5*time.Second),
				WithDefaultTimeout(10 * time.Second),
				WithGracePeriod(1 * time.Minute),
				WithNegativeCache(5 * time.Minute),
			},
			expectErr: false,
		},
		{
			name: "Success with negative cache TTL of zero",
			options: []Option{
				WithHotStore(validHotStore),
				WithNegativeCache(0),
			},
			expectErr: false,
		},

		// --- 실패 케이스 ---
		{
			name:        "Failure without any options",
			options:     []Option{},
			expectErr:   true,
			expectedMsg: "hotStore is required",
		},
		{
			name:        "Failure with nil HotStore",
			options:     []Option{WithHotStore(nil)},
			expectErr:   true,
			expectedMsg: "hot store cannot be nil",
		},
		{
			name: "Failure with empty worker strategy",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("", 10, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker strategy type cannot be empty",
		},
		{
			name: "Failure with zero worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 0, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker pool size must be positive",
		},
		{
			name: "Failure with negative worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", -5, 100, 1*time.Second),
			},
			expectErr:   true,
			expectedMsg: "worker pool size must be positive",
		},
		{
			name: "Failure with zero worker job timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 10, 100, 0),
			},
			expectErr:   true,
			expectedMsg: "worker job timeout must be positive",
		},
		{
			name: "Failure with zero default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(0),
			},
			expectErr:   true,
			expectedMsg: "default timeout must be positive",
		},
		{
			name: "Failure with negative default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(-5 * time.Second),
			},
			expectErr:   true,
			expectedMsg: "default timeout must be positive",
		},
		{
			name: "Failure with zero grace period",
			options: []Option{
				WithHotStore(validHotStore),
				WithGracePeriod(0),
			},
			expectErr:   true,
			expectedMsg: "positive cache TTL cannot be a negative value and zero",
		},
		{
			name: "Failure with negative grace period",
			options: []Option{
				WithHotStore(validHotStore),
				WithGracePeriod(-1 * time.Minute),
			},
			expectErr:   true,
			expectedMsg: "positive cache TTL cannot be a negative value and zero",
		},
		{
			name: "Failure with negative value for negative cache",
			options: []Option{
				WithHotStore(validHotStore),
				WithNegativeCache(-1 * time.Second),
			},
			expectErr:   true,
			expectedMsg: "negative cache TTL cannot be a negative value",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := New(nil, tc.options...)

			if tc.expectErr {
				require.Error(t, err, "Expected an error for invalid option")
				assert.Contains(t, err.Error(), tc.expectedMsg, "Error message mismatch")
				assert.Nil(t, cache, "Cache should be nil on creation failure")
			} else {
				require.NoError(t, err, "Expected no error for valid options")
				assert.NotNil(t, cache, "Cache should not be nil on successful creation")
				// 성공 시에는 리소스를 정리해야 합니다.
				cache.Close()
			}
		})
	}
}

// --- 엣지 케이스 테스트 ---

// TestNew_OptionOverrides는 동일한 옵션이 여러 번 제공되었을 때
// 마지막에 제공된 옵션이 적용되는지 검증합니다.
func TestNew_OptionOverrides(t *testing.T) {
	validHotStore := &optionsTestMockStore{}
	finalTimeout := 15 * time.Second
	finalGracePeriod := 10 * time.Minute

	options := []Option{
		WithHotStore(validHotStore),
		WithDefaultTimeout(5 * time.Second), // 초기값
		WithGracePeriod(1 * time.Minute),    // 초기값
		WithDefaultTimeout(finalTimeout),    // 최종값
		WithGracePeriod(finalGracePeriod),   // 최종값
	}

	cache, err := New(nil, options...)
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.Close()

	// Cache 인터페이스를 실제 구현체인 DaramjweeCache로 타입 단언(type assertion)하여
	// 내부 설정값을 확인합니다.
	dCache, ok := cache.(*DaramjweeCache)
	require.True(t, ok, "Failed to assert cache to *DaramjweeCache")

	assert.Equal(t, finalTimeout, dCache.DefaultTimeout, "The last DefaultTimeout option should be applied")
	assert.Equal(t, finalGracePeriod, dCache.PositiveGracePeriod, "The last WithGracePeriod option should be applied")
}

// TestNew_NilColdStoreIsValid는 ColdStore로 nil을 전달하는 것이 유효하며,
// 이 경우 내부적으로 nullStore가 사용되는 것을 (간접적으로) 검증합니다.
// New 함수는 에러 없이 성공적으로 Cache 객체를 반환해야 합니다.
func TestNew_NilColdStoreIsValid(t *testing.T) {
	validHotStore := &optionsTestMockStore{}

	options := []Option{
		WithHotStore(validHotStore),
		WithColdStore(nil), // 명시적으로 nil ColdStore 설정
	}

	cache, err := New(nil, options...)
	require.NoError(t, err, "Providing a nil ColdStore should be valid")
	require.NotNil(t, cache)
	defer cache.Close()

	// 내부적으로 nullStore가 사용되었는지 확인하려면, 실제 ColdStore에 접근해야 합니다.
	dCache, ok := cache.(*DaramjweeCache)
	require.True(t, ok)

	// nullStore 타입인지 확인합니다.
	_, ok = dCache.ColdStore.(*nullStore)
	assert.True(t, ok, "ColdStore should be an instance of nullStore when configured with nil")
}
