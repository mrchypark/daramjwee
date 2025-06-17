// Filename: options_test.go
package daramjwee

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOptions_Validation은 각 설정 옵션(Option)의 유효성 검사 로직이
// 올바르게 동작하는지 검증합니다.
func TestOptions_Validation(t *testing.T) {
	// WithHotStore를 제외한 다른 옵션 테스트를 위한 기본 HotStore
	validHotStore := newMockStore()

	testCases := []struct {
		name        string
		options     []Option
		expectedErr string
	}{
		{
			name:        "Success with valid options",
			options:     []Option{WithHotStore(validHotStore)},
			expectedErr: "",
		},
		{
			name:        "Error on nil HotStore",
			options:     []Option{WithHotStore(nil)},
			expectedErr: "hot store cannot be nil",
		},
		{
			name: "Error on empty worker strategy",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("", 10, 100, 1*time.Second),
			},
			expectedErr: "worker strategy type cannot be empty",
		},
		{
			name: "Error on zero worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 0, 100, 1*time.Second),
			},
			// "be be" -> "be"로 오타 수정
			expectedErr: "worker pool size must be positive",
		},
		{
			name: "Error on negative worker pool size",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", -5, 100, 1*time.Second),
			},
			// "be be" -> "be"로 오타 수정
			expectedErr: "worker pool size must be positive",
		},
		{
			name: "Error on zero worker job timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithWorker("pool", 10, 100, 0),
			},
			expectedErr: "worker job timeout must be positive",
		},
		{
			name: "Error on zero default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(0),
			},
			expectedErr: "default timeout must be positive",
		},
		{
			name: "Error on negative default timeout",
			options: []Option{
				WithHotStore(validHotStore),
				WithDefaultTimeout(-5 * time.Second),
			},
			expectedErr: "default timeout must be positive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := New(nil, tc.options...)

			if tc.expectedErr != "" {
				require.Error(t, err, "Expected an error for invalid option")
				assert.Contains(t, err.Error(), tc.expectedErr)
				assert.Nil(t, cache)
			} else {
				require.NoError(t, err, "Expected no error for valid options")
				assert.NotNil(t, cache)
				cache.Close()
			}
		})
	}
}
