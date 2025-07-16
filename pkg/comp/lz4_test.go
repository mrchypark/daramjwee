package comp

import (
	"fmt"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLZ4Compressor(t *testing.T) {
	// 유효한 압축 레벨로 생성
	compressor, err := NewLZ4Compressor(1)
	require.NoError(t, err)
	assert.NotNil(t, compressor)
	assert.Equal(t, 1, compressor.Level())

	// 유효하지 않은 압축 레벨로 생성 (너무 낮음)
	_, err = NewLZ4Compressor(0)
	assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)

	// 유효하지 않은 압축 레벨로 생성 (너무 높음)
	_, err = NewLZ4Compressor(13)
	assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)
}

func TestNewDefaultLZ4Compressor(t *testing.T) {
	compressor := NewDefaultLZ4Compressor()
	assert.NotNil(t, compressor)
	assert.Equal(t, 1, compressor.Level())
	assert.Equal(t, "lz4", compressor.Algorithm())
}

func TestLZ4Compressor_NotImplemented(t *testing.T) {
	compressor := NewDefaultLZ4Compressor()

	// 압축 시도 - 아직 구현되지 않음
	_, err := compressor.Compress(nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)

	// 압축 해제 시도 - 아직 구현되지 않음
	_, err = compressor.Decompress(nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)
}

func TestLZ4Compressor_Interface(t *testing.T) {
	// Compressor 인터페이스를 구현하는지 확인
	var _ daramjwee.Compressor = (*LZ4Compressor)(nil)
	var _ daramjwee.Compressor = NewDefaultLZ4Compressor()
}

func TestLZ4Compressor_ValidLevels(t *testing.T) {
	validLevels := []int{1, 3, 6, 9, 12}

	for _, level := range validLevels {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			compressor, err := NewLZ4Compressor(level)
			require.NoError(t, err)
			assert.Equal(t, level, compressor.Level())
			assert.Equal(t, "lz4", compressor.Algorithm())
		})
	}
}
