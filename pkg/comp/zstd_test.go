package comp

import (
	"fmt"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewZstdCompressor(t *testing.T) {
	// 유효한 압축 레벨로 생성
	compressor, err := NewZstdCompressor(3)
	require.NoError(t, err)
	assert.NotNil(t, compressor)
	assert.Equal(t, 3, compressor.Level())

	// 유효하지 않은 압축 레벨로 생성 (너무 낮음)
	_, err = NewZstdCompressor(0)
	assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)

	// 유효하지 않은 압축 레벨로 생성 (너무 높음)
	_, err = NewZstdCompressor(23)
	assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)
}

func TestNewDefaultZstdCompressor(t *testing.T) {
	compressor := NewDefaultZstdCompressor()
	assert.NotNil(t, compressor)
	assert.Equal(t, 3, compressor.Level())
	assert.Equal(t, "zstd", compressor.Algorithm())
}

func TestZstdCompressor_NotImplemented(t *testing.T) {
	compressor := NewDefaultZstdCompressor()

	// 압축 시도 - 아직 구현되지 않음
	_, err := compressor.Compress(nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)

	// 압축 해제 시도 - 아직 구현되지 않음
	_, err = compressor.Decompress(nil, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)
}

func TestZstdCompressor_Interface(t *testing.T) {
	// Compressor 인터페이스를 구현하는지 확인
	var _ daramjwee.Compressor = (*ZstdCompressor)(nil)
	var _ daramjwee.Compressor = NewDefaultZstdCompressor()
}

func TestZstdCompressor_ValidLevels(t *testing.T) {
	validLevels := []int{1, 3, 6, 9, 15, 19, 22}

	for _, level := range validLevels {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			compressor, err := NewZstdCompressor(level)
			require.NoError(t, err)
			assert.Equal(t, level, compressor.Level())
			assert.Equal(t, "zstd", compressor.Algorithm())
		})
	}
}
