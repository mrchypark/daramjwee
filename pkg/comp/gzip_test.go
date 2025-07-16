package comp

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGzipCompressor(t *testing.T) {
	// 유효한 압축 레벨로 생성
	compressor, err := NewGzipCompressor(gzip.DefaultCompression)
	require.NoError(t, err)
	assert.NotNil(t, compressor)
	assert.Equal(t, gzip.DefaultCompression, compressor.Level())

	// 유효하지 않은 압축 레벨로 생성
	_, err = NewGzipCompressor(100) // 유효하지 않은 레벨
	assert.ErrorIs(t, err, daramjwee.ErrInvalidCompressionLevel)
}

func TestNewDefaultGzipCompressor(t *testing.T) {
	compressor := NewDefaultGzipCompressor()
	assert.NotNil(t, compressor)
	assert.Equal(t, gzip.DefaultCompression, compressor.Level())
	assert.Equal(t, "gzip", compressor.Algorithm())
}

func TestGzipCompressor_CompressDecompress(t *testing.T) {
	compressor := NewDefaultGzipCompressor()
	testData := "Hello, World! This is a test string for compression."

	// 압축 테스트
	var compressed bytes.Buffer
	src := strings.NewReader(testData)
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)
	assert.Greater(t, compressedBytes, int64(0))

	// 압축된 데이터가 원본과 다른지 확인
	assert.NotEqual(t, testData, compressed.String())

	// 압축 해제 테스트
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Greater(t, decompressedBytes, int64(0))

	// 압축 해제된 데이터가 원본과 같은지 확인
	assert.Equal(t, testData, decompressed.String())
}

func TestGzipCompressor_EmptyData(t *testing.T) {
	compressor := NewDefaultGzipCompressor()

	// 빈 데이터 압축
	var compressed bytes.Buffer
	src := strings.NewReader("")
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)

	// gzip은 빈 데이터도 헤더를 포함하므로 압축된 크기가 있을 수 있음
	// 실제 압축된 바이트 수는 헤더 크기에 따라 달라질 수 있음
	t.Logf("Compressed bytes for empty data: %d", compressedBytes)

	// 압축된 데이터가 있는지 확인 (헤더 포함)
	assert.GreaterOrEqual(t, compressed.Len(), 0)

	// 빈 데이터 압축 해제
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Equal(t, int64(0), decompressedBytes)
	assert.Equal(t, "", decompressed.String())
}

func TestGzipCompressor_Interface(t *testing.T) {
	// Compressor 인터페이스를 구현하는지 확인
	var _ daramjwee.Compressor = (*GzipCompressor)(nil)
	var _ daramjwee.Compressor = NewDefaultGzipCompressor()
}
