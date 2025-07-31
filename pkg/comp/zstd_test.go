package comp

import (
	"bytes"
	"fmt"
	"strings"
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

func TestZstdCompressor_CompressDecompress(t *testing.T) {
	compressor := NewDefaultZstdCompressor()
	testData := "Hello, World! This is a test string for Zstd compression."

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
func TestZstdCompressor_EmptyData(t *testing.T) {
	compressor := NewDefaultZstdCompressor()

	// 빈 데이터 압축
	var compressed bytes.Buffer
	src := strings.NewReader("")
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)

	// Zstd는 빈 데이터도 헤더를 포함하므로 압축된 크기가 있을 수 있음
	t.Logf("Compressed bytes for empty data: %d", compressedBytes)
	assert.GreaterOrEqual(t, compressed.Len(), 0)

	// 빈 데이터 압축 해제
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Equal(t, int64(0), decompressedBytes)
	assert.Equal(t, "", decompressed.String())
}

func TestZstdCompressor_LargeData(t *testing.T) {
	compressor := NewDefaultZstdCompressor()

	// 큰 데이터 생성 (1MB)
	largeData := strings.Repeat("Hello, World! This is a large test data for Zstd. ", 20000)

	// 압축 테스트
	var compressed bytes.Buffer
	src := strings.NewReader(largeData)
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)
	assert.Greater(t, compressedBytes, int64(0))

	// 압축률 확인 (반복되는 데이터이므로 압축률이 매우 좋아야 함)
	compressionRatio := float64(compressed.Len()) / float64(len(largeData))
	assert.Less(t, compressionRatio, 0.1, "압축률이 10% 미만이어야 함")

	// 압축 해제 테스트
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)), decompressedBytes)
	assert.Equal(t, largeData, decompressed.String())
}

func TestZstdCompressor_DifferentLevels(t *testing.T) {
	testData := "This is a test string that should compress well with different levels. " +
		"The quick brown fox jumps over the lazy dog. " +
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit."

	levels := []int{1, 3, 6, 9, 15, 19, 22}

	for _, level := range levels {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			compressor, err := NewZstdCompressor(level)
			require.NoError(t, err)
			assert.Equal(t, level, compressor.Level())

			// 압축 테스트
			var compressed bytes.Buffer
			src := strings.NewReader(testData)
			compressedBytes, err := compressor.Compress(&compressed, src)
			require.NoError(t, err)
			assert.Greater(t, compressedBytes, int64(0))

			// 압축 해제 테스트
			var decompressed bytes.Buffer
			_, err = compressor.Decompress(&decompressed, &compressed)
			require.NoError(t, err)
			assert.Equal(t, testData, decompressed.String())
		})
	}
}

// 벤치마크 테스트들
func BenchmarkZstdCompressor_Compress(b *testing.B) {
	compressor := NewDefaultZstdCompressor()
	testData := strings.Repeat("Hello, World! This is benchmark test data. ", 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var compressed bytes.Buffer
		src := strings.NewReader(testData)
		_, err := compressor.Compress(&compressed, src)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZstdCompressor_Decompress(b *testing.B) {
	compressor := NewDefaultZstdCompressor()
	testData := strings.Repeat("Hello, World! This is benchmark test data. ", 1000)

	// 미리 압축된 데이터 준비
	var compressed bytes.Buffer
	src := strings.NewReader(testData)
	_, err := compressor.Compress(&compressed, src)
	if err != nil {
		b.Fatal(err)
	}
	compressedData := compressed.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var decompressed bytes.Buffer
		src := bytes.NewReader(compressedData)
		_, err := compressor.Decompress(&decompressed, src)
		if err != nil {
			b.Fatal(err)
		}
	}
}
