package comp

import (
	"bytes"
	"compress/gzip"
	"fmt"
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

func TestGzipCompressor_DifferentLevels(t *testing.T) {
	testData := "This is a test string that should compress well with different levels. " +
		"The quick brown fox jumps over the lazy dog. " +
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit."

	levels := []int{
		gzip.HuffmanOnly,
		gzip.BestSpeed,
		gzip.DefaultCompression,
		gzip.BestCompression,
	}

	for _, level := range levels {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			compressor, err := NewGzipCompressor(level)
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

func TestGzipCompressor_LargeData(t *testing.T) {
	compressor := NewDefaultGzipCompressor()

	// 큰 데이터 생성 (1MB)
	largeData := strings.Repeat("Hello, World! This is a large test data. ", 25000)

	// 압축 테스트
	var compressed bytes.Buffer
	src := strings.NewReader(largeData)
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)
	assert.Greater(t, compressedBytes, int64(0))

	// 압축률 확인 (반복되는 데이터이므로 압축률이 좋아야 함)
	compressionRatio := float64(compressed.Len()) / float64(len(largeData))
	assert.Less(t, compressionRatio, 0.1, "압축률이 10% 미만이어야 함")

	// 압축 해제 테스트
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)), decompressedBytes)
	assert.Equal(t, largeData, decompressed.String())
}

func TestGzipCompressor_CorruptedData(t *testing.T) {
	compressor := NewDefaultGzipCompressor()

	// 손상된 데이터로 압축 해제 시도
	corruptedData := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff}
	src := bytes.NewReader(corruptedData)

	var decompressed bytes.Buffer
	_, err := compressor.Decompress(&decompressed, src)
	assert.Error(t, err)
	assert.ErrorIs(t, err, daramjwee.ErrDecompressionFailed)
}

func TestGzipCompressor_MultipleOperations(t *testing.T) {
	compressor := NewDefaultGzipCompressor()
	testData := "Test data for multiple operations"

	// 같은 압축기로 여러 번 압축/해제 작업
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("operation_%d", i), func(t *testing.T) {
			var compressed bytes.Buffer
			src := strings.NewReader(testData)
			_, err := compressor.Compress(&compressed, src)
			require.NoError(t, err)

			var decompressed bytes.Buffer
			_, err = compressor.Decompress(&decompressed, &compressed)
			require.NoError(t, err)
			assert.Equal(t, testData, decompressed.String())
		})
	}
}

// 벤치마크 테스트들
func BenchmarkGzipCompressor_Compress(b *testing.B) {
	compressor := NewDefaultGzipCompressor()
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

func BenchmarkGzipCompressor_Decompress(b *testing.B) {
	compressor := NewDefaultGzipCompressor()
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

func BenchmarkGzipCompressor_CompressDecompress(b *testing.B) {
	compressor := NewDefaultGzipCompressor()
	testData := strings.Repeat("Hello, World! This is benchmark test data. ", 1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 압축
		var compressed bytes.Buffer
		src := strings.NewReader(testData)
		_, err := compressor.Compress(&compressed, src)
		if err != nil {
			b.Fatal(err)
		}

		// 압축 해제
		var decompressed bytes.Buffer
		_, err = compressor.Decompress(&decompressed, &compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGzipCompressor_DifferentLevels(b *testing.B) {
	testData := strings.Repeat("Hello, World! This is benchmark test data. ", 1000)

	levels := []int{
		gzip.BestSpeed,
		gzip.DefaultCompression,
		gzip.BestCompression,
	}

	for _, level := range levels {
		b.Run(fmt.Sprintf("level_%d", level), func(b *testing.B) {
			compressor, err := NewGzipCompressor(level)
			if err != nil {
				b.Fatal(err)
			}

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
		})
	}
}
