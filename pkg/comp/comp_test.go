// Package comp provides compression functionality tests for the daramjwee cache system.
// It includes comprehensive tests for various compression algorithms including gzip,
// lz4, zstd, and a pass-through none compressor.
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

// TestAllCompressors는 모든 압축기가 인터페이스를 올바르게 구현하는지 테스트합니다
func TestAllCompressors(t *testing.T) {
	compressors := []struct {
		name        string
		compressor  daramjwee.Compressor
		implemented bool // 실제 압축/해제가 구현되었는지 여부
	}{
		{"gzip", NewDefaultGzipCompressor(), true},
		{"lz4", NewDefaultLZ4Compressor(), true},
		{"zstd", NewDefaultZstdCompressor(), true},
		{"none", daramjwee.NewNoneCompressor(), true},
	}

	for _, tc := range compressors {
		t.Run(tc.name, func(t *testing.T) {
			// 인터페이스 구현 확인
			assert.NotNil(t, tc.compressor)
			assert.NotEmpty(t, tc.compressor.Algorithm())
			// gzip.DefaultCompression은 -1이므로 >= -2로 체크
			assert.GreaterOrEqual(t, tc.compressor.Level(), -2)

			if tc.implemented {
				// 구현된 압축기는 실제 압축/해제 테스트
				testData := "Hello, World! Test data for compression."

				var compressed bytes.Buffer
				src := strings.NewReader(testData)
				compressedBytes, err := tc.compressor.Compress(&compressed, src)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, compressedBytes, int64(0))

				var decompressed bytes.Buffer
				decompressedBytes, err := tc.compressor.Decompress(&decompressed, &compressed)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, decompressedBytes, int64(0))
				assert.Equal(t, testData, decompressed.String())
			} else {
				// 미구현 압축기는 에러 반환 확인
				_, err := tc.compressor.Compress(nil, nil)
				assert.Error(t, err)
				assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)

				_, err = tc.compressor.Decompress(nil, nil)
				assert.Error(t, err)
				assert.ErrorIs(t, err, daramjwee.ErrUnsupportedAlgorithm)
			}
		})
	}
}

// TestNoneCompressor는 NoneCompressor의 동작을 테스트합니다
func TestNoneCompressor(t *testing.T) {
	compressor := daramjwee.NewNoneCompressor()
	testData := "Hello, World! This should pass through without compression."

	// "압축" 테스트 (실제로는 그대로 복사)
	var compressed bytes.Buffer
	src := strings.NewReader(testData)
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), compressedBytes)
	assert.Equal(t, testData, compressed.String())

	// "압축 해제" 테스트 (실제로는 그대로 복사)
	var decompressed bytes.Buffer
	decompressedBytes, err := compressor.Decompress(&decompressed, &compressed)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), decompressedBytes)
	assert.Equal(t, testData, decompressed.String())

	// 메타데이터 확인
	assert.Equal(t, "none", compressor.Algorithm())
	assert.Equal(t, 0, compressor.Level())
}

// TestCompressionMetadata는 압축 메타데이터 생성을 테스트합니다
func TestCompressionMetadata(t *testing.T) {
	testData := "Hello, World! This is test data for metadata generation."
	originalSize := int64(len(testData))

	compressor := NewDefaultGzipCompressor()

	var compressed bytes.Buffer
	src := strings.NewReader(testData)
	compressedBytes, err := compressor.Compress(&compressed, src)
	require.NoError(t, err)

	// CompressionMetadata 생성
	metadata := &daramjwee.CompressionMetadata{
		Algorithm:        compressor.Algorithm(),
		Level:            compressor.Level(),
		OriginalSize:     originalSize,
		CompressedSize:   compressedBytes,
		CompressionRatio: float64(compressedBytes) / float64(originalSize),
	}

	assert.Equal(t, "gzip", metadata.Algorithm)
	assert.Equal(t, compressor.Level(), metadata.Level)
	assert.Equal(t, originalSize, metadata.OriginalSize)
	assert.Equal(t, compressedBytes, metadata.CompressedSize)
	assert.Greater(t, metadata.CompressionRatio, 0.0)
	assert.LessOrEqual(t, metadata.CompressionRatio, 1.0)
}

// BenchmarkAllCompressors는 구현된 모든 압축기의 성능을 비교합니다
func BenchmarkAllCompressors(b *testing.B) {
	testData := strings.Repeat("Hello, World! This is benchmark test data. ", 1000)

	compressors := []struct {
		name        string
		compressor  daramjwee.Compressor
		implemented bool
	}{
		{"gzip", NewDefaultGzipCompressor(), true},
		{"lz4", NewDefaultLZ4Compressor(), true},
		{"zstd", NewDefaultZstdCompressor(), true},
		{"none", daramjwee.NewNoneCompressor(), true},
	}

	for _, tc := range compressors {
		if !tc.implemented {
			continue
		}

		b.Run(fmt.Sprintf("%s_compress", tc.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var compressed bytes.Buffer
				src := strings.NewReader(testData)
				_, err := tc.compressor.Compress(&compressed, src)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
