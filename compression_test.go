package daramjwee

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressionType_Constants(t *testing.T) {
	// 압축 타입 상수들이 올바르게 정의되었는지 확인
	assert.Equal(t, CompressionType("gzip"), CompressionGzip)
	assert.Equal(t, CompressionType("zstd"), CompressionZstd)
	assert.Equal(t, CompressionType("lz4"), CompressionLZ4)
	assert.Equal(t, CompressionType("none"), CompressionNone)
}

func TestCompressionMetadata_JSONSerialization(t *testing.T) {
	// CompressionMetadata가 JSON으로 올바르게 직렬화/역직렬화되는지 확인
	original := &CompressionMetadata{
		Algorithm:        "gzip",
		Level:            6,
		OriginalSize:     1024,
		CompressedSize:   512,
		CompressionRatio: 0.5,
	}

	// JSON 직렬화
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// JSON 역직렬화
	var restored CompressionMetadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// 값 검증
	assert.Equal(t, original.Algorithm, restored.Algorithm)
	assert.Equal(t, original.Level, restored.Level)
	assert.Equal(t, original.OriginalSize, restored.OriginalSize)
	assert.Equal(t, original.CompressedSize, restored.CompressedSize)
	assert.Equal(t, original.CompressionRatio, restored.CompressionRatio)
}

func TestMetadata_WithCompression(t *testing.T) {
	// 확장된 Metadata 구조체가 압축 정보를 올바르게 포함하는지 확인
	compressionMeta := &CompressionMetadata{
		Algorithm:        "gzip",
		Level:            6,
		OriginalSize:     2048,
		CompressedSize:   1024,
		CompressionRatio: 0.5,
	}

	metadata := &Metadata{
		ETag:        "test-etag",
		IsNegative:  false,
		CachedAt:    time.Now(),
		Compression: compressionMeta,
	}

	// JSON 직렬화 테스트
	data, err := json.Marshal(metadata)
	require.NoError(t, err)

	// JSON 역직렬화 테스트
	var restored Metadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// 기본 메타데이터 검증
	assert.Equal(t, metadata.ETag, restored.ETag)
	assert.Equal(t, metadata.IsNegative, restored.IsNegative)

	// 압축 메타데이터 검증
	require.NotNil(t, restored.Compression)
	assert.Equal(t, compressionMeta.Algorithm, restored.Compression.Algorithm)
	assert.Equal(t, compressionMeta.Level, restored.Compression.Level)
	assert.Equal(t, compressionMeta.OriginalSize, restored.Compression.OriginalSize)
	assert.Equal(t, compressionMeta.CompressedSize, restored.Compression.CompressedSize)
	assert.Equal(t, compressionMeta.CompressionRatio, restored.Compression.CompressionRatio)
}

func TestMetadata_WithoutCompression(t *testing.T) {
	// 압축 정보가 없는 경우의 Metadata 테스트
	metadata := &Metadata{
		ETag:        "test-etag",
		IsNegative:  false,
		CachedAt:    time.Now(),
		Compression: nil, // 압축 정보 없음
	}

	// JSON 직렬화 테스트
	data, err := json.Marshal(metadata)
	require.NoError(t, err)

	// JSON 역직렬화 테스트
	var restored Metadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// 기본 메타데이터 검증
	assert.Equal(t, metadata.ETag, restored.ETag)
	assert.Equal(t, metadata.IsNegative, restored.IsNegative)

	// 압축 메타데이터가 nil인지 확인
	assert.Nil(t, restored.Compression)
}

func TestCompressionErrors(t *testing.T) {
	// 압축 관련 에러들이 올바르게 정의되었는지 확인
	errors := []error{
		ErrCompressionFailed,
		ErrDecompressionFailed,
		ErrUnsupportedAlgorithm,
		ErrCorruptedData,
		ErrInvalidCompressionLevel,
	}

	for _, err := range errors {
		assert.NotNil(t, err)
		assert.NotEmpty(t, err.Error())
		assert.Contains(t, err.Error(), "daramjwee:")
	}
}
