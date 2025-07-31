package unit

import (
	"github.com/mrchypark/daramjwee"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressionType_Constants(t *testing.T) {
	// Verify that compression type constants are correctly defined
	assert.Equal(t, daramjwee.CompressionType("gzip"), CompressionGzip)
	assert.Equal(t, daramjwee.CompressionType("zstd"), CompressionZstd)
	assert.Equal(t, daramjwee.CompressionType("lz4"), CompressionLZ4)
	assert.Equal(t, daramjwee.CompressionType("none"), CompressionNone)
}

func TestCompressionMetadata_JSONSerialization(t *testing.T) {
	// Verify that daramjwee.CompressionMetadata serializes/deserializes correctly to/from JSON
	original := &daramjwee.CompressionMetadata{
		Algorithm:        "gzip",
		Level:            6,
		OriginalSize:     1024,
		CompressedSize:   512,
		CompressionRatio: 0.5,
	}

	// JSON serialization
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// JSON deserialization
	var restored CompressionMetadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// Value verification
	assert.Equal(t, original.Algorithm, restored.Algorithm)
	assert.Equal(t, original.Level, restored.Level)
	assert.Equal(t, original.OriginalSize, restored.OriginalSize)
	assert.Equal(t, original.CompressedSize, restored.CompressedSize)
	assert.Equal(t, original.CompressionRatio, restored.CompressionRatio)
}

func TestMetadata_WithCompression(t *testing.T) {
	// Verify that the extended daramjwee.Metadata struct correctly includes compression information
	compressionMeta := &daramjwee.CompressionMetadata{
		Algorithm:        "gzip",
		Level:            6,
		OriginalSize:     2048,
		CompressedSize:   1024,
		CompressionRatio: 0.5,
	}

	metadata := &daramjwee.Metadata{
		ETag:        "test-etag",
		IsNegative:  false,
		CachedAt:    time.Now(),
		Compression: compressionMeta,
	}

	// JSON serialization test
	data, err := json.Marshal(metadata)
	require.NoError(t, err)

	// JSON deserialization test
	var restored Metadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// Basic metadata verification
	assert.Equal(t, metadata.ETag, restored.ETag)
	assert.Equal(t, metadata.IsNegative, restored.IsNegative)

	// Compression metadata verification
	require.NotNil(t, restored.Compression)
	assert.Equal(t, compressionMeta.Algorithm, restored.Compression.Algorithm)
	assert.Equal(t, compressionMeta.Level, restored.Compression.Level)
	assert.Equal(t, compressionMeta.OriginalSize, restored.Compression.OriginalSize)
	assert.Equal(t, compressionMeta.CompressedSize, restored.Compression.CompressedSize)
	assert.Equal(t, compressionMeta.CompressionRatio, restored.Compression.CompressionRatio)
}

func TestMetadata_WithoutCompression(t *testing.T) {
	// Test daramjwee.Metadata without compression information
	metadata := &daramjwee.Metadata{
		ETag:        "test-etag",
		IsNegative:  false,
		CachedAt:    time.Now(),
		Compression: nil, // No compression information
	}

	// JSON serialization test
	data, err := json.Marshal(metadata)
	require.NoError(t, err)

	// JSON deserialization test
	var restored Metadata
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	// Basic metadata verification
	assert.Equal(t, metadata.ETag, restored.ETag)
	assert.Equal(t, metadata.IsNegative, restored.IsNegative)

	// Verify that compression metadata is nil
	assert.Nil(t, restored.Compression)
}

func TestCompressionErrors(t *testing.T) {
	// Verify that compression-related errors are correctly defined
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
