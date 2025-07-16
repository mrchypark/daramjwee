package comp

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/mrchypark/daramjwee"
)

// ZstdCompressor implements the zstd compression algorithm
type ZstdCompressor struct {
	level int
}

// NewZstdCompressor creates a new zstd compressor with the specified compression level
func NewZstdCompressor(level int) (*ZstdCompressor, error) {
	// zstd compression level range: 1-22 (typical range)
	if level < 1 || level > 22 {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &ZstdCompressor{level: level}, nil
}

// NewDefaultZstdCompressor creates a zstd compressor with default compression level
func NewDefaultZstdCompressor() *ZstdCompressor {
	return &ZstdCompressor{level: 3} // zstd default level
}

// Compress compresses the input stream with zstd
func (z *ZstdCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	// Map our level to zstd encoder level
	encoderLevel := zstd.EncoderLevelFromZstd(z.level)

	// Create encoder with specified compression level
	encoder, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(encoderLevel))
	if err != nil {
		return 0, fmt.Errorf("%w: failed to create zstd encoder: %v", daramjwee.ErrCompressionFailed, err)
	}
	defer encoder.Close()

	written, err := io.Copy(encoder, src)
	if err != nil {
		return written, fmt.Errorf("%w: failed to compress data: %v", daramjwee.ErrCompressionFailed, err)
	}

	if err := encoder.Close(); err != nil {
		return written, fmt.Errorf("%w: failed to finalize compression: %v", daramjwee.ErrCompressionFailed, err)
	}

	return written, nil
}

// Decompress decompresses a zstd-compressed stream
func (z *ZstdCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	decoder, err := zstd.NewReader(src)
	if err != nil {
		return 0, fmt.Errorf("%w: failed to create zstd decoder: %v", daramjwee.ErrDecompressionFailed, err)
	}
	defer decoder.Close()

	written, err := io.Copy(dst, decoder)
	if err != nil {
		return written, fmt.Errorf("%w: failed to decompress data: %v", daramjwee.ErrDecompressionFailed, err)
	}

	return written, nil
}

// Algorithm returns the compression algorithm name
func (z *ZstdCompressor) Algorithm() string {
	return "zstd"
}

// Level returns the compression level
func (z *ZstdCompressor) Level() int {
	return z.level
}
