package comp

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
)

// GzipCompressor implements the gzip compression algorithm
type GzipCompressor struct {
	level int
}

// NewGzipCompressor creates a new gzip compressor
func NewGzipCompressor(level int) (*GzipCompressor, error) {
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &GzipCompressor{level: level}, nil
}

// NewDefaultGzipCompressor creates a gzip compressor with default compression level
func NewDefaultGzipCompressor() *GzipCompressor {
	return &GzipCompressor{level: gzip.DefaultCompression}
}

// Compress compresses the input stream with gzip
func (g *GzipCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	writer, err := gzip.NewWriterLevel(dst, g.level)
	if err != nil {
		return 0, fmt.Errorf("%w: failed to create gzip writer: %v", daramjwee.ErrCompressionFailed, err)
	}
	defer writer.Close()

	written, err := io.Copy(writer, src)
	if err != nil {
		return written, fmt.Errorf("%w: failed to compress data: %v", daramjwee.ErrCompressionFailed, err)
	}

	if err := writer.Close(); err != nil {
		return written, fmt.Errorf("%w: failed to finalize compression: %v", daramjwee.ErrCompressionFailed, err)
	}

	return written, nil
}

// Decompress decompresses a gzip-compressed stream
func (g *GzipCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	reader, err := gzip.NewReader(src)
	if err != nil {
		return 0, fmt.Errorf("%w: failed to create gzip reader: %v", daramjwee.ErrDecompressionFailed, err)
	}
	defer reader.Close()

	written, err := io.Copy(dst, reader)
	if err != nil {
		return written, fmt.Errorf("%w: failed to decompress data: %v", daramjwee.ErrDecompressionFailed, err)
	}

	return written, nil
}

// Algorithm returns the compression algorithm name
func (g *GzipCompressor) Algorithm() string {
	return "gzip"
}

// Level returns the compression level
func (g *GzipCompressor) Level() int {
	return g.level
}
