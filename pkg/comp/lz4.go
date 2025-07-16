package comp

import (
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
	"github.com/pierrec/lz4/v4"
)

// LZ4Compressor implements the LZ4 compression algorithm
type LZ4Compressor struct {
	level int
}

// NewLZ4Compressor creates a new LZ4 compressor with the specified compression level
func NewLZ4Compressor(level int) (*LZ4Compressor, error) {
	// LZ4 compression level range: 1-12 (typical range)
	if level < 1 || level > 12 {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &LZ4Compressor{level: level}, nil
}

// NewDefaultLZ4Compressor creates an LZ4 compressor with default compression level
func NewDefaultLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{level: 1} // LZ4 default level (speed priority)
}

// Compress compresses the input stream with LZ4
func (l *LZ4Compressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	var options []lz4.Option

	// Set compression level based on our level setting
	if l.level > 6 {
		// Use high compression for higher levels
		options = append(options, lz4.CompressionLevelOption(lz4.Level9))
	} else if l.level > 3 {
		// Use medium compression
		options = append(options, lz4.CompressionLevelOption(lz4.Level6))
	} else {
		// Use fast compression for low levels
		options = append(options, lz4.CompressionLevelOption(lz4.Fast))
	}

	writer := lz4.NewWriter(dst)

	// Apply options
	for _, opt := range options {
		if err := opt(writer); err != nil {
			return 0, fmt.Errorf("%w: failed to set LZ4 options: %v", daramjwee.ErrCompressionFailed, err)
		}
	}

	written, err := io.Copy(writer, src)
	if err != nil {
		writer.Close()
		return written, fmt.Errorf("%w: failed to compress data: %v", daramjwee.ErrCompressionFailed, err)
	}

	if err := writer.Close(); err != nil {
		return written, fmt.Errorf("%w: failed to finalize compression: %v", daramjwee.ErrCompressionFailed, err)
	}

	return written, nil
}

// Decompress decompresses an LZ4-compressed stream
func (l *LZ4Compressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	reader := lz4.NewReader(src)

	written, err := io.Copy(dst, reader)
	if err != nil {
		return written, fmt.Errorf("%w: failed to decompress data: %v", daramjwee.ErrDecompressionFailed, err)
	}

	return written, nil
}

// Algorithm returns the compression algorithm name
func (l *LZ4Compressor) Algorithm() string {
	return "lz4"
}

// Level returns the compression level
func (l *LZ4Compressor) Level() int {
	return l.level
}
