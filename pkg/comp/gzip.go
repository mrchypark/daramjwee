package comp

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
)

// GzipCompressor는 gzip 압축 알고리즘을 구현합니다
type GzipCompressor struct {
	level int
}

// NewGzipCompressor는 새로운 gzip 압축기를 생성합니다
func NewGzipCompressor(level int) (*GzipCompressor, error) {
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &GzipCompressor{level: level}, nil
}

// NewDefaultGzipCompressor는 기본 압축 레벨로 gzip 압축기를 생성합니다
func NewDefaultGzipCompressor() *GzipCompressor {
	return &GzipCompressor{level: gzip.DefaultCompression}
}

// Compress는 입력 스트림을 gzip으로 압축합니다
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

// Decompress는 gzip으로 압축된 스트림을 해제합니다
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

// Algorithm은 압축 알고리즘 이름을 반환합니다
func (g *GzipCompressor) Algorithm() string {
	return "gzip"
}

// Level은 압축 레벨을 반환합니다
func (g *GzipCompressor) Level() int {
	return g.level
}
