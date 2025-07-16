package comp

import (
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
)

// LZ4Compressor는 LZ4 압축 알고리즘을 구현합니다
// 현재는 기본 구현만 제공하며, 실제 LZ4 라이브러리 통합은 향후 구현 예정입니다
type LZ4Compressor struct {
	level int
}

// NewLZ4Compressor는 새로운 LZ4 압축기를 생성합니다
func NewLZ4Compressor(level int) (*LZ4Compressor, error) {
	// LZ4 압축 레벨 범위: 1-12 (일반적인 범위)
	if level < 1 || level > 12 {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &LZ4Compressor{level: level}, nil
}

// NewDefaultLZ4Compressor는 기본 압축 레벨로 LZ4 압축기를 생성합니다
func NewDefaultLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{level: 1} // LZ4 기본 레벨 (속도 우선)
}

// Compress는 입력 스트림을 LZ4로 압축합니다
// 현재는 미구현 상태이며, 실제 LZ4 라이브러리 통합이 필요합니다
func (l *LZ4Compressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	return 0, fmt.Errorf("%w: lz4 compression not yet implemented", daramjwee.ErrUnsupportedAlgorithm)
}

// Decompress는 LZ4로 압축된 스트림을 해제합니다
// 현재는 미구현 상태이며, 실제 LZ4 라이브러리 통합이 필요합니다
func (l *LZ4Compressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	return 0, fmt.Errorf("%w: lz4 decompression not yet implemented", daramjwee.ErrUnsupportedAlgorithm)
}

// Algorithm은 압축 알고리즘 이름을 반환합니다
func (l *LZ4Compressor) Algorithm() string {
	return "lz4"
}

// Level은 압축 레벨을 반환합니다
func (l *LZ4Compressor) Level() int {
	return l.level
}
