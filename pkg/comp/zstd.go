package comp

import (
	"fmt"
	"io"

	"github.com/mrchypark/daramjwee"
)

// ZstdCompressor는 zstd 압축 알고리즘을 구현합니다
// 현재는 기본 구현만 제공하며, 실제 zstd 라이브러리 통합은 향후 구현 예정입니다
type ZstdCompressor struct {
	level int
}

// NewZstdCompressor는 새로운 zstd 압축기를 생성합니다
func NewZstdCompressor(level int) (*ZstdCompressor, error) {
	// zstd 압축 레벨 범위: 1-22 (일반적인 범위)
	if level < 1 || level > 22 {
		return nil, daramjwee.ErrInvalidCompressionLevel
	}
	return &ZstdCompressor{level: level}, nil
}

// NewDefaultZstdCompressor는 기본 압축 레벨로 zstd 압축기를 생성합니다
func NewDefaultZstdCompressor() *ZstdCompressor {
	return &ZstdCompressor{level: 3} // zstd 기본 레벨
}

// Compress는 입력 스트림을 zstd로 압축합니다
// 현재는 미구현 상태이며, 실제 zstd 라이브러리 통합이 필요합니다
func (z *ZstdCompressor) Compress(dst io.Writer, src io.Reader) (int64, error) {
	return 0, fmt.Errorf("%w: zstd compression not yet implemented", daramjwee.ErrUnsupportedAlgorithm)
}

// Decompress는 zstd로 압축된 스트림을 해제합니다
// 현재는 미구현 상태이며, 실제 zstd 라이브러리 통합이 필요합니다
func (z *ZstdCompressor) Decompress(dst io.Writer, src io.Reader) (int64, error) {
	return 0, fmt.Errorf("%w: zstd decompression not yet implemented", daramjwee.ErrUnsupportedAlgorithm)
}

// Algorithm은 압축 알고리즘 이름을 반환합니다
func (z *ZstdCompressor) Algorithm() string {
	return "zstd"
}

// Level은 압축 레벨을 반환합니다
func (z *ZstdCompressor) Level() int {
	return z.level
}
