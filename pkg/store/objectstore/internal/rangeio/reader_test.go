package rangeio

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader_ReadReturnsUnexpectedEOFOnZeroProgressPage(t *testing.T) {
	reader := New(8, 4, func(pageIndex int64) ([]byte, error) {
		return []byte{}, nil
	}, nil)

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
