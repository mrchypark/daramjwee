package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_SealMovesSegmentToSealedDirectory(t *testing.T) {
	writer, err := Open(t.TempDir(), "ab", "segment-1")
	require.NoError(t, err)

	n, err := writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.Equal(t, len("payload"), n)

	sealedPath, size, err := writer.Seal()
	require.NoError(t, err)
	assert.EqualValues(t, len("payload"), size)
	assert.Equal(t, filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(writer.activePath))), "sealed", "ab", "segment-1.seg"), sealedPath)

	_, err = os.Stat(writer.activePath)
	assert.True(t, os.IsNotExist(err))

	data, err := os.ReadFile(sealedPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data)
}

func TestWriter_AbortRemovesActiveSegment(t *testing.T) {
	writer, err := Open(t.TempDir(), "cd", "segment-2")
	require.NoError(t, err)

	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Abort())

	_, err = os.Stat(writer.activePath)
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(writer.sealedPath)
	assert.True(t, os.IsNotExist(err))
}
