//go:build !windows

package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_AbortReturnsRemoveErrorWhenFileAlreadyClosed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can unlink files from non-writable directories")
	}

	writer, err := Open(t.TempDir(), "ef", "segment-3")
	require.NoError(t, err)
	_, err = writer.Write([]byte("payload"))
	require.NoError(t, err)

	require.NoError(t, writer.file.Close())
	activeDir := filepath.Dir(writer.activePath)
	require.NoError(t, os.Chmod(activeDir, 0o555))
	t.Cleanup(func() {
		_ = os.Chmod(activeDir, 0o755)
		_ = os.Remove(writer.activePath)
	})

	err = writer.Abort()
	require.Error(t, err)
	assert.False(t, errors.Is(err, os.ErrClosed))
	assert.ErrorIs(t, err, os.ErrPermission)
}
