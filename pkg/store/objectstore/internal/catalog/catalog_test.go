package catalog

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalogSetRollsBackOnPreCommitFailure(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncPath := syncPathFn
	syncPathFn = func(string) error {
		return errors.New("sync path failed")
	}
	t.Cleanup(func() {
		syncPathFn = restoreSyncPath
	})

	err = cat.Set("precommit", Entry{Metadata: daramjwee.Metadata{ETag: "v1"}})
	require.Error(t, err)

	_, ok := cat.Get("precommit")
	assert.False(t, ok)

	reopened, err := Open(dir)
	require.NoError(t, err)
	_, ok = reopened.Get("precommit")
	assert.False(t, ok)
}

func TestCatalogSetKeepsCommittedStateOnPostRenameFailure(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncDir := syncDirFn
	syncDirFn = func(string) error {
		return errors.New("sync dir failed")
	}
	t.Cleanup(func() {
		syncDirFn = restoreSyncDir
	})

	entry := Entry{
		SegmentPath: filepath.Join(dir, "segment.seg"),
		Metadata:    daramjwee.Metadata{ETag: "v2"},
	}

	err = cat.Set("postrename", entry)
	require.Error(t, err)

	current, ok := cat.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, current)

	reopened, err := Open(dir)
	require.NoError(t, err)
	reloaded, ok := reopened.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, reloaded)
}
