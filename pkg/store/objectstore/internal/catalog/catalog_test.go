package catalog

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
)

func TestCatalogUnchangedMutationsSkipPersist(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	entry := Entry{SegmentPath: filepath.Join(dir, "segment.seg"), Generation: 1}

	require.NoError(t, cat.Set("key", entry))

	var persistCalls atomic.Int32
	restoreWrite := writeFileFn
	writeFileFn = func(path string, data []byte, perm os.FileMode) error {
		persistCalls.Add(1)
		return restoreWrite(path, data, perm)
	}
	t.Cleanup(func() {
		writeFileFn = restoreWrite
	})

	require.NoError(t, cat.Set("key", entry))
	_, err = cat.Update("key", func(current Entry, exists bool) (Entry, bool) {
		return current, exists
	})
	require.NoError(t, err)
	require.NoError(t, cat.Delete("missing-key"))
	require.Equal(t, int32(0), persistCalls.Load(), "unchanged mutations must not rewrite the snapshot")

	_, err = cat.Update("key", func(current Entry, exists bool) (Entry, bool) {
		current.Generation = 2
		return current, true
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), persistCalls.Load(), "real mutations must still persist")

	updated, ok := cat.Get("key")
	require.True(t, ok)
	assert.Equal(t, uint64(2), updated.Generation)
}

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

	err = cat.Set("precommit", Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}})
	require.Error(t, err)

	_, ok := cat.Get("precommit")
	assert.False(t, ok)

	reopened, err := Open(dir)
	require.NoError(t, err)
	_, ok = reopened.Get("precommit")
	assert.False(t, ok)
}

func TestCatalogUpdateReportsPreCommitFailure(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncPath := syncPathFn
	syncPathFn = func(string) error { return errors.New("sync path failed") }
	t.Cleanup(func() { syncPathFn = restoreSyncPath })

	committed, err := cat.Update("precommit", func(Entry, bool) (Entry, bool) {
		return Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}}, true
	})
	require.Error(t, err)
	assert.False(t, committed)
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
		Metadata:    daramjwee.Metadata{CacheTag: "v2"},
	}

	err = cat.Set("postrename", entry)
	require.Error(t, err)

	current, ok := cat.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, current)

	_, err = Open(dir)
	require.ErrorContains(t, err, "sync dir failed")

	syncDirFn = restoreSyncDir
	reopened, err := Open(dir)
	require.NoError(t, err)
	reloaded, ok := reopened.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, reloaded)
}

func TestOpenFailsWhenExistingSnapshotDirectoryCannotSync(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)
	require.NoError(t, cat.Set("key", Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}}))

	restoreSyncDir := syncDirFn
	syncDirFn = func(string) error { return errors.New("sync dir failed") }
	t.Cleanup(func() { syncDirFn = restoreSyncDir })

	_, err = Open(dir)
	require.ErrorContains(t, err, "sync dir failed")
}

func TestCatalogUpdateReportsPostRenameCommit(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncDir := syncDirFn
	syncDirFn = func(string) error { return errors.New("sync dir failed") }
	t.Cleanup(func() { syncDirFn = restoreSyncDir })

	entry := Entry{
		SegmentPath: filepath.Join(dir, "segment.seg"),
		Metadata:    daramjwee.Metadata{CacheTag: "v2"},
	}
	committed, err := cat.Update("postrename", func(Entry, bool) (Entry, bool) {
		return entry, true
	})
	require.Error(t, err)
	assert.True(t, committed)

	current, ok := cat.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, current)
}
