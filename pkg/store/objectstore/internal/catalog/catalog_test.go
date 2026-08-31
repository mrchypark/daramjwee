package catalog

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
)

func TestCatalogMigratesLegacyEnvelopeWithoutKeyCollisions(t *testing.T) {
	dir := t.TempDir()
	legacy := map[string]Entry{
		"format_version":     {SegmentPath: "version.seg", Length: 3},
		"entries":            {SegmentPath: "entries.seg", Length: 5},
		"_daramjwee_catalog": {PendingRemotePath: "segments/pack", Length: 3},
	}
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json.state"), []byte(markerClean), 0o644))

	cat, err := Open(dir)
	require.NoError(t, err)
	require.Len(t, cat.Entries(), 3)
	require.False(t, cat.Entries()["_daramjwee_catalog"].PendingRemoteSizeKnown)

	upgraded, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	require.NoError(t, err)
	var stored snapshot
	require.NoError(t, json.Unmarshal(upgraded, &stored))
	require.Equal(t, snapshotMagic, stored.Magic)
	require.Equal(t, currentSnapshotFormat, stored.FormatVersion)
	var oldDecoder map[string]Entry
	require.Error(t, json.Unmarshal(upgraded, &oldDecoder))
}

func TestCatalogAlwaysBlocksRollbackAfterOpeningLegacySnapshot(t *testing.T) {
	dir := t.TempDir()
	data, err := json.Marshal(map[string]Entry{"steady": {RemotePath: "blobs/steady", Length: 6}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json.state"), []byte(markerClean), 0o644))

	_, err = Open(dir)
	require.NoError(t, err)
	upgraded, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	require.NoError(t, err)
	var stored snapshot
	require.NoError(t, json.Unmarshal(upgraded, &stored))
	require.Equal(t, snapshotMagic, stored.Magic)
	var oldDecoder map[string]Entry
	require.Error(t, json.Unmarshal(upgraded, &oldDecoder))
}

func TestCatalogMigrationDoesNotCompletePartiallyPublishedUploadPlan(t *testing.T) {
	dir := t.TempDir()
	const remotePath = "segments/packed"
	legacy := map[string]Entry{
		"published": {RemotePath: remotePath, IntentCleanupPending: true, Generation: 1, PublicationToken: "published", Length: 4},
		"pending":   {PendingRemotePath: remotePath, Generation: 2, PublicationToken: "pending", Length: 4},
	}
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json.state"), []byte(markerClean), 0o644))

	cat, err := Open(dir)
	require.NoError(t, err)
	plan, ok := cat.UploadPlans()[remotePath]
	require.True(t, ok)
	require.Empty(t, plan.Terminal)
	require.Len(t, plan.Members, 2)
}

func TestCatalogUnchangedMutationsSkipPersist(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	entry := Entry{SegmentPath: filepath.Join(dir, "segment.seg"), Generation: 1}

	require.NoError(t, cat.Set("key", entry))

	var persistCalls atomic.Int32
	restoreWrite := writeFileFn
	writeFileFn = func(path string, data []byte, perm os.FileMode) error {
		if filepath.Base(path) == "snapshot.json.tmp" {
			persistCalls.Add(1)
		}
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
	syncPathFn = func(path string) error {
		if filepath.Base(path) == "snapshot.json.tmp" {
			return errors.New("sync path failed")
		}
		return restoreSyncPath(path)
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
	syncPathFn = func(path string) error {
		if filepath.Base(path) == "snapshot.json.tmp" {
			return errors.New("sync path failed")
		}
		return restoreSyncPath(path)
	}
	t.Cleanup(func() { syncPathFn = restoreSyncPath })

	committed, err := cat.Update("precommit", func(Entry, bool) (Entry, bool) {
		return Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}}, true
	})
	require.Error(t, err)
	assert.False(t, committed)
}

func TestCatalogSetPoisonsCatalogOnPostRenameFailure(t *testing.T) {
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
	require.ErrorIs(t, cat.Health(), ErrAmbiguousCommit)
	require.Error(t, cat.Set("later", Entry{}))

	syncDirFn = restoreSyncDir
	_, err = Open(dir)
	require.ErrorIs(t, err, ErrAmbiguousCommit)
	require.ErrorContains(t, err, "recovery marker")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json.state"), []byte(markerClean), 0o644))
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

func TestCatalogUpdateReportsPostRenameAmbiguityAndPoisons(t *testing.T) {
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
	require.ErrorIs(t, err, ErrAmbiguousCommit)
	assert.True(t, committed)
	require.ErrorIs(t, cat.Health(), ErrAmbiguousCommit)

	current, ok := cat.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, current)

	syncDirFn = restoreSyncDir
	_, err = Open(dir)
	require.ErrorIs(t, err, ErrAmbiguousCommit)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot.json.state"), []byte(markerClean), 0o644))
	reopened, err := Open(dir)
	require.NoError(t, err)
	reloaded, ok := reopened.Get("postrename")
	require.True(t, ok)
	assert.Equal(t, entry, reloaded)
}

func TestCatalogMarkerCleanupFailureIsAmbiguousAndPoisons(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncPath := syncPathFn
	markerSyncs := 0
	syncPathFn = func(path string) error {
		if filepath.Base(path) == "snapshot.json.state" {
			markerSyncs++
			if markerSyncs == 2 {
				return errors.New("marker sync failed")
			}
		}
		return restoreSyncPath(path)
	}
	t.Cleanup(func() { syncPathFn = restoreSyncPath })

	committed, err := cat.Update("key", func(Entry, bool) (Entry, bool) {
		return Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}}, true
	})
	require.True(t, committed)
	require.ErrorIs(t, err, ErrAmbiguousCommit)
	require.ErrorIs(t, err, daramjwee.ErrCommitOutcomeUnknown)
	require.ErrorIs(t, cat.Health(), ErrAmbiguousCommit)
}

func TestCatalogBlocksUpdatesAfterPostRenameFailure(t *testing.T) {
	dir := t.TempDir()
	cat, err := Open(dir)
	require.NoError(t, err)

	restoreSyncDir := syncDirFn
	syncDirFn = func(string) error { return errors.New("sync dir failed") }
	t.Cleanup(func() { syncDirFn = restoreSyncDir })

	entry := Entry{Metadata: daramjwee.Metadata{CacheTag: "v1"}}
	committed, err := cat.Update("key", func(Entry, bool) (Entry, bool) { return entry, true })
	require.True(t, committed)
	require.ErrorIs(t, err, ErrAmbiguousCommit)

	committed, err = cat.Update("key", func(current Entry, exists bool) (Entry, bool) {
		return current, exists
	})
	require.False(t, committed)
	require.ErrorIs(t, err, ErrAmbiguousCommit)
}
