package filestore

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/stretchr/testify/require"
)

// reciprocalEvictionPolicy makes each publish select the other test key once.
// The first Evict call stops while FileStore holds fs.mu.
type reciprocalEvictionPolicy struct {
	mu sync.Mutex

	keys    [2]string
	last    string
	used    bool
	pause   bool
	evicted []string

	firstEvictStarted chan struct{}
	releaseFirstEvict chan struct{}
	firstEvictOnce    sync.Once
}

func newReciprocalEvictionPolicy(keys [2]string) *reciprocalEvictionPolicy {
	return &reciprocalEvictionPolicy{
		keys:              keys,
		firstEvictStarted: make(chan struct{}),
		releaseFirstEvict: make(chan struct{}),
	}
}

func (p *reciprocalEvictionPolicy) Touch(string) {}

func (p *reciprocalEvictionPolicy) Add(key string, _ int64) {
	p.mu.Lock()
	p.last = key
	p.used = false
	p.mu.Unlock()
}

func (p *reciprocalEvictionPolicy) Remove(string) {}

func (p *reciprocalEvictionPolicy) Evict() []string {
	p.mu.Lock()
	if p.used {
		p.mu.Unlock()
		return nil
	}
	p.used = true
	other := p.keys[0]
	if p.last == other {
		other = p.keys[1]
	}
	pause := p.pause
	p.pause = false
	p.evicted = append(p.evicted, other)
	p.mu.Unlock()

	if pause {
		p.firstEvictOnce.Do(func() { close(p.firstEvictStarted) })
		<-p.releaseFirstEvict
	}
	return []string{other}
}

func (p *reciprocalEvictionPolicy) selectedVictims() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.evicted...)
}

func (p *reciprocalEvictionPolicy) pauseNextEviction() {
	p.mu.Lock()
	p.pause = true
	p.mu.Unlock()
}

func (p *reciprocalEvictionPolicy) release() {
	p.firstEvictOnce.Do(func() {})
	select {
	case <-p.releaseFirstEvict:
	default:
		close(p.releaseFirstEvict)
	}
}

func TestFileStore_ReciprocalEvictionsReleasePublishStripes(t *testing.T) {
	for _, test := range evictionWriteModes() {
		t.Run(test.name, func(t *testing.T) {
			fs := setupTestStore(t, test.options...)
			keys := disjointEvictionKeys(t, fs)
			policy := newReciprocalEvictionPolicy(keys)
			fs.policy = policy

			seedEvictionValue(t, fs, keys[0], "seed-a", "old-a")
			seedEvictionValue(t, fs, keys[1], "seed-b", "old-b")
			setEvictionCapacity(t, fs, 1)
			policy.pauseNextEviction()
			t.Cleanup(policy.release)

			first := stagedEvictionValue(t, fs, keys[0], "next-a", "new-a")
			replacement := stagedEvictionValue(t, fs, keys[1], "next-b", "new-b")
			firstDone := commitEvictionValue(first)

			waitForFirstEviction(t, policy)
			replacementDone := commitEvictionValue(replacement)
			waitForPublishedTag(t, fs.toDataPath(keys[1]), "next-b")
			policy.release()

			require.NoError(t, waitForCommit(t, firstDone))
			require.NoError(t, waitForCommit(t, replacementDone))
			require.Equal(t, []string{keys[1], keys[0]}, policy.selectedVictims())
			requireFileStoreValue(t, fs, keys[1], "next-b", "new-b")
		})
	}
}

func TestFileStore_StaleEvictionVictimKeepsEqualSizeReplacement(t *testing.T) {
	for _, test := range evictionWriteModes() {
		t.Run(test.name, func(t *testing.T) {
			fs := setupTestStore(t, test.options...)
			keys := disjointEvictionKeys(t, fs)
			policy := newReciprocalEvictionPolicy(keys)
			fs.policy = policy

			seedEvictionValue(t, fs, keys[0], "seed-a", "same-a")
			seedEvictionValue(t, fs, keys[1], "seed-b", "same-b")
			setEvictionCapacity(t, fs, 1)
			policy.pauseNextEviction()
			t.Cleanup(policy.release)

			first := stagedEvictionValue(t, fs, keys[0], "next-a", "same-a")
			replacement := stagedEvictionValue(t, fs, keys[1], "next-b", "same-b")
			firstDone := commitEvictionValue(first)

			waitForFirstEviction(t, policy)
			replacementDone := commitEvictionValue(replacement)
			waitForPublishedTag(t, fs.toDataPath(keys[1]), "next-b")
			policy.release()

			require.NoError(t, waitForCommit(t, firstDone))
			require.NoError(t, waitForCommit(t, replacementDone))
			require.Equal(t, []string{keys[1], keys[0]}, policy.selectedVictims())
			requireFileStoreValue(t, fs, keys[1], "next-b", "same-b")

			info, err := os.Stat(fs.toDataPath(keys[1]))
			require.NoError(t, err)
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			require.Equal(t, info.Size(), fs.fileSizes[keys[1]])
			require.Equal(t, info.Size(), fs.currentSize)
			require.Len(t, fs.fileSizes, 1)
		})
	}
}

func TestFileStore_ResidentSnapshotLifecycle(t *testing.T) {
	fs := setupTestStore(t)
	policy := newRecordingEvictionPolicy()
	fs.policy = policy
	key := "snapshot-lifecycle"

	seedEvictionValue(t, fs, key, "old-tag", "old")
	stale := residentSnapshot(t, fs, key)
	oldSize := trackedSize(t, fs, key)

	require.NoError(t, fs.Delete(context.Background(), key))
	require.NoError(t, fs.evictKey(stale))
	require.NoError(t, fs.evictKey(stale))
	require.Equal(t, int64(0), trackedCurrentSize(t, fs))
	require.NotContains(t, policy.entries, key)

	replacement := stagedEvictionValue(t, fs, key, "new-tag", "new")
	require.NoError(t, waitForCommit(t, commitEvictionValue(replacement)))
	require.Equal(t, oldSize, trackedSize(t, fs, key))
	requireFileStoreValue(t, fs, key, "new-tag", "new")

	fs.generationMu.Lock()
	_, floorPresent := fs.generations[key]
	_, writerPresent := fs.activeWriters[key]
	fs.generationMu.Unlock()
	require.False(t, floorPresent)
	require.False(t, writerPresent)

	require.NoError(t, fs.evictKey(stale))
	requireFileStoreValue(t, fs, key, "new-tag", "new")
	require.Equal(t, oldSize, trackedCurrentSize(t, fs))
	require.Contains(t, policy.entries, key)
}

func TestFileStore_ReopenedZeroGenerationSnapshots(t *testing.T) {
	dir := t.TempDir()
	key := "reopened-snapshot"

	first, err := New(dir, log.NewNopLogger(), WithEviction(policy.NewLRU()))
	require.NoError(t, err)
	seedEvictionValue(t, first, key, "old-tag", "old")

	loaded, err := New(dir, log.NewNopLogger(), WithEviction(policy.NewLRU()))
	require.NoError(t, err)
	zero := residentSnapshot(t, loaded, key)
	require.Zero(t, zero.generation)
	require.NoError(t, loaded.evictKey(zero))
	_, _, err = loaded.GetStream(context.Background(), key)
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	seedEvictionValue(t, loaded, key, "old-tag", "old")
	reopened, err := New(dir, log.NewNopLogger(), WithEviction(policy.NewLRU()))
	require.NoError(t, err)
	zero = residentSnapshot(t, reopened, key)
	require.Zero(t, zero.generation)

	replacement := stagedEvictionValue(t, reopened, key, "new-tag", "new")
	require.NoError(t, waitForCommit(t, commitEvictionValue(replacement)))
	require.NoError(t, reopened.evictKey(zero))
	requireFileStoreValue(t, reopened, key, "new-tag", "new")
	require.Equal(t, trackedSize(t, reopened, key), trackedCurrentSize(t, reopened))
}

func TestFileStore_OneStripeEvictionAndDeleteComplete(t *testing.T) {
	fs := setupTestStore(t, WithEviction(policy.NewLRU()))
	fs.lockManager = NewFileLockManager(1)
	seedEvictionValue(t, fs, "one-stripe-a", "seed-a", "old-a")
	setEvictionCapacity(t, fs, 1)

	replacement := stagedEvictionValue(t, fs, "one-stripe-b", "next-b", "new-b")
	require.NoError(t, waitForCommit(t, commitEvictionValue(replacement)))

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- fs.Delete(context.Background(), "one-stripe-b") }()
	require.NoError(t, waitForCommit(t, deleteDone))
	require.Equal(t, int64(0), trackedCurrentSize(t, fs))
	fs.mu.RLock()
	require.Empty(t, fs.fileSizes)
	fs.mu.RUnlock()
}

func evictionWriteModes() []struct {
	name    string
	options []Option
} {
	return []struct {
		name    string
		options []Option
	}{
		{name: "rename"},
		{name: "copy", options: []Option{WithCopyWrite()}},
	}
}

func disjointEvictionKeys(t *testing.T, fs *FileStore) [2]string {
	t.Helper()
	for a := 0; a < 128; a++ {
		first := fmt.Sprintf("eviction-a-%d", a)
		firstEncoded := fs.lockManager.getSlot(fs.toDataPath(first))
		firstLegacy := fs.lockManager.getSlot(fs.legacyDataPath(first))
		if firstEncoded == firstLegacy {
			continue
		}
		for b := 0; b < 128; b++ {
			second := fmt.Sprintf("eviction-b-%d", b)
			secondEncoded := fs.lockManager.getSlot(fs.toDataPath(second))
			secondLegacy := fs.lockManager.getSlot(fs.legacyDataPath(second))
			if firstEncoded != secondEncoded && firstEncoded != secondLegacy &&
				firstLegacy != secondEncoded && firstLegacy != secondLegacy && secondEncoded != secondLegacy {
				return [2]string{first, second}
			}
		}
	}
	t.Fatal("could not find keys with disjoint encoded and legacy stripes")
	return [2]string{}
}

func seedEvictionValue(t *testing.T, fs *FileStore, key, tag, body string) {
	t.Helper()
	sink := stagedEvictionValue(t, fs, key, tag, body)
	require.NoError(t, sink.Commit(context.Background()))
}

func stagedEvictionValue(t *testing.T, fs *FileStore, key, tag, body string) daramjwee.StagedWriteSink {
	t.Helper()
	sink, err := fs.BeginStagedSet(context.Background(), key, &daramjwee.Metadata{CacheTag: tag})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Abort() })
	_, err = sink.Write([]byte(body))
	require.NoError(t, err)
	return sink
}

func setEvictionCapacity(t *testing.T, fs *FileStore, capacity int64) {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	require.Greater(t, fs.currentSize, capacity)
	fs.capacity = capacity
}

func residentSnapshot(t *testing.T, fs *FileStore, key string) evictionVictim {
	t.Helper()
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	_, exists := fs.fileSizes[key]
	require.True(t, exists)
	return evictionVictim{key: key, generation: fs.residentGenerations[key]}
}

func trackedSize(t *testing.T, fs *FileStore, key string) int64 {
	t.Helper()
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	size, exists := fs.fileSizes[key]
	require.True(t, exists)
	return size
}

func trackedCurrentSize(t *testing.T, fs *FileStore) int64 {
	t.Helper()
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.currentSize
}

func commitEvictionValue(sink daramjwee.StagedWriteSink) <-chan error {
	done := make(chan error, 1)
	go func() { done <- sink.Commit(context.Background()) }()
	return done
}

func waitForFirstEviction(t *testing.T, policy *reciprocalEvictionPolicy) {
	t.Helper()
	select {
	case <-policy.firstEvictStarted:
	case <-time.After(time.Second):
		t.Fatal("first eviction policy call did not start")
	}
}

func waitForPublishedTag(t *testing.T, path, tag string) {
	t.Helper()
	require.Eventually(t, func() bool {
		file, err := os.Open(path)
		if err != nil {
			return false
		}
		defer file.Close()
		metadata, _, _, _, err := readStoredMetadata(file)
		return err == nil && metadata.CacheTag == tag
	}, time.Second, 10*time.Millisecond, "replacement did not publish before waiting on fs.mu")
}

func waitForCommit(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("commit did not finish")
		return nil
	}
}

func requireFileStoreValue(t *testing.T, fs *FileStore, key, tag, body string) {
	t.Helper()
	reader, metadata, err := fs.GetStream(context.Background(), key)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, tag, metadata.CacheTag)
	require.Equal(t, body, string(data))
}
