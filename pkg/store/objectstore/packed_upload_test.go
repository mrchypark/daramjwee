package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestPackedRecordReaderStreamsExactSectionsOneFileAtATime(t *testing.T) {
	files := newTrackingPackedFiles(map[string]string{
		"first.seg":  "--alpha--tail",
		"second.seg": "!beta?",
		"third.seg":  "xxgamma",
	})
	records := []pendingFlushRecord{
		{key: "a", entry: localCatalogEntry{SegmentPath: "first.seg", Offset: 2, Length: 5}},
		{key: "b", entry: localCatalogEntry{SegmentPath: "second.seg", Offset: 1, Length: 4}},
		{key: "c", entry: localCatalogEntry{SegmentPath: "third.seg", Offset: 2, Length: 5}},
	}

	reader, offsets, err := newPackedRecordReader(records, files.open)
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"a": 0, "b": 5, "c": 9}, offsets)

	var body bytes.Buffer
	_, err = io.CopyBuffer(&body, reader, make([]byte, 2))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "alphabetagamma", body.String())
	assert.Equal(t, 1, files.maxOpenCount())
	assert.Equal(t, 0, files.openCount())
	assert.Equal(t, []string{
		"open:first.seg", "close:first.seg",
		"open:second.seg", "close:second.seg",
		"open:third.seg", "close:third.seg",
	}, files.eventsSnapshot())
}

func TestPackedRecordReaderExplicitCloseClosesCurrentFile(t *testing.T) {
	files := newTrackingPackedFiles(map[string]string{"only.seg": "payload"})
	reader, _, err := newPackedRecordReader([]pendingFlushRecord{
		{key: "only", entry: localCatalogEntry{SegmentPath: "only.seg", Length: 7}},
	}, files.open)
	require.NoError(t, err)

	buffer := make([]byte, 1)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, reader.Close())
	assert.Equal(t, 0, files.openCount())
	assert.Equal(t, []string{"open:only.seg", "close:only.seg"}, files.eventsSnapshot())

	_, err = reader.Read(buffer)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestPackedRecordReaderJoinsReadAndCloseErrors(t *testing.T) {
	readErr := errors.New("read failed")
	closeErr := errors.New("close failed")
	files := newTrackingPackedFiles(map[string]string{"broken.seg": "payload"})
	files.readErrors["broken.seg"] = readErr
	files.closeErrors["broken.seg"] = closeErr
	reader, _, err := newPackedRecordReader([]pendingFlushRecord{
		{key: "broken", entry: localCatalogEntry{SegmentPath: "broken.seg", Length: 7}},
	}, files.open)
	require.NoError(t, err)

	_, err = reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, readErr)
	require.ErrorIs(t, err, closeErr)
	assert.Equal(t, 0, files.openCount())
	assert.Equal(t, []string{"open:broken.seg", "close:broken.seg"}, files.eventsSnapshot())
}

func TestPackedRecordReaderRejectsSectionBoundsOverflowBeforeOpen(t *testing.T) {
	openCalls := 0
	reader, offsets, err := newPackedRecordReader([]pendingFlushRecord{
		{key: "overflow", entry: localCatalogEntry{
			SegmentPath: "overflow.seg",
			Offset:      int64(^uint64(0) >> 1),
			Length:      1,
		}},
	}, func(string) (packedSourceFile, error) {
		openCalls++
		return nil, errors.New("must not open")
	})
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Nil(t, offsets)
	assert.Zero(t, openCalls)
}

func TestStoreUploadPackedBodyClosesReaderWhenUploadStopsEarly(t *testing.T) {
	uploadErr := errors.New("upload stopped early")
	files := newTrackingPackedFiles(map[string]string{"early.seg": "payload"})
	reader, _, err := newPackedRecordReader([]pendingFlushRecord{
		{key: "early", entry: localCatalogEntry{SegmentPath: "early.seg", Length: 7}},
	}, files.open)
	require.NoError(t, err)
	store := &Store{bucket: &earlyExitUploadBucket{
		Bucket: objstore.NewInMemBucket(),
		err:    uploadErr,
	}}

	err = store.uploadPackedBody(context.Background(), "segments/packed", reader)
	require.ErrorIs(t, err, uploadErr)
	assert.Equal(t, 0, files.openCount())
	assert.Equal(t, []string{"open:early.seg", "close:early.seg"}, files.eventsSnapshot())
}

func TestStorePackedFlushPinsSourcesUntilUploadReaderCloses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *Store, string) <-chan error
	}{
		{
			name: "overwrite",
			mutate: func(t *testing.T, store *Store, key string) <-chan error {
				done := make(chan error, 1)
				go func() {
					writer, err := store.BeginSet(context.Background(), key, nil)
					if err == nil {
						_, err = io.WriteString(writer, "replacement payload")
					}
					if err == nil {
						err = writer.Close()
					}
					done <- err
				}()
				return done
			},
		},
		{
			name: "delete",
			mutate: func(t *testing.T, store *Store, key string) <-chan error {
				done := make(chan error, 1)
				go func() { done <- store.Delete(context.Background(), key) }()
				return done
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseBucket := objstore.NewInMemBucket()
			bucket := newBlockingPackedUploadBucket(baseBucket)
			t.Cleanup(bucket.releaseUpload)
			store := New(bucket, log.NewNopLogger(), WithDir(t.TempDir()))
			store.autoFlush = false
			keyA, keyB := sameShardKeys("pin-packed-source-" + tc.name)
			writePendingObject(t, store, keyA, "original payload")
			writePendingObject(t, store, keyB, "neighbor payload")

			entryA, ok := store.catalog.Get(keyA)
			require.True(t, ok)
			entryB, ok := store.catalog.Get(keyB)
			require.True(t, ok)

			flushDone := make(chan error, 1)
			go func() { flushDone <- store.flushPending(context.Background()) }()
			select {
			case <-bucket.uploadStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("packed upload did not reach the before-read gate")
			}

			assert.Equal(t, 1, store.localSegmentRefCount(entryA.SegmentPath))
			assert.Equal(t, 1, store.localSegmentRefCount(entryB.SegmentPath))
			mutationDone := tc.mutate(t, store, keyA)
			select {
			case err := <-mutationDone:
				t.Fatalf("mutation completed before publication: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			_, err := os.Stat(entryA.SegmentPath)
			require.NoError(t, err, "concurrent mutation reclaimed a not-yet-read packed source")

			bucket.releaseUpload()
			require.NoError(t, <-flushDone)
			require.NoError(t, <-mutationDone)
			_, err = os.Stat(entryA.SegmentPath)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestStorePinPackedSegmentsReleasesEarlierPinsOnFailure(t *testing.T) {
	store := New(objstore.NewInMemBucket(), log.NewNopLogger(), WithDir(t.TempDir()))
	store.autoFlush = false
	existingPath := filepath.Join(t.TempDir(), "existing.seg")
	require.NoError(t, os.WriteFile(existingPath, []byte("body"), 0o600))
	missingPath := filepath.Join(t.TempDir(), "missing.seg")

	release, err := store.pinPackedSegments([]pendingFlushRecord{
		{key: "first", entry: localCatalogEntry{SegmentPath: existingPath, Length: 4}},
		{key: "second", entry: localCatalogEntry{SegmentPath: missingPath, Length: 4}},
	})
	require.Error(t, err)
	assert.Nil(t, release)
	assert.Zero(t, store.localSegmentRefCount(existingPath))
	assert.Zero(t, store.localSegmentRefCount(missingPath))
}

type trackingPackedFiles struct {
	mu          sync.Mutex
	contents    map[string]string
	readErrors  map[string]error
	closeErrors map[string]error
	events      []string
	openFiles   int
	maxOpen     int
}

func newTrackingPackedFiles(contents map[string]string) *trackingPackedFiles {
	return &trackingPackedFiles{
		contents:    contents,
		readErrors:  make(map[string]error),
		closeErrors: make(map[string]error),
	}
}

func (s *Store) localSegmentRefCount(path string) int {
	s.segmentRefsMu.Lock()
	defer s.segmentRefsMu.Unlock()
	return s.segmentRefs[path]
}

func (f *trackingPackedFiles) open(path string) (packedSourceFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	content, ok := f.contents[path]
	if !ok {
		return nil, errors.New("file not found")
	}
	f.openFiles++
	if f.openFiles > f.maxOpen {
		f.maxOpen = f.openFiles
	}
	f.events = append(f.events, "open:"+path)
	return &trackingPackedFile{
		Reader:   bytes.NewReader([]byte(content)),
		owner:    f,
		path:     path,
		readErr:  f.readErrors[path],
		closeErr: f.closeErrors[path],
	}, nil
}

func (f *trackingPackedFiles) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openFiles
}

func (f *trackingPackedFiles) maxOpenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxOpen
}

func (f *trackingPackedFiles) eventsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

type trackingPackedFile struct {
	*bytes.Reader
	owner    *trackingPackedFiles
	path     string
	readErr  error
	closeErr error
	closed   bool
}

func (f *trackingPackedFile) ReadAt(p []byte, off int64) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.Reader.ReadAt(p, off)
}

func (f *trackingPackedFile) Close() error {
	f.owner.mu.Lock()
	defer f.owner.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.owner.openFiles--
	f.owner.events = append(f.owner.events, "close:"+f.path)
	return f.closeErr
}

type earlyExitUploadBucket struct {
	objstore.Bucket
	err error
}

func (b *earlyExitUploadBucket) Upload(_ context.Context, _ string, reader io.Reader, _ ...objstore.ObjectUploadOption) error {
	_, _ = reader.Read(make([]byte, 1))
	return b.err
}

type blockingPackedUploadBucket struct {
	objstore.Bucket
	uploadStarted chan struct{}
	uploadRelease chan struct{}
	startOnce     sync.Once
	releaseOnce   sync.Once
}

func newBlockingPackedUploadBucket(bucket objstore.Bucket) *blockingPackedUploadBucket {
	return &blockingPackedUploadBucket{
		Bucket:        bucket,
		uploadStarted: make(chan struct{}),
		uploadRelease: make(chan struct{}),
	}
}

func (b *blockingPackedUploadBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...objstore.ObjectUploadOption) error {
	blocked := false
	if strings.Contains(name, "segments/") {
		b.startOnce.Do(func() {
			blocked = true
			close(b.uploadStarted)
		})
	}
	if blocked {
		select {
		case <-b.uploadRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func (b *blockingPackedUploadBucket) releaseUpload() {
	b.releaseOnce.Do(func() { close(b.uploadRelease) })
}
