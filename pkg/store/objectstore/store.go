package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/internal/stripedlock"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/blockcache"
	internalcatalog "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/catalog"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/rangeio"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/segment"
	internalshard "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/shard"
)

type layout string

const (
	layoutWhole layout = "whole"
	layoutPaged layout = "paged"
)

type manifest struct {
	Version  string             `json:"version"`
	Layout   layout             `json:"layout"`
	BlobPath string             `json:"blob_path"`
	Size     int64              `json:"size"`
	PageSize int64              `json:"page_size,omitempty"`
	Metadata daramjwee.Metadata `json:"metadata"`
}

type contextSemaphore chan struct{}

func newContextSemaphore() contextSemaphore {
	semaphore := make(contextSemaphore, 1)
	semaphore <- struct{}{}
	return semaphore
}

func (s contextSemaphore) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s:
		return nil
	}
}

func (s contextSemaphore) release() {
	s <- struct{}{}
}

// Store is a first-party object storage backend.
// It currently publishes immutable blob versions via internal manifest pointers.
type Store struct {
	bucket              objstore.Bucket
	logger              log.Logger
	dataDir             string
	prefix              string
	gcGrace             time.Duration
	packThreshold       int64
	pagedThreshold      int64
	pageSize            int64
	blockCache          *blockcache.Cache
	pageCache           *blockcache.Cache
	checkpointCache     *checkpointCache
	catalog             *internalcatalog.Catalog
	updateCatalog       func(string, func(localCatalogEntry, bool) (localCatalogEntry, bool)) (bool, error)
	updateCatalogManyIf func(map[string]localCatalogEntry, map[string]localCatalogEntry) (bool, error)
	updateCatalogState  func(map[string]localCatalogEntry, map[string]localCatalogEntry, map[string]uploadPlan, []string) (bool, error)
	syncCatalog         func() error
	lockManager         *stripedlock.Manager
	manifestCache       *manifestCache
	instanceID          string
	versionSeq          atomic.Uint64
	generationSeq       atomic.Uint64
	initErr             error
	segmentRefsMu       sync.Mutex
	segmentRefs         map[string]int
	reclaimableSegs     map[string]struct{}
	pendingDurableSegs  map[string]struct{}
	flushMu             sync.Mutex
	flushRun            contextSemaphore
	remoteState         contextSemaphore
	pendingShards       map[string]struct{}
	flushScheduled      bool
	flushRetryDelay     time.Duration
	scheduleFlushAfter  func(time.Duration, func())
	afterAutoFlushCheck func()
	beforeFlushAcquire  func()
	autoFlush           bool
	now                 func() time.Time
	openSegmentWriter   func(root, shard, segmentID string) (segmentWriter, error)
	isClosed            atomic.Bool
	writersMu           sync.Mutex
	writers             sync.WaitGroup
	closeOnce           sync.Once
	closeDone           chan struct{}
	closeErr            error
}

func (s *Store) GetStreamUsesContext() bool { return true }

func (s *Store) BeginSetUsesContext() bool { return true }

var _ daramjwee.Store = (*Store)(nil)
var _ daramjwee.TierValidator = (*Store)(nil)

var openCatalog = internalcatalog.Open

// New creates a new object storage backend.
func New(bucket objstore.Bucket, logger log.Logger, opts ...Option) *Store {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cfg := config{
		gcGrace:  time.Hour,
		pageSize: 256 << 10,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 256 << 10
	}

	dataDir := cfg.dir
	initErr := validateRemoteEntryCAS(bucket)
	if initErr == nil && dataDir == "" {
		dataDir, initErr = os.MkdirTemp("", "daramjwee-objectstore-*")
	}

	var cat *internalcatalog.Catalog
	if initErr == nil {
		cat, initErr = openCatalog(filepath.Join(dataDir, "catalog"))
	}
	store := &Store{
		bucket:             bucket,
		logger:             logger,
		dataDir:            dataDir,
		prefix:             trimSlashes(cfg.prefix),
		gcGrace:            cfg.gcGrace,
		packThreshold:      cfg.packThreshold,
		pagedThreshold:     cfg.pagedThreshold,
		pageSize:           cfg.pageSize,
		blockCache:         blockcache.New(cfg.blockCacheBytes),
		pageCache:          blockcache.New(cfg.pageCacheBytes),
		catalog:            cat,
		lockManager:        stripedlock.New(2048),
		instanceID:         newInstanceID(),
		initErr:            initErr,
		segmentRefs:        make(map[string]int),
		reclaimableSegs:    make(map[string]struct{}),
		pendingDurableSegs: make(map[string]struct{}),
		pendingShards:      make(map[string]struct{}),
		flushRun:           newContextSemaphore(),
		remoteState:        newContextSemaphore(),
		autoFlush:          true,
		now:                time.Now,
		scheduleFlushAfter: func(delay time.Duration, run func()) {
			time.AfterFunc(delay, run)
		},
		openSegmentWriter: func(root, shard, segmentID string) (segmentWriter, error) {
			return segment.Open(root, shard, segmentID)
		},
	}
	store.manifestCache = newManifestCache(cfg.manifestCacheBytes, cfg.manifestTTL, func() time.Time {
		return store.now()
	})
	store.checkpointCache = newCheckpointCache(cfg.checkpointCacheBytes, cfg.checkpointTTL, func() time.Time {
		return store.now()
	})
	if cat != nil {
		store.updateCatalog = cat.Update
		store.updateCatalogManyIf = cat.UpdateManyIf
		store.updateCatalogState = cat.UpdateManyIfWithPlans
		store.syncCatalog = cat.Sync
	}
	if store.initErr == nil {
		if err := store.recoverLocalState(); err != nil {
			store.initErr = fmt.Errorf("failed to recover local objectstore state: %w", err)
		} else if err := store.protectLiveUploadPlans(context.Background(), ""); err != nil {
			store.initErr = fmt.Errorf("failed to protect recovering objectstore uploads: %w", err)
		} else if store.autoFlush {
			store.flushMu.Lock()
			if len(store.pendingShards) > 0 {
				store.scheduleFlushLocked()
			}
			store.flushMu.Unlock()
		}
	}
	return store
}

func validateRemoteEntryCAS(bucket objstore.Bucket) error {
	if bucket == nil {
		return errors.New("objectstore: bucket is nil")
	}
	supported := bucket.SupportedObjectUploadOptions()
	if !slices.Contains(supported, objstore.IfNotExists) || !slices.Contains(supported, objstore.IfMatch) {
		return errors.New("objectstore: bucket must support IfNotExists and IfMatch uploads")
	}
	return nil
}

func (s *Store) ValidateTier(index int) error {
	if err := s.ensureReady(); err != nil {
		return fmt.Errorf("objectstore: initialization failed: %w", err)
	}
	return nil
}

// Close gracefully shuts down the objectstore, flushing any pending writes.
// Multiple calls are safe: only the first call performs the shutdown.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeDone = make(chan struct{})
		defer close(s.closeDone)

		s.writersMu.Lock()
		if s.isClosed.Swap(true) {
			s.writersMu.Unlock()
			return
		}
		s.writersMu.Unlock()

		s.flushMu.Lock()
		s.autoFlush = false
		s.flushMu.Unlock()
		s.writers.Wait()

		// Flush any pending writes.
		if err := s.flushPending(context.Background()); err != nil {
			s.closeErr = fmt.Errorf("objectstore: flush on close: %w", err)
			_ = level.Warn(s.logger).Log("msg", "objectstore flush on close failed", "err", err)
		}
	})

	if s.closeDone != nil {
		<-s.closeDone
	}
	return s.closeErr
}

// GetStream returns the current published generation for a key.
func (s *Store) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.ensureReady(); err != nil {
		if errors.Is(err, ErrAmbiguousCommit) {
			return nil, nil, uncertainRead(ctx, err)
		}
		return nil, nil, err
	}

	stream, metadata, ok, localEntry, err := s.openCurrentLocalEntry(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return stream, metadata, nil
	}

	ownedRemote := ownsPublishedRemoteEntry(localEntry)
	var entry *checkpointEntry
	if ownedRemote {
		entry, err = s.loadRemoteEntryDirect(ctx, key, false)
	} else {
		entry, err = s.loadRemoteEntry(ctx, key)
	}
	if err != nil {
		if errors.Is(err, errRemoteEntryTombstone) {
			return nil, nil, uncertainRead(ctx, err)
		}
		if ownedRemote {
			return nil, nil, uncertainRead(ctx, err)
		}
		if !errors.Is(err, daramjwee.ErrNotFound) {
			return nil, nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		m, manifestErr := s.loadManifest(ctx, key)
		if manifestErr != nil {
			return nil, nil, manifestErr
		}
		if m.Layout == layoutPaged {
			pageSize := s.effectivePageSize(m)
			pagedCtx, cancel := context.WithCancel(ctx)
			reader := rangeio.New(m.Size, pageSize, func(pageIndex int64) ([]byte, error) {
				return s.loadPage(pagedCtx, m, pageIndex)
			}, func() error {
				cancel()
				return nil
			})
			return reader, daramjwee.CloneMetadata(&m.Metadata), nil
		}

		reader, manifestErr := s.bucket.Get(ctx, m.BlobPath)
		if manifestErr != nil {
			return nil, nil, uncertainRead(ctx, fmt.Errorf("objectstore: manifest for %q points to unreadable blob %q: %w", key, m.BlobPath, manifestErr))
		}
		return reader, daramjwee.CloneMetadata(&m.Metadata), nil
	}
	reader, err := s.openRemoteEntry(ctx, *entry)
	if err != nil {
		return nil, nil, err
	}
	return reader, daramjwee.CloneMetadata(&entry.Metadata), nil
}

func (s *Store) openCurrentLocalEntry(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, bool, localCatalogEntry, error) {
	const maxLocalOpenAttempts = 3

	for attempts := 0; attempts < maxLocalOpenAttempts; attempts++ {
		entry, ok, err := s.loadLiveLocalEntry(ctx, key)
		if err != nil {
			if errors.Is(err, errMissingLocalEntry) {
				return nil, nil, false, localCatalogEntry{}, daramjwee.ErrReadStateUncertain
			}
			return nil, nil, false, localCatalogEntry{}, err
		}
		if !ok {
			return nil, nil, false, entry, nil
		}

		stream, err := s.openLocalEntry(entry)
		if err == nil {
			return stream, daramjwee.CloneMetadata(&entry.Metadata), true, localCatalogEntry{}, nil
		}
		if !os.IsNotExist(err) {
			return nil, nil, false, localCatalogEntry{}, uncertainRead(ctx, err)
		}
		if attempts < maxLocalOpenAttempts-1 {
			continue
		}

		recheckEntry, recheckOK, repairErr := s.loadLiveLocalEntry(ctx, key)
		if repairErr != nil {
			if errors.Is(repairErr, errMissingLocalEntry) {
				return nil, nil, false, localCatalogEntry{}, daramjwee.ErrReadStateUncertain
			}
			return nil, nil, false, localCatalogEntry{}, repairErr
		}
		if !recheckOK {
			if ownsPublishedRemoteEntry(recheckEntry) {
				return nil, nil, false, recheckEntry, nil
			}
			return nil, nil, false, localCatalogEntry{}, uncertainRead(ctx, os.ErrNotExist)
		}

		recheckStream, recheckErr := s.openLocalEntry(recheckEntry)
		if recheckErr == nil {
			return recheckStream, daramjwee.CloneMetadata(&recheckEntry.Metadata), true, localCatalogEntry{}, nil
		}
		if os.IsNotExist(recheckErr) {
			return nil, nil, false, localCatalogEntry{}, uncertainRead(ctx, recheckErr)
		}
		return nil, nil, false, localCatalogEntry{}, uncertainRead(ctx, recheckErr)
	}

	return nil, nil, false, localCatalogEntry{}, uncertainRead(ctx, os.ErrNotExist)
}

// BeginSet starts a staged write for a new immutable generation.
func (s *Store) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return s.beginSet(ctx, key, metadata, "begin set")
}

func (s *Store) BeginStagedSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.StagedWriteSink, error) {
	return s.beginSet(ctx, key, metadata, "begin staged set")
}

func (s *Store) beginSet(ctx context.Context, key string, metadata *daramjwee.Metadata, operation string) (*writer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("objectstore: %s: %w", operation, err)
	}
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	s.writersMu.Lock()
	if s.isClosed.Load() {
		s.writersMu.Unlock()
		return nil, errors.New("objectstore: store is closed")
	}
	s.writers.Add(1)
	s.writersMu.Unlock()

	generation := s.nextGeneration()
	segmentID := s.nextVersion()
	segmentWriter, err := s.openSegmentWriter(s.dataDir, shardForKey(key), segmentID) //nolint:govet // shadow: intentional variable reuse
	if err != nil {
		s.writers.Done()
		return nil, err
	}

	w := &writer{
		ctx:        ctx,
		store:      s,
		key:        key,
		segment:    segmentWriter,
		generation: generation,
		metadata:   daramjwee.CloneMetadata(metadata),
		doneCh:     make(chan struct{}),
	}

	return w, nil
}

// Delete removes the currently visible entry for a key.
// Blob reclamation is handled by best-effort cleanup and conservative sweep.
func (s *Store) Delete(ctx context.Context, key string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.ensureReady(); err != nil {
		return err
	}
	generation := s.nextGeneration()
	if err := s.flushRun.acquire(ctx); err != nil {
		return err
	}
	applied, err := s.publishDeleteTombstone(key, generation)
	s.flushRun.release()
	if err != nil {
		if applied {
			s.enqueueFlush(key)
		}
		return err
	}
	if !applied {
		return nil
	}
	s.enqueueFlush(key)
	if err := s.flushPending(ctx); err != nil { //nolint:govet // shadow: sequential error handling
		return err
	}
	return nil
}

// Stat returns metadata for the published generation.
func (s *Store) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.ensureReady(); err != nil {
		if errors.Is(err, ErrAmbiguousCommit) {
			return nil, uncertainRead(ctx, err)
		}
		return nil, err
	}

	localEntry, ok, err := s.loadLiveLocalEntry(ctx, key)
	if err != nil {
		if errors.Is(err, errMissingLocalEntry) {
			return nil, daramjwee.ErrReadStateUncertain
		}
		return nil, err
	}
	if ok {
		return daramjwee.CloneMetadata(&localEntry.Metadata), nil
	}

	ownedRemote := ownsPublishedRemoteEntry(localEntry)
	var entry *checkpointEntry
	if ownedRemote {
		entry, err = s.loadRemoteEntryDirect(ctx, key, false)
	} else {
		entry, err = s.loadRemoteEntry(ctx, key)
	}
	if err != nil {
		if errors.Is(err, errRemoteEntryTombstone) {
			return nil, uncertainRead(ctx, err)
		}
		if ownedRemote {
			return nil, uncertainRead(ctx, err)
		}
		if !errors.Is(err, daramjwee.ErrNotFound) {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, manifestErr := s.loadManifest(ctx, key)
		if manifestErr != nil {
			return nil, manifestErr
		}
		return daramjwee.CloneMetadata(&m.Metadata), nil
	}
	return daramjwee.CloneMetadata(&entry.Metadata), nil
}

func ownsPublishedRemoteEntry(entry localCatalogEntry) bool {
	return !entry.Superseded && ((entry.Missing && entry.RemotePublished) || (!entry.Missing && entry.RemotePath != "" && entry.PendingRemotePath == ""))
}

func (s *Store) ensureReady() error {
	if s.initErr != nil {
		return s.initErr
	}
	if s.catalog != nil {
		return s.catalog.Health()
	}
	return nil
}

func (s *Store) loadManifest(ctx context.Context, key string) (*manifest, error) {
	if m, ok := s.manifestCache.Get(key); ok {
		return m, nil
	}

	reader, err := s.bucket.Get(ctx, s.manifestPath(key))
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			_ = level.Warn(s.logger).Log("msg", "failed to close manifest reader", "key", key, "err", closeErr)
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, uncertainRead(ctx, err)
	}

	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, uncertainRead(ctx, fmt.Errorf("objectstore: decode manifest for %q: %w", key, err))
	}
	s.manifestCache.Set(key, &m)
	return &m, nil
}

func (s *Store) publishManifest(ctx context.Context, key, blobPath string, size int64, metadata *daramjwee.Metadata) error {
	layout := layoutWhole
	pageSize := int64(0)
	if s.pagedThreshold > 0 && size > s.pagedThreshold {
		layout = layoutPaged
		pageSize = s.pageSize
	}

	m := manifest{
		Version:  path.Base(strings.TrimSuffix(blobPath, ".data")),
		Layout:   layout,
		BlobPath: blobPath,
		Size:     size,
		PageSize: pageSize,
	}
	if metadata != nil {
		m.Metadata = *metadata
	}

	manifestBytes, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	return s.bucket.Upload(ctx, s.manifestPath(key), bytes.NewReader(manifestBytes))
}

func (s *Store) loadPage(ctx context.Context, m *manifest, pageIndex int64) ([]byte, error) {
	key := blockcache.Key{ID: m.Version, Index: pageIndex}
	if page, ok := s.pageCache.Get(key); ok {
		return page, nil
	}

	pageSize := s.effectivePageSize(m)
	start := pageIndex * pageSize
	length := pageSize
	if remaining := m.Size - start; remaining < length {
		length = remaining
	}
	reader, err := s.bucket.GetRange(ctx, m.BlobPath, start, length)
	if err != nil {
		return nil, uncertainRead(ctx, err)
	}
	defer reader.Close()
	page, err := io.ReadAll(reader)
	if err != nil {
		return nil, uncertainRead(ctx, err)
	}
	s.pageCache.Set(key, page)
	return page, nil
}

func (s *Store) effectivePageSize(m *manifest) int64 {
	if m != nil && m.PageSize > 0 {
		return m.PageSize
	}
	return s.pageSize
}

func (s *Store) manifestPath(key string) string {
	return joinPath(s.prefix, "manifests", shardForKey(key), encodeKey(key)+".json")
}

func (s *Store) remoteEntryPath(key string) string {
	return joinPath(s.prefix, "entries", shardForKey(key), encodeKey(key)+".json")
}

func (s *Store) uploadIntentPath(remotePath string) string {
	return joinPath(s.prefix, "uploads", encodeKey(remotePath)+".json")
}

func (s *Store) gcReceiptPath(remotePath string) string {
	return joinPath(s.prefix, "gc-receipts", encodeKey(remotePath)+".json")
}

func (s *Store) publishUploadIntent(ctx context.Context, remotePath string) error {
	intent := uploadIntent{Version: 2, RemotePath: remotePath}
	if s.catalog != nil {
		if plan, ok := s.catalog.UploadPlans()[remotePath]; ok {
			intent.Size = plan.Size
			intent.SizeKnown = plan.SizeKnown
			intent.Members = plan.Members
		}
	}
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return s.bucket.Upload(ctx, s.uploadIntentPath(remotePath), bytes.NewReader(data))
}

func (s *Store) blobDir(key string) string {
	return joinPath(s.prefix, "blobs", shardForKey(key), encodeKey(key))
}

func (s *Store) blobRoot() string {
	return ensureDir(joinPath(s.prefix, "blobs"))
}

func (s *Store) blobPath(key, version string) string {
	return joinPath(s.blobDir(key), version+".data")
}

func (s *Store) nextVersion() string {
	return fmt.Sprintf("%020d-%s-%06d", s.now().UnixNano(), s.instanceID, s.versionSeq.Add(1))
}

var instanceSeq atomic.Uint64

func newInstanceID() string {
	var id [8]byte
	if _, err := rand.Read(id[:]); err == nil {
		return hex.EncodeToString(id[:])
	}
	return fmt.Sprintf("%x-%x-%x", time.Now().UnixNano(), os.Getpid(), instanceSeq.Add(1))
}

func encodeKey(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeKey(encoded string) (string, error) {
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(key), err
}

func shardForKey(key string) string {
	return internalshard.ForKey(key)
}

func joinPath(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = trimSlashes(part)
		if part != "" {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	return path.Join(filtered...)
}

func ensureDir(prefix string) string {
	prefix = trimSlashes(prefix)
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func trimSlashes(s string) string {
	return strings.Trim(s, "/")
}

// OwnsObjectPath reports whether the provided bucket object path belongs to the
// internal remote namespace used by this store instance.
func (s *Store) OwnsObjectPath(name string) bool {
	name = trimSlashes(name)
	if name == "" {
		return false
	}

	prefix := trimSlashes(s.prefix)
	if prefix != "" {
		if name == prefix {
			return false
		}
		if !strings.HasPrefix(name, prefix+"/") {
			return false
		}
		name = strings.TrimPrefix(name, prefix+"/")
	}

	for _, root := range []string{"manifests", "entries", "uploads", "blobs", "segments", "checkpoints"} {
		if name == root || strings.HasPrefix(name, root+"/") {
			return true
		}
	}
	return false
}

type fileSectionReadCloser struct {
	io.Reader
	closeFn func() error
	once    sync.Once
	err     error
}

func (r *fileSectionReadCloser) Close() error {
	r.once.Do(func() {
		r.err = r.closeFn()
	})
	return r.err
}

func objectTimestampFromPath(objectPath string) (time.Time, bool) {
	version := strings.TrimSuffix(path.Base(objectPath), path.Ext(objectPath))
	sep := strings.IndexByte(version, '-')
	if sep <= 0 {
		return time.Time{}, false
	}
	nanos, err := strconv.ParseInt(version[:sep], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
}

func (s *Store) nextGeneration() uint64 {
	return s.generationSeq.Add(1)
}

func (s *Store) observeGeneration(generation uint64) {
	if generation == 0 {
		return
	}
	for {
		current := s.generationSeq.Load()
		if current >= generation || s.generationSeq.CompareAndSwap(current, generation) {
			return
		}
	}
}

func ignoreNotFound(err error, bucket objstore.Bucket) error {
	if err == nil || bucket.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}
