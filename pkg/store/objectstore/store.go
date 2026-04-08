package objectstore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/blockcache"
	internalcatalog "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/catalog"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/pagecache"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/rangeio"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/segment"
	internalshard "github.com/mrchypark/daramjwee/pkg/store/objectstore/internal/shard"
	"github.com/thanos-io/objstore"
	"github.com/zeebo/xxh3"
	"golang.org/x/sync/singleflight"
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

// Store is a first-party object storage backend.
// It currently publishes immutable blob versions via internal manifest pointers.
type Store struct {
	bucket            objstore.Bucket
	logger            log.Logger
	dataDir           string
	prefix            string
	gcGrace           time.Duration
	packThreshold     int64
	pagedThreshold    int64
	pageSize          int64
	blockCache        *blockcache.Cache
	pageCache         *pagecache.Cache
	checkpointCache   *checkpointCache
	catalog           *internalcatalog.Catalog
	lockManager       *keyLockManager
	blockLoads        singleflight.Group
	pageLoads         singleflight.Group
	versionSeq        atomic.Uint64
	generationSeq     atomic.Uint64
	initErr           error
	segmentRefsMu     sync.Mutex
	segmentRefs       map[string]int
	reclaimableSegs   map[string]struct{}
	flushMu           sync.Mutex
	flushRunMu        sync.Mutex
	pendingShards     map[string]struct{}
	flushTimer        *time.Timer
	autoFlush         bool
	now               func() time.Time
	openSegmentWriter func(root, shard, segmentID string) (segmentWriter, error)
}

func (s *Store) GetStreamUsesContext() bool { return true }

func (s *Store) BeginSetUsesContext() bool { return true }

var _ daramjwee.Store = (*Store)(nil)

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
	var initErr error
	if dataDir == "" {
		dataDir, initErr = os.MkdirTemp("", "daramjwee-objectstore-*")
	}

	var cat *internalcatalog.Catalog
	if initErr == nil {
		cat, initErr = internalcatalog.Open(filepath.Join(dataDir, "catalog"))
	}
	store := &Store{
		bucket:          bucket,
		logger:          logger,
		dataDir:         dataDir,
		prefix:          trimSlashes(cfg.prefix),
		gcGrace:         cfg.gcGrace,
		packThreshold:   cfg.packThreshold,
		pagedThreshold:  cfg.pagedThreshold,
		pageSize:        cfg.pageSize,
		blockCache:      blockcache.New(cfg.blockCacheBytes),
		pageCache:       pagecache.New(cfg.pageCacheBytes),
		catalog:         cat,
		lockManager:     newKeyLockManager(2048),
		initErr:         initErr,
		segmentRefs:     make(map[string]int),
		reclaimableSegs: make(map[string]struct{}),
		pendingShards:   make(map[string]struct{}),
		autoFlush:       true,
		now:             time.Now,
		openSegmentWriter: func(root, shard, segmentID string) (segmentWriter, error) {
			return segment.Open(root, shard, segmentID)
		},
	}
	store.checkpointCache = newCheckpointCache(cfg.checkpointCacheBytes, cfg.checkpointTTL, func() time.Time {
		return store.now()
	})
	if store.initErr == nil {
		if err := store.recoverLocalState(); err != nil {
			store.initErr = fmt.Errorf("failed to recover local objectstore state: %w", err)
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

// GetStream returns the current published generation for a key.
func (s *Store) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if err := s.ensureReady(); err != nil {
		return nil, nil, err
	}

	if stream, metadata, ok, err := s.openCurrentLocalEntry(key); err != nil {
		return nil, nil, err
	} else if ok {
		return stream, metadata, nil
	}

	entry, err := s.loadRemoteEntry(ctx, key)
	if err != nil {
		if !errors.Is(err, daramjwee.ErrNotFound) {
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
			return reader, cloneMetadata(&m.Metadata), nil
		}

		reader, manifestErr := s.bucket.Get(ctx, m.BlobPath)
		if manifestErr != nil {
			if s.bucket.IsObjNotFoundErr(manifestErr) {
				return nil, nil, fmt.Errorf("objectstore: manifest for %q points to missing blob %q: %w", key, m.BlobPath, manifestErr)
			}
			return nil, nil, manifestErr
		}
		return reader, cloneMetadata(&m.Metadata), nil
	}
	reader, err := s.openRemoteEntry(ctx, *entry)
	if err != nil {
		return nil, nil, err
	}
	return reader, cloneMetadata(&entry.Metadata), nil
}

func (s *Store) openCurrentLocalEntry(key string) (io.ReadCloser, *daramjwee.Metadata, bool, error) {
	const maxLocalOpenAttempts = 3

	for attempts := 0; attempts < maxLocalOpenAttempts; attempts++ {
		entry, ok, err := s.loadLiveLocalEntry(key)
		if err != nil {
			if errors.Is(err, errMissingLocalEntry) {
				return nil, nil, false, daramjwee.ErrNotFound
			}
			return nil, nil, false, err
		}
		if !ok {
			return nil, nil, false, nil
		}

		stream, err := s.openLocalEntry(entry)
		if err == nil {
			return stream, cloneMetadata(&entry.Metadata), true, nil
		}
		if !os.IsNotExist(err) {
			return nil, nil, false, err
		}
		if attempts < maxLocalOpenAttempts-1 {
			continue
		}

		recheckEntry, recheckOK, repairErr := s.loadLiveLocalEntry(key)
		if repairErr != nil {
			if errors.Is(repairErr, errMissingLocalEntry) {
				return nil, nil, false, daramjwee.ErrNotFound
			}
			return nil, nil, false, repairErr
		}
		if !recheckOK {
			return nil, nil, false, nil
		}

		recheckStream, recheckErr := s.openLocalEntry(recheckEntry)
		if recheckErr == nil {
			return recheckStream, cloneMetadata(&recheckEntry.Metadata), true, nil
		}
		if os.IsNotExist(recheckErr) {
			return nil, nil, false, nil
		}
		return nil, nil, false, recheckErr
	}

	return nil, nil, false, nil
}

// BeginSet starts a staged write for a new immutable generation.
func (s *Store) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	generation := s.nextGeneration()
	segmentID := s.nextVersion()
	segmentWriter, err := s.openSegmentWriter(s.dataDir, shardForKey(key), segmentID)
	if err != nil {
		return nil, err
	}

	w := &writer{
		ctx:        ctx,
		store:      s,
		key:        key,
		segment:    segmentWriter,
		generation: generation,
		metadata:   cloneMetadata(metadata),
	}

	return w, nil
}

// Delete removes the currently visible entry for a key.
// Blob reclamation is handled by best-effort cleanup and conservative sweep.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	generation := s.nextGeneration()
	applied, err := s.publishDeleteTombstone(key, generation)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	s.enqueueFlush(key)
	if err := s.flushPending(ctx); err != nil {
		return err
	}

	err = s.bucket.Delete(ctx, s.manifestPath(key))
	if err == nil || s.bucket.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}

// Stat returns metadata for the published generation.
func (s *Store) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}

	if entry, ok, err := s.loadLiveLocalEntry(key); err != nil {
		if errors.Is(err, errMissingLocalEntry) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	} else if ok {
		return cloneMetadata(&entry.Metadata), nil
	}

	entry, err := s.loadRemoteEntry(ctx, key)
	if err != nil {
		if !errors.Is(err, daramjwee.ErrNotFound) {
			return nil, err
		}
		m, manifestErr := s.loadManifest(ctx, key)
		if manifestErr != nil {
			return nil, manifestErr
		}
		return cloneMetadata(&m.Metadata), nil
	}
	return cloneMetadata(&entry.Metadata), nil
}

func (s *Store) ensureReady() error {
	return s.initErr
}

func (s *Store) loadManifest(ctx context.Context, key string) (*manifest, error) {
	reader, err := s.bucket.Get(ctx, s.manifestPath(key))
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			level.Warn(s.logger).Log("msg", "failed to close manifest reader", "key", key, "err", closeErr)
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("objectstore: decode manifest for %q: %w", key, err)
	}
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

	bytes, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	return s.bucket.Upload(ctx, s.manifestPath(key), strings.NewReader(string(bytes)))
}

func (s *Store) loadPage(ctx context.Context, m *manifest, pageIndex int64) ([]byte, error) {
	key := pagecache.Key{Version: m.Version, Index: pageIndex}
	if page, ok := s.pageCache.Get(key); ok {
		return page, nil
	}

	value, err, _ := s.pageLoads.Do(key.String(), func() (any, error) {
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
			return nil, err
		}
		defer reader.Close()

		page, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		s.pageCache.Set(key, page)
		return page, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
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

func (s *Store) manifestRoot() string {
	return ensureDir(joinPath(s.prefix, "manifests"))
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
	return fmt.Sprintf("%020d-%06d", s.now().UnixNano(), s.versionSeq.Add(1))
}

func encodeKey(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
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

	for _, root := range []string{"manifests", "blobs", "segments", "checkpoints"} {
		if name == root || strings.HasPrefix(name, root+"/") {
			return true
		}
	}
	return false
}

func cloneMetadata(meta *daramjwee.Metadata) *daramjwee.Metadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
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

func blobTimestampFromPath(blobPath string) (time.Time, bool) {
	return objectTimestampFromPath(blobPath)
}

type keyLockManager struct {
	locks []sync.Mutex
	slots uint64
}

func newKeyLockManager(slots int) *keyLockManager {
	if slots <= 0 {
		slots = 2048
	}
	return &keyLockManager{
		locks: make([]sync.Mutex, slots),
		slots: uint64(slots),
	}
}

func (m *keyLockManager) Lock(key string) {
	m.locks[xxh3.HashString(key)%m.slots].Lock()
}

func (m *keyLockManager) Unlock(key string) {
	m.locks[xxh3.HashString(key)%m.slots].Unlock()
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
