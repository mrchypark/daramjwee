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
	bucket         objstore.Bucket
	logger         log.Logger
	dataDir        string
	prefix         string
	defaultGCGrace time.Duration
	wholeThreshold int64
	pageSize       int64
	pageCache      *pagecache.Cache
	catalog        *internalcatalog.Catalog
	lockManager    *keyLockManager
	pageLoads      singleflight.Group
	versionSeq     atomic.Uint64
	initErr        error
	flushMu        sync.Mutex
	flushRunMu     sync.Mutex
	pendingShards  map[string]struct{}
	flushTimer     *time.Timer
	autoFlush      bool
	now            func() time.Time
}

var _ daramjwee.Store = (*Store)(nil)

// New creates a new object storage backend.
func New(bucket objstore.Bucket, logger log.Logger, opts ...Option) *Store {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cfg := config{
		defaultGCGrace: time.Hour,
		pageSize:       256 << 10,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 256 << 10
	}

	dataDir := cfg.dataDir
	var initErr error
	if dataDir == "" {
		dataDir, initErr = os.MkdirTemp("", "daramjwee-objectstore-*")
	}

	var cat *internalcatalog.Catalog
	if initErr == nil {
		cat, initErr = internalcatalog.Open(filepath.Join(dataDir, "catalog"))
	}
	store := &Store{
		bucket:         bucket,
		logger:         logger,
		dataDir:        dataDir,
		prefix:         trimSlashes(cfg.prefix),
		defaultGCGrace: cfg.defaultGCGrace,
		wholeThreshold: cfg.wholeThreshold,
		pageSize:       cfg.pageSize,
		pageCache:      pagecache.New(cfg.memoryPageCacheBytes),
		catalog:        cat,
		lockManager:    newKeyLockManager(2048),
		initErr:        initErr,
		pendingShards:  make(map[string]struct{}),
		autoFlush:      true,
		now:            time.Now,
	}
	if store.initErr == nil {
		if err := store.sweepOrphanedLocalSegments(); err != nil {
			level.Warn(store.logger).Log("msg", "failed to sweep orphaned local segments", "dir", store.dataDir, "err", err)
		}
	}
	return store
}

// GetStream returns the current published generation for a key.
func (s *Store) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if err := s.ensureReady(); err != nil {
		return nil, nil, err
	}

	if entry, ok, err := s.loadLiveLocalEntry(key); err != nil {
		if errors.Is(err, errMissingLocalEntry) {
			return nil, nil, daramjwee.ErrNotFound
		}
		return nil, nil, err
	} else if ok {
		stream, err := openLocalEntry(entry)
		if err != nil {
			return nil, nil, err
		}
		return stream, cloneMetadata(&entry.Metadata), nil
	}

	m, err := s.loadManifest(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	if m.Layout == layoutPaged {
		pageSize := m.PageSize
		if pageSize <= 0 {
			pageSize = s.pageSize
		}
		pagedCtx, cancel := context.WithCancel(ctx)
		reader := rangeio.New(m.Size, pageSize, func(pageIndex int64) ([]byte, error) {
			return s.loadPage(pagedCtx, m, pageIndex)
		}, func() error {
			cancel()
			return nil
		})
		return reader, cloneMetadata(&m.Metadata), nil
	}

	reader, err := s.bucket.Get(ctx, m.BlobPath)
	if err != nil {
		if s.bucket.IsObjNotFoundErr(err) {
			return nil, nil, fmt.Errorf("objectstore: manifest for %q points to missing blob %q: %w", key, m.BlobPath, err)
		}
		return nil, nil, err
	}
	return reader, cloneMetadata(&m.Metadata), nil
}

// BeginSet starts a staged write for a new immutable generation.
func (s *Store) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	s.lockManager.Lock(key)

	segmentID := s.nextVersion()
	segmentWriter, err := segment.Open(s.dataDir, shardForKey(key), segmentID)
	if err != nil {
		s.lockManager.Unlock(key)
		return nil, err
	}

	w := &writer{
		ctx:      ctx,
		store:    s,
		key:      key,
		segment:  segmentWriter,
		metadata: cloneMetadata(metadata),
	}

	return w, nil
}

// Delete removes the currently visible entry for a key.
// Blob reclamation is handled by best-effort cleanup and conservative sweep.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	s.lockManager.Lock(key)
	defer s.lockManager.Unlock(key)

	prev, _, err := s.loadLocalEntry(key)
	if err != nil {
		return err
	}
	if err := s.deleteLocalEntry(key); err != nil {
		return err
	}
	if prev.RemotePath != "" || prev.Missing {
		s.enqueueFlush(key)
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

	m, err := s.loadManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	return cloneMetadata(&m.Metadata), nil
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
	if s.wholeThreshold > 0 && size > s.wholeThreshold {
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
		start := pageIndex * m.PageSize
		length := m.PageSize
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

func cloneMetadata(meta *daramjwee.Metadata) *daramjwee.Metadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}

type fileSectionReadCloser struct {
	io.Reader
	file *os.File
}

func (r *fileSectionReadCloser) Close() error {
	return r.file.Close()
}

func openLocalEntry(entry localCatalogEntry) (io.ReadCloser, error) {
	file, err := os.Open(entry.SegmentPath)
	if err != nil {
		return nil, err
	}
	section := io.NewSectionReader(file, entry.Offset, entry.Length)
	return &fileSectionReadCloser{Reader: section, file: file}, nil
}

func blobTimestampFromPath(blobPath string) (time.Time, bool) {
	version := strings.TrimSuffix(path.Base(blobPath), ".data")
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

func ignoreNotFound(err error, bucket objstore.Bucket) error {
	if err == nil || bucket.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}
