package filestore

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
)

// FileStore is a disk-based implementation of the daramjwee.Store.
type FileStore struct {
	baseDir      string
	logger       log.Logger
	lockManager  *FileLockManager
	useCopyWrite bool // useCopyWrite, if true, uses a copy-based write strategy instead of atomic rename.

	generationMu  sync.Mutex
	generations   map[string]uint64
	activeWriters map[string]int
	generationSeq atomic.Uint64

	// Policy-related fields
	mu          sync.RWMutex
	capacity    int64 // Capacity in bytes (0 means unlimited)
	currentSize int64 // Current total size of stored files in bytes
	policy      daramjwee.EvictionPolicy
	fileSizes   map[string]int64 // Track file sizes for eviction
}

var _ daramjwee.TierValidator = (*FileStore)(nil)

const encodedKeyPrefix = "b64_"
const encodedKeyDir = "__encoded__"

type dataPathCandidate struct {
	path      string
	isLegacy  bool
	ambiguous bool
}

type storedMetadata struct {
	daramjwee.Metadata
	StoredKey *string `json:"__stored_key,omitempty"`
}

func (m storedMetadata) MarshalJSON() ([]byte, error) {
	type payload struct {
		CacheTag   string    `json:"CacheTag,omitempty"`
		IsNegative bool      `json:"IsNegative"`
		CachedAt   time.Time `json:"CachedAt"`
		StoredKey  *string   `json:"__stored_key,omitempty"`
	}
	return json.Marshal(payload{
		CacheTag:   m.Metadata.CacheTag,
		IsNegative: m.Metadata.IsNegative,
		CachedAt:   m.Metadata.CachedAt,
		StoredKey:  m.StoredKey,
	})
}

func (m *storedMetadata) UnmarshalJSON(data []byte) error {
	type payload struct {
		CacheTag   string    `json:"CacheTag,omitempty"`
		LegacyETag string    `json:"ETag,omitempty"`
		IsNegative bool      `json:"IsNegative"`
		CachedAt   time.Time `json:"CachedAt"`
		StoredKey  *string   `json:"__stored_key,omitempty"`
	}
	var p payload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	m.Metadata = daramjwee.Metadata{
		CacheTag:   p.CacheTag,
		IsNegative: p.IsNegative,
		CachedAt:   p.CachedAt,
	}
	if m.Metadata.CacheTag == "" {
		m.Metadata.CacheTag = p.LegacyETag
	}
	m.StoredKey = p.StoredKey
	return nil
}

// Option configures the FileStore.
type Option func(*FileStore)

// WithCopyWrite sets the store to use a copy-and-truncate strategy
// instead of an atomic rename. This can be necessary for compatibility with
// some network filesystems like NFS, but it is non-atomic and may leave
// partial or orphaned files behind if a write fails.
func WithCopyWrite() Option {
	return func(fs *FileStore) {
		fs.useCopyWrite = true
	}
}

// WithCapacity sets the maximum capacity of the store in bytes.
// When capacity is exceeded, the eviction policy will be used to remove files.
// If capacity is 0 or less, the store has no limit.
func WithCapacity(capacity int64) Option {
	return func(fs *FileStore) {
		fs.capacity = capacity
	}
}

// WithEviction sets the eviction policy for the store.
// If policy is nil, a no-op policy is used (no eviction).
func WithEviction(policy daramjwee.EvictionPolicy) Option {
	return func(fs *FileStore) {
		fs.policy = policy
	}
}

// New creates a new FileStore.
// It ensures the base directory exists and initializes the file locking mechanism.
func New(dir string, logger log.Logger, opts ...Option) (*FileStore, error) {
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory %s: %w", dir, err)
	}
	fs := &FileStore{
		baseDir:       dir,
		logger:        logger,
		lockManager:   NewFileLockManager(2048),
		generations:   make(map[string]uint64),
		activeWriters: make(map[string]int),
		fileSizes:     make(map[string]int64),
	}
	if now := time.Now().UnixNano(); now > 0 {
		fs.generationSeq.Store(uint64(now))
	}

	// Apply options
	for _, opt := range opts {
		opt(fs)
	}

	// Set default policy if none provided
	if fs.policy == nil {
		fs.policy = daramjwee.NewNullEvictionPolicy()
	}

	// Initialize current size by scanning existing files
	if err := fs.initializeCurrentSize(); err != nil {
		level.Warn(logger).Log("msg", "failed to initialize current size", "err", err)
	}

	return fs, nil
}

// ValidateTier rejects tier positions that would weaken the stream-through
// publish contract expected from the ordered tier chain.
func (fs *FileStore) ValidateTier(index int) error {
	if index == 0 && fs.useCopyWrite {
		return errors.New("filestore: WithCopyWrite mode does not support stream-through publish semantics and cannot be used as tier 0")
	}
	return nil
}

// GetStream reads an object from the store.
// It returns an io.ReadCloser, the object's metadata, and an error if any.
func (fs *FileStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	for _, candidate := range fs.dataPathCandidates(key) {
		fs.lockManager.RLock(candidate.path)

		file, err := os.Open(candidate.path)
		if err != nil {
			fs.lockManager.RUnlock(candidate.path)
			if os.IsNotExist(err) {
				continue
			}
			return nil, nil, err
		}

		meta, storedKey, storedKeyPresent, dataOffset, err := readStoredMetadata(file)
		if err != nil {
			file.Close()
			fs.lockManager.RUnlock(candidate.path)
			return nil, nil, err
		}
		if !fs.candidateMatchesKey(candidate, key, storedKey, storedKeyPresent) {
			file.Close()
			fs.lockManager.RUnlock(candidate.path)
			continue
		}

		if _, err := file.Seek(dataOffset, io.SeekStart); err != nil {
			file.Close()
			fs.lockManager.RUnlock(candidate.path)
			return nil, nil, fmt.Errorf("failed to seek to data section: %w", err)
		}

		// Notify the policy that this key was accessed
		fs.mu.Lock()
		fs.policy.Touch(key)
		fs.mu.Unlock()

		return newLockedReadCloser(file, func() { fs.lockManager.RUnlock(candidate.path) }), meta, nil
	}
	return nil, nil, daramjwee.ErrNotFound
}

// BeginSet returns a sink that streams data to the store.
// The data is written to a temporary file and then atomically moved to the final location
// upon closing the writer, or copied if WithCopyWrite option is used.
func (fs *FileStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	path := fs.toDataPath(key)
	fs.beginGenerationTracking(key)
	generation := fs.nextGeneration()
	trackingActive := true
	defer func() {
		if trackingActive {
			fs.finishGenerationTracking(key)
		}
	}()

	// Ensure the directory exists for the target path
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for key %s: %w", key, err)
	}

	tmpFile, err := os.CreateTemp(fs.baseDir, "daramjwee-tmp-*.data")
	if err != nil {
		return nil, err
	}

	// Write metadata to the temporary file first.
	if err := writeStoredMetadata(tmpFile, key, metadata); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, err
	}

	cleanupTemp := func() error {
		if err := os.Remove(tmpFile.Name()); err != nil && !os.IsNotExist(err) {
			level.Warn(fs.logger).Log("msg", "failed to remove temporary file", "file", tmpFile.Name(), "err", err)
			return err
		}
		return nil
	}

	abortCleanup := func() error {
		defer fs.finishGenerationTracking(key)
		return cleanupTemp()
	}

	onClose := func() (err error) {
		defer fs.finishGenerationTracking(key)
		defer func() {
			if cleanupErr := cleanupTemp(); cleanupErr != nil && err == nil {
				err = cleanupErr
			}
		}()

		locked := fs.lockPaths([]string{path}, nil)
		defer func() {
			fs.unlockPaths(locked)
		}()

		if current := fs.generationFloor(key); current > generation {
			return nil
		}

		if fs.useCopyWrite {
			// Non-atomic copy strategy for NFS compatibility.
			if err := copyFile(tmpFile.Name(), path); err != nil {
				return fmt.Errorf("failed to copy temp file to final path: %w", err)
			}
		} else {
			// Atomic rename strategy.
			if err := os.Rename(tmpFile.Name(), path); err != nil {
				return fmt.Errorf("failed to rename temporary file: %w", err)
			}
		}

		// Update policy and size tracking after successful write
		if err := fs.updateAfterSet(key, path); err != nil {
			level.Warn(fs.logger).Log("msg", "failed to update policy after set", "key", key, "err", err)
		}
		fs.setGenerationFloor(key, generation)

		// Release the encoded-path publish lock before best-effort legacy cleanup.
		fs.unlockPaths(locked)
		locked = nil
		if err := fs.removeLegacyPathOnly(key, nil); err != nil {
			level.Warn(fs.logger).Log("msg", "failed to remove legacy path after set", "key", key, "err", err)
		}

		return nil
	}

	trackingActive = false
	return newLockedWriteCloser(tmpFile, onClose, abortCleanup), nil
}

// Delete removes an object from the store.
func (fs *FileStore) Delete(ctx context.Context, key string) error {
	candidates := fs.dataPathCandidates(key)
	paths := candidatePaths(candidates)
	locked := fs.lockPaths(paths, nil)
	defer fs.unlockPaths(locked)
	generation := fs.nextGeneration()

	removablePaths, err := fs.deletablePaths(candidates, key)
	if err != nil {
		return err
	}
	if err := removeFiles(removablePaths); err != nil {
		return err
	}

	fs.mu.Lock()
	if fileSize, exists := fs.fileSizes[key]; exists {
		fs.currentSize -= fileSize
		delete(fs.fileSizes, key)
	}
	fs.policy.Remove(key)
	fs.mu.Unlock()
	fs.setGenerationFloor(key, generation)
	fs.pruneGenerationIfIdle(key)

	return nil
}

// Stat reads the metadata of an object without reading the data.
func (fs *FileStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	for _, candidate := range fs.dataPathCandidates(key) {
		fs.lockManager.RLock(candidate.path)

		file, err := os.Open(candidate.path)
		if err != nil {
			fs.lockManager.RUnlock(candidate.path)
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		meta, storedKey, storedKeyPresent, _, err := readStoredMetadata(file)
		closeErr := file.Close()
		fs.lockManager.RUnlock(candidate.path)
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if !fs.candidateMatchesKey(candidate, key, storedKey, storedKeyPresent) {
			continue
		}

		// Access via Stat should also be considered a "touch"
		fs.mu.Lock()
		fs.policy.Touch(key)
		fs.mu.Unlock()

		return meta, nil
	}
	return nil, daramjwee.ErrNotFound
}

// toDataPath converts a key into a safe file path within the base directory.
// It prevents path traversal by cleaning the path and ensuring it stays within baseDir.
func (fs *FileStore) toDataPath(key string) string {
	return filepath.Join(fs.baseDir, encodedKeyDir, encodeKey(key))
}

func (fs *FileStore) legacyDataPath(key string) string {
	safeFallback := func(key string) string {
		safeKey := strings.ReplaceAll(key, "..", "")
		safeKey = strings.ReplaceAll(safeKey, string(os.PathSeparator), "_")
		safeKey = strings.ReplaceAll(safeKey, "/", "_")
		if safeKey == "" {
			safeKey = "safe_fallback"
		}
		return filepath.Join(fs.baseDir, safeKey)
	}

	if key == "" {
		return filepath.Join(fs.baseDir, "empty_key")
	}

	slashedKey := filepath.ToSlash(key)
	cleanKey := filepath.Clean("/" + slashedKey)
	cleanKey = strings.TrimPrefix(cleanKey, "/")

	if cleanKey == "" || cleanKey == "." {
		cleanKey = "root_file"
	}

	fullPath := filepath.Join(fs.baseDir, cleanKey)

	absBase, err := filepath.Abs(fs.baseDir)
	if err != nil {
		return safeFallback(key)
	}

	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		return safeFallback(key)
	}

	if !strings.HasPrefix(absPath+string(os.PathSeparator), absBase+string(os.PathSeparator)) && absPath != absBase {
		return safeFallback(key)
	}

	return fullPath
}

func (fs *FileStore) dataPathCandidates(key string) []dataPathCandidate {
	encodedPath := fs.toDataPath(key)
	legacyPath := fs.legacyDataPath(key)
	if legacyPath == encodedPath {
		return []dataPathCandidate{{path: encodedPath, ambiguous: fs.isAmbiguousLegacyPath(encodedPath)}}
	}
	return []dataPathCandidate{
		{path: encodedPath, ambiguous: fs.isAmbiguousLegacyPath(encodedPath)},
		{path: legacyPath, isLegacy: true, ambiguous: fs.isAmbiguousLegacyPath(legacyPath)},
	}
}

func (fs *FileStore) isAmbiguousLegacyPath(path string) bool {
	return filepath.Dir(path) == fs.baseDir && strings.HasPrefix(filepath.Base(path), encodedKeyPrefix)
}

func candidatePaths(candidates []dataPathCandidate) []string {
	paths := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		paths = append(paths, candidate.path)
	}
	return paths
}

func (fs *FileStore) candidateMatchesKey(candidate dataPathCandidate, key, storedKey string, storedKeyPresent bool) bool {
	if !candidate.ambiguous {
		return true
	}
	if !storedKeyPresent {
		return candidate.isLegacy
	}
	return storedKey == key
}

func (fs *FileStore) deletablePaths(candidates []dataPathCandidate, key string) ([]string, error) {
	removable := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.ambiguous {
			removable = append(removable, candidate.path)
			continue
		}

		storedKey, storedKeyPresent, err := fs.readStoredKeyForCandidate(candidate.path)
		if err != nil {
			return nil, err
		}
		if fs.candidateMatchesKey(candidate, key, storedKey, storedKeyPresent) {
			removable = append(removable, candidate.path)
		}
	}
	return removable, nil
}

func (fs *FileStore) readStoredKeyForCandidate(path string) (string, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	defer file.Close()

	_, storedKey, storedKeyPresent, _, err := readStoredMetadata(file)
	if err != nil {
		return "", false, err
	}
	return storedKey, storedKeyPresent, nil
}

func (fs *FileStore) lockPaths(paths []string, heldSlots map[uint64]struct{}) []string {
	type slotPath struct {
		slot uint64
		path string
	}

	bySlot := make(map[uint64]string, len(paths))
	for _, path := range paths {
		slot := fs.lockManager.getSlot(path)
		if _, skip := heldSlots[slot]; skip {
			continue
		}
		if existing, ok := bySlot[slot]; !ok || path < existing {
			bySlot[slot] = path
		}
	}

	ordered := make([]slotPath, 0, len(bySlot))
	for slot, path := range bySlot {
		ordered = append(ordered, slotPath{slot: slot, path: path})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].slot == ordered[j].slot {
			return ordered[i].path < ordered[j].path
		}
		return ordered[i].slot < ordered[j].slot
	})

	locked := make([]string, 0, len(ordered))
	for _, candidate := range ordered {
		fs.lockManager.Lock(candidate.path)
		locked = append(locked, candidate.path)
	}
	return locked
}

func (fs *FileStore) unlockPaths(paths []string) {
	for i := len(paths) - 1; i >= 0; i-- {
		fs.lockManager.Unlock(paths[i])
	}
}

func removeFiles(paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// writeMetadata serializes metadata, prefixes it with its length, and writes it to the provided writer.
func writeMetadata(w io.Writer, meta *daramjwee.Metadata) error {
	return writeStoredMetadataEnvelope(w, storedMetadata{Metadata: derefMetadata(meta)})
}

func writeStoredMetadata(w io.Writer, key string, meta *daramjwee.Metadata) error {
	storedKey := key
	return writeStoredMetadataEnvelope(w, storedMetadata{
		Metadata:  derefMetadata(meta),
		StoredKey: &storedKey,
	})
}

func writeStoredMetadataEnvelope(w io.Writer, meta storedMetadata) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(metaBytes)))

	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write metadata length: %w", err)
	}
	if _, err := w.Write(metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	return nil
}

func derefMetadata(meta *daramjwee.Metadata) daramjwee.Metadata {
	if meta == nil {
		return daramjwee.Metadata{}
	}
	return *meta
}

// readMetadata reads metadata from a reader.
// It expects the metadata length as a uint32 prefix, followed by the JSON-encoded metadata.
// It returns the metadata, the offset where the data begins, and an error if any.
func readMetadata(r io.Reader) (*daramjwee.Metadata, int64, error) {
	meta, _, _, dataOffset, err := readStoredMetadata(r)
	return meta, dataOffset, err
}

func readStoredMetadata(r io.Reader) (*daramjwee.Metadata, string, bool, int64, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, "", false, 0, daramjwee.ErrNotFound // Treat empty/short files as not found
		}
		return nil, "", false, 0, fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaLen := binary.BigEndian.Uint32(lenBuf)
	// Add a sanity check for metaLen to avoid allocating huge buffers
	if metaLen > 10*1024*1024 { // 10MB limit for metadata
		return nil, "", false, 0, fmt.Errorf("metadata size is too large: %d bytes", metaLen)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return nil, "", false, 0, fmt.Errorf("failed to read metadata bytes: %w", err)
	}

	var meta storedMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, "", false, 0, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	dataOffset := int64(4 + metaLen)
	if meta.StoredKey == nil {
		return &meta.Metadata, "", false, dataOffset, nil
	}
	return &meta.Metadata, *meta.StoredKey, true, dataOffset, nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := in.Close(); err == nil {
			err = closeErr
		}
	}()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
	}()

	_, err = io.Copy(out, in)
	return err
}

// lockedReadCloser wraps an os.File and executes an unlock function on Close.
type lockedReadCloser struct {
	*os.File
	unlockFunc func()
}

// newLockedReadCloser creates a new lockedReadCloser.
func newLockedReadCloser(f *os.File, unlockFunc func()) io.ReadCloser {
	return &lockedReadCloser{File: f, unlockFunc: unlockFunc}
}

// Close closes the underlying file and executes the unlock function.
func (lrc *lockedReadCloser) Close() error {
	defer lrc.unlockFunc()
	return lrc.File.Close()
}

// lockedWriteCloser wraps an os.File and executes an onClose function on Close.
type lockedWriteCloser struct {
	*os.File
	onClose func() error
	onAbort func() error
	mu      sync.Mutex
	done    bool
}

// newLockedWriteCloser creates a new lockedWriteCloser.
func newLockedWriteCloser(f *os.File, onClose func() error, onAbort func() error) daramjwee.WriteSink {
	return &lockedWriteCloser{File: f, onClose: onClose, onAbort: onAbort}
}

// Close closes the underlying file and then executes the onClose callback.
// It prioritizes the error from the onClose callback.
func (lwc *lockedWriteCloser) Close() error {
	if !lwc.markDone() {
		return nil
	}

	closeErr := lwc.File.Close()
	if closeErr != nil {
		if lwc.onAbort != nil {
			abortErr := lwc.onAbort()
			if abortErr != nil {
				return errors.Join(closeErr, abortErr)
			}
		}
		return closeErr
	}

	onCloseErr := lwc.onClose()

	if onCloseErr != nil {
		return onCloseErr
	}
	return closeErr
}

func (lwc *lockedWriteCloser) Abort() error {
	if !lwc.markDone() {
		return nil
	}

	closeErr := lwc.File.Close()
	var abortErr error
	if lwc.onAbort != nil {
		abortErr = lwc.onAbort()
	}
	return errors.Join(closeErr, abortErr)
}

func (lwc *lockedWriteCloser) markDone() bool {
	lwc.mu.Lock()
	defer lwc.mu.Unlock()
	if lwc.done {
		return false
	}
	lwc.done = true
	return true
}

// initializeCurrentSize scans the base directory to calculate the current total size
// and populate the fileSizes map for existing files.
func (fs *FileStore) initializeCurrentSize() error {
	return filepath.Walk(fs.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Skip temporary files
		if strings.Contains(info.Name(), "daramjwee-tmp-") {
			return nil
		}

		// Convert file path back to key
		relPath, err := filepath.Rel(fs.baseDir, path)
		if err != nil {
			return err
		}

		key, err := fs.storedKeyForPath(path, filepath.ToSlash(relPath))
		if err != nil {
			return err
		}
		size := info.Size()

		if oldSize, exists := fs.fileSizes[key]; exists {
			fs.currentSize -= oldSize
			fs.policy.Remove(key)
		}
		fs.fileSizes[key] = size
		fs.currentSize += size
		fs.policy.Add(key, size)

		return nil
	})
}

func (fs *FileStore) nextGeneration() uint64 {
	return fs.generationSeq.Add(1)
}

func (fs *FileStore) generationFloor(key string) uint64 {
	fs.generationMu.Lock()
	defer fs.generationMu.Unlock()
	return fs.generations[key]
}

func (fs *FileStore) setGenerationFloor(key string, generation uint64) {
	fs.generationMu.Lock()
	defer fs.generationMu.Unlock()
	if generation > fs.generations[key] {
		fs.generations[key] = generation
	}
}

func (fs *FileStore) beginGenerationTracking(key string) {
	fs.generationMu.Lock()
	defer fs.generationMu.Unlock()
	fs.activeWriters[key]++
}

func (fs *FileStore) finishGenerationTracking(key string) {
	fs.generationMu.Lock()
	defer fs.generationMu.Unlock()

	active := fs.activeWriters[key]
	if active == 0 {
		return
	}
	if active == 1 {
		delete(fs.activeWriters, key)
		delete(fs.generations, key)
		return
	}
	fs.activeWriters[key] = active - 1
}

func (fs *FileStore) pruneGenerationIfIdle(key string) {
	fs.generationMu.Lock()
	defer fs.generationMu.Unlock()
	if fs.activeWriters[key] == 0 {
		delete(fs.activeWriters, key)
		delete(fs.generations, key)
	}
}

func (fs *FileStore) storedKeyForPath(path, relPath string) (string, error) {
	if fs.isAmbiguousLegacyPath(path) {
		storedKey, storedKeyPresent, err := fs.readStoredKeyForCandidate(path)
		if err != nil {
			return "", err
		}
		if storedKeyPresent {
			return storedKey, nil
		}
		return relPath, nil
	}

	if key, ok := decodeStoredKey(relPath); ok {
		return key, nil
	}
	return relPath, nil
}

// updateAfterSet updates the policy and size tracking after a successful file write.
func (fs *FileStore) updateAfterSet(key, path string) error {
	// Get the file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file after write: %w", err)
	}

	newFileSize := fileInfo.Size()

	fs.mu.Lock()

	// If the file already existed, subtract its old size
	if oldSize, exists := fs.fileSizes[key]; exists {
		fs.currentSize -= oldSize
	}

	// Add the new file size
	fs.fileSizes[key] = newFileSize
	fs.currentSize += newFileSize
	fs.policy.Add(key, newFileSize)

	// Collect keys to evict while holding the lock
	var keysToEvict []string
	projectedSize := fs.currentSize
	scheduled := make(map[string]struct{})
	if fs.capacity > 0 {
		for projectedSize > fs.capacity {
			candidates := fs.policy.Evict()
			if len(candidates) == 0 {
				break
			}
			// Filter out the current key to avoid deadlock
			var filteredCandidates []string
			for _, candidate := range candidates {
				if candidate != key {
					if _, alreadyScheduled := scheduled[candidate]; alreadyScheduled {
						continue
					}
					filteredCandidates = append(filteredCandidates, candidate)
				}
			}

			if len(filteredCandidates) == 0 {
				// If only the current key was a candidate, we can't evict it now.
				// This means the cache might temporarily exceed capacity.
				level.Warn(fs.logger).Log("msg", "eviction failed, policy only suggested evicting the key being written", "key", key)
				break
			}

			keysToEvict = append(keysToEvict, filteredCandidates...)

			for _, keyToEvict := range filteredCandidates {
				scheduled[keyToEvict] = struct{}{}
				if size, exists := fs.fileSizes[keyToEvict]; exists {
					projectedSize -= size
				}
			}
		}
	}

	fs.mu.Unlock()

	// Perform actual file deletions without holding the mutex
	heldSlots := map[uint64]struct{}{
		fs.lockManager.getSlot(path): {},
	}
	for _, keyToEvict := range keysToEvict {
		if err := fs.evictKey(keyToEvict, heldSlots); err != nil {
			level.Warn(fs.logger).Log("msg", "failed to evict file", "key", keyToEvict, "err", err)
			fs.mu.Lock()
			if size, exists := fs.fileSizes[keyToEvict]; exists {
				fs.policy.Add(keyToEvict, size)
			}
			fs.mu.Unlock()
		} else {
			level.Debug(fs.logger).Log("msg", "file evicted", "key", keyToEvict)
		}
	}

	return nil
}

func (fs *FileStore) evictKey(key string, heldSlots map[uint64]struct{}) error {
	candidates := fs.dataPathCandidates(key)
	paths := candidatePaths(candidates)
	locked := fs.lockPaths(paths, heldSlots)
	defer fs.unlockPaths(locked)

	removablePaths, err := fs.deletablePaths(candidates, key)
	if err != nil {
		return err
	}
	if err := removeFiles(removablePaths); err != nil {
		return fmt.Errorf("failed to remove file during eviction: %w", err)
	}

	fs.mu.Lock()
	if size, exists := fs.fileSizes[key]; exists {
		fs.currentSize -= size
		delete(fs.fileSizes, key)
	}
	fs.policy.Remove(key)
	fs.mu.Unlock()

	return nil
}

func (fs *FileStore) removeLegacyPathOnly(key string, heldSlots map[uint64]struct{}) error {
	legacyPath := fs.legacyDataPath(key)
	currentPath := fs.toDataPath(key)
	if legacyPath == currentPath {
		return nil
	}

	candidate := dataPathCandidate{
		path:      legacyPath,
		isLegacy:  true,
		ambiguous: fs.isAmbiguousLegacyPath(legacyPath),
	}

	locked := fs.lockPaths([]string{candidate.path}, heldSlots)
	defer fs.unlockPaths(locked)

	if candidate.ambiguous {
		storedKey, storedKeyPresent, err := fs.readStoredKeyForCandidate(candidate.path)
		if err != nil {
			return err
		}
		if !fs.candidateMatchesKey(candidate, key, storedKey, storedKeyPresent) {
			return nil
		}
	}

	err := os.Remove(candidate.path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove legacy file: %w", err)
	}
	return nil
}

func encodeKey(key string) string {
	return encodedKeyPrefix + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeStoredKey(relPath string) (string, bool) {
	if dir := filepath.Dir(relPath); dir != "." && dir != encodedKeyDir {
		return "", false
	}
	name := filepath.Base(relPath)
	if !strings.HasPrefix(name, encodedKeyPrefix) {
		return "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(name, encodedKeyPrefix))
	if err != nil {
		return "", false
	}
	return string(decoded), true
}
