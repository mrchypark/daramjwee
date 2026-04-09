package objectstore

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/zeebo/xxh3"
)

type packedWholeObjectCache struct {
	logger   log.Logger
	root     string
	capacity int64

	mu      sync.Mutex
	entries map[string]*packedWholeObjectCacheEntry
	lru     *list.List
	bytes   int64
}

type packedWholeObjectCacheEntry struct {
	key            string
	path           string
	size           int64
	refs           int
	evicted        bool
	evictOnRelease bool
	lruElement     *list.Element
}

func newPackedWholeObjectCache(root string, capacity int64, logger log.Logger) (*packedWholeObjectCache, error) {
	if capacity <= 0 {
		return nil, nil
	}
	if err := os.RemoveAll(root); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &packedWholeObjectCache{
		logger:   logger,
		root:     root,
		capacity: capacity,
		entries:  make(map[string]*packedWholeObjectCacheEntry),
		lru:      list.New(),
	}, nil
}

func (c *packedWholeObjectCache) open(entry checkpointEntry) (io.ReadCloser, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	key := c.cacheKey(entry)

	c.mu.Lock()
	cacheEntry, ok := c.entries[key]
	if !ok || cacheEntry.evicted {
		c.mu.Unlock()
		return nil, false, nil
	}
	cacheEntry.refs++
	c.lru.MoveToBack(cacheEntry.lruElement)
	path := cacheEntry.path
	c.mu.Unlock()

	file, err := os.Open(path)
	if err == nil {
		return &fileSectionReadCloser{
			Reader: file,
			closeFn: func() error {
				err := file.Close()
				c.release(key)
				return err
			},
		}, true, nil
	}

	c.releaseBrokenOpen(key, err)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	return nil, false, err
}

func (c *packedWholeObjectCache) wrapRemoteReader(entry checkpointEntry, reader io.ReadCloser) (io.ReadCloser, error) {
	if c == nil {
		return reader, nil
	}

	file, err := os.CreateTemp(c.root, "packed-whole-*.data")
	if err != nil {
		return reader, nil
	}

	return &packedWholeObjectCachingReader{
		reader:       reader,
		file:         file,
		cache:        c,
		cacheKey:     c.cacheKey(entry),
		size:         entry.Length,
		tempPath:     file.Name(),
		cacheEnabled: true,
	}, nil
}

func (c *packedWholeObjectCache) cacheKey(entry checkpointEntry) string {
	identity := fmt.Sprintf("%s:%d:%d:%s", entry.SegmentPath, entry.Offset, entry.Length, entry.Metadata.CacheTag)
	return fmt.Sprintf("%016x", xxh3.HashString(identity))
}

func (c *packedWholeObjectCache) publish(key, path string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.entries[key]; ok {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			level.Warn(c.logger).Log("msg", "failed to discard duplicate packed whole-object cache file", "path", path, "err", err)
		}
		return
	}

	entry := &packedWholeObjectCacheEntry{
		key:  key,
		path: path,
		size: size,
	}
	entry.lruElement = c.lru.PushBack(entry)
	c.entries[key] = entry
	c.bytes += size
	c.evictLocked()
}

func (c *packedWholeObjectCache) evictLocked() {
	for c.bytes > c.capacity {
		elem := c.lru.Front()
		if elem == nil {
			return
		}
		entry := elem.Value.(*packedWholeObjectCacheEntry)
		if entry.refs > 0 {
			entry.evicted = true
			entry.evictOnRelease = true
			c.lru.Remove(elem)
			entry.lruElement = nil
			continue
		}
		c.deleteEntryLocked(entry)
	}
}

func (c *packedWholeObjectCache) release(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && entry.evictOnRelease {
		c.deleteEntryLocked(entry)
	}
}

func (c *packedWholeObjectCache) releaseBrokenOpen(key string, openErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	if os.IsNotExist(openErr) {
		c.deleteEntryLocked(entry)
	}
}

func (c *packedWholeObjectCache) deleteEntryLocked(entry *packedWholeObjectCacheEntry) {
	if entry == nil {
		return
	}
	delete(c.entries, entry.key)
	if entry.lruElement != nil {
		c.lru.Remove(entry.lruElement)
		entry.lruElement = nil
	}
	c.bytes -= entry.size
	if c.bytes < 0 {
		c.bytes = 0
	}
	if err := os.Remove(entry.path); err != nil && !os.IsNotExist(err) {
		level.Warn(c.logger).Log("msg", "failed to delete packed whole-object cache file", "path", entry.path, "err", err)
	}
}

type packedWholeObjectCachingReader struct {
	reader       io.ReadCloser
	file         *os.File
	cache        *packedWholeObjectCache
	cacheKey     string
	size         int64
	read         int64
	tempPath     string
	cacheEnabled bool
	eof          bool

	once sync.Once
	err  error
}

func (r *packedWholeObjectCachingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.read += int64(n)
		if r.cacheEnabled {
			if _, writeErr := r.file.Write(p[:n]); writeErr != nil {
				r.cacheEnabled = false
			}
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.cacheEnabled = false
	}
	if errors.Is(err, io.EOF) && r.read == r.size {
		r.eof = true
	}
	return n, err
}

func (r *packedWholeObjectCachingReader) Close() error {
	r.once.Do(func() {
		closeErr := r.reader.Close()
		fileErr := r.file.Close()
		if closeErr == nil && fileErr == nil && r.cacheEnabled && r.eof && r.read == r.size {
			r.cache.publish(r.cacheKey, r.tempPath, r.size)
		} else if err := os.Remove(r.tempPath); err != nil && !os.IsNotExist(err) {
			level.Warn(r.cache.logger).Log("msg", "failed to remove discarded packed whole-object cache file", "path", r.tempPath, "err", err)
		}

		r.err = closeErr
		if r.err == nil {
			r.err = fileErr
		}
	})
	return r.err
}
