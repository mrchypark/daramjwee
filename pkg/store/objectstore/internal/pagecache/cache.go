package pagecache

import (
	"container/list"
	"strconv"
	"sync"
)

type Key struct {
	Version string
	Index   int64
}

func (k Key) String() string {
	return k.Version + ":" + strconv.FormatInt(k.Index, 10)
}

type Cache struct {
	mu       sync.Mutex
	maxBytes int64
	current  int64
	lru      *list.List
	entries  map[Key]*list.Element
}

type entry struct {
	key  Key
	data []byte
	size int64
}

func New(maxBytes int64) *Cache {
	if maxBytes <= 0 {
		return nil
	}
	return &Cache{
		maxBytes: maxBytes,
		lru:      list.New(),
		entries:  make(map[Key]*list.Element),
	}
}

func (c *Cache) Get(key Key) ([]byte, bool) {
	if c == nil {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(elem)
	return elem.Value.(*entry).data, true
}

func (c *Cache) Set(key Key, data []byte) {
	if c == nil || c.maxBytes <= 0 {
		return
	}

	size := int64(len(data))
	if size <= 0 || size > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		ent := elem.Value.(*entry)
		c.current -= ent.size
		ent.data = data
		ent.size = size
		c.current += size
		c.lru.MoveToFront(elem)
		c.evict()
		return
	}

	elem := c.lru.PushFront(&entry{key: key, data: data, size: size})
	c.entries[key] = elem
	c.current += size
	c.evict()
}

func (c *Cache) evict() {
	for c.current > c.maxBytes {
		back := c.lru.Back()
		if back == nil {
			return
		}
		ent := back.Value.(*entry)
		delete(c.entries, ent.key)
		c.current -= ent.size
		c.lru.Remove(back)
	}
}
