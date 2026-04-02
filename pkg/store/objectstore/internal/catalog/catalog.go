package catalog

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
)

var (
	writeFileFn = os.WriteFile
	renameFn    = os.Rename
	syncPathFn  = syncPath
	syncDirFn   = syncDir
)

type Entry struct {
	SegmentPath  string             `json:"segment_path"`
	Offset       int64              `json:"offset"`
	Length       int64              `json:"length"`
	Missing      bool               `json:"missing,omitempty"`
	RemotePath   string             `json:"remote_path,omitempty"`
	RemoteOffset int64              `json:"remote_offset,omitempty"`
	Metadata     daramjwee.Metadata `json:"metadata"`
}

type Catalog struct {
	path    string
	mu      sync.RWMutex
	entries map[string]Entry
}

func Open(dir string) (*Catalog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	c := &Catalog{
		path:    filepath.Join(dir, "snapshot.json"),
		entries: make(map[string]Entry),
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(data, &c.entries); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Catalog) Get(key string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	return entry, ok
}

func (c *Catalog) Entries() map[string]Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	snapshot := make(map[string]Entry, len(c.entries))
	for key, entry := range c.entries {
		snapshot[key] = entry
	}
	return snapshot
}

func (c *Catalog) Update(key string, fn func(Entry, bool) (Entry, bool)) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	current, ok := c.entries[key]
	next, keep := fn(current, ok)
	if keep {
		c.entries[key] = next
	} else {
		delete(c.entries, key)
	}
	if committed, err := c.persistLocked(); err != nil {
		if !committed {
			if ok {
				c.entries[key] = current
			} else {
				delete(c.entries, key)
			}
		}
		return err
	}
	return nil
}

func (c *Catalog) UpdateMany(updates map[string]Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	previous := make(map[string]Entry, len(updates))
	existed := make(map[string]bool, len(updates))
	for key, next := range updates {
		prev, ok := c.entries[key]
		previous[key] = prev
		existed[key] = ok
		c.entries[key] = next
	}
	if committed, err := c.persistLocked(); err != nil {
		if !committed {
			for key := range updates {
				if existed[key] {
					c.entries[key] = previous[key]
				} else {
					delete(c.entries, key)
				}
			}
		}
		return err
	}
	return nil
}

func (c *Catalog) Set(key string, entry Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, existed := c.entries[key]
	c.entries[key] = entry
	if committed, err := c.persistLocked(); err != nil {
		if !committed {
			if existed {
				c.entries[key] = prev
			} else {
				delete(c.entries, key)
			}
		}
		return err
	}
	return nil
}

func (c *Catalog) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, existed := c.entries[key]
	delete(c.entries, key)
	if committed, err := c.persistLocked(); err != nil {
		if !committed && existed {
			c.entries[key] = prev
		}
		return err
	}
	return nil
}

func (c *Catalog) persistLocked() (bool, error) {
	data, err := json.Marshal(c.entries)
	if err != nil {
		return false, err
	}

	tmpPath := c.path + ".tmp"
	if err := writeFileFn(tmpPath, data, 0o644); err != nil {
		return false, err
	}
	if err := syncPathFn(tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}
	if err := renameFn(tmpPath, c.path); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}
	if err := syncDirFn(filepath.Dir(c.path)); err != nil {
		return true, err
	}
	return true, nil
}

func syncPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
