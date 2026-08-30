package catalog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/goccy/go-json"

	"github.com/mrchypark/daramjwee"
)

var ErrAmbiguousCommit = fmt.Errorf("catalog: commit durability is ambiguous: %w", daramjwee.ErrCommitOutcomeUnknown)

const (
	markerClean  = "clean\n"
	markerActive = "active\n"
)

var (
	writeFileFn = os.WriteFile
	renameFn    = os.Rename
	syncPathFn  = syncPath
	syncDirFn   = syncDir
)

type Entry struct {
	SegmentPath         string             `json:"segment_path"`
	Offset              int64              `json:"offset"`
	Length              int64              `json:"length"`
	Generation          uint64             `json:"generation,omitempty"`
	Missing             bool               `json:"missing,omitempty"`
	RemotePublished     bool               `json:"remote_published,omitempty"`
	CleanupPending      bool               `json:"cleanup_pending,omitempty"`
	RemotePath          string             `json:"remote_path,omitempty"`
	RemoteOffset        int64              `json:"remote_offset,omitempty"`
	PendingRemotePath   string             `json:"pending_remote_path,omitempty"`
	PendingRemoteOffset int64              `json:"pending_remote_offset,omitempty"`
	Metadata            daramjwee.Metadata `json:"metadata"`
}

type Catalog struct {
	path                 string
	mu                   sync.RWMutex
	entries              map[string]Entry
	dirDurabilityPending bool
	markerCleanupPending bool
	terminalErr          error
}

func Open(dir string) (*Catalog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	c := &Catalog{
		path:    filepath.Join(dir, "snapshot.json"),
		entries: make(map[string]Entry),
	}
	markerPath := c.path + ".state"
	marker, err := os.ReadFile(markerPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.WriteFile(markerPath, []byte(markerClean), 0o644); err != nil {
			return nil, err
		}
		if err := syncPath(markerPath); err != nil {
			return nil, err
		}
		if err := syncDirFn(dir); err != nil {
			return nil, err
		}
	} else if string(marker) != markerClean {
		return nil, fmt.Errorf("%w: recovery marker %q is not clean", ErrAmbiguousCommit, markerPath)
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &c.entries); err != nil {
			return nil, err
		}
	}
	if err := syncDirFn(dir); err != nil {
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

func (c *Catalog) Health() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.terminalErr
}

func (c *Catalog) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncPendingDirLocked()
}

// Update reports whether the requested state is visible, even when the final
// directory sync fails after the snapshot rename.
func (c *Catalog) Update(key string, fn func(Entry, bool) (Entry, bool)) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncPendingDirLocked(); err != nil {
		return false, err
	}

	current, ok := c.entries[key]
	next, keep := fn(current, ok)
	if keep {
		if next == current {
			return true, nil
		}
		c.entries[key] = next
	} else {
		if !ok {
			return true, nil
		}
		delete(c.entries, key)
	}
	if committed, err := c.persistLocked(); err != nil {
		restore := func() {
			if ok {
				c.entries[key] = current
			} else {
				delete(c.entries, key)
			}
		}
		return c.finishPersistLocked(committed, err, restore)
	}
	return true, nil
}

func (c *Catalog) UpdateMany(updates map[string]Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncPendingDirLocked(); err != nil {
		return err
	}

	previous := make(map[string]Entry, len(updates))
	existed := make(map[string]bool, len(updates))
	changed := false
	for key, next := range updates {
		prev, ok := c.entries[key]
		previous[key] = prev
		existed[key] = ok
		if ok && next == prev {
			continue
		}
		changed = true
		c.entries[key] = next
	}
	if !changed {
		return nil
	}
	if committed, err := c.persistLocked(); err != nil {
		restore := func() {
			for key := range updates {
				if existed[key] {
					c.entries[key] = previous[key]
				} else {
					delete(c.entries, key)
				}
			}
		}
		_, err = c.finishPersistLocked(committed, err, restore)
		return err
	}
	return nil
}

func (c *Catalog) Set(key string, entry Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncPendingDirLocked(); err != nil {
		return err
	}
	prev, existed := c.entries[key]
	if existed && entry == prev {
		return nil
	}
	c.entries[key] = entry
	if committed, err := c.persistLocked(); err != nil {
		restore := func() {
			if existed {
				c.entries[key] = prev
			} else {
				delete(c.entries, key)
			}
		}
		_, err = c.finishPersistLocked(committed, err, restore)
		return err
	}
	return nil
}

func (c *Catalog) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncPendingDirLocked(); err != nil {
		return err
	}
	prev, existed := c.entries[key]
	if !existed {
		return nil
	}
	delete(c.entries, key)
	if committed, err := c.persistLocked(); err != nil {
		restore := func() {
			if existed {
				c.entries[key] = prev
			}
		}
		_, err = c.finishPersistLocked(committed, err, restore)
		return err
	}
	return nil
}

func (c *Catalog) persistLocked() (bool, error) {
	if err := c.writeMarkerLocked(markerActive); err != nil {
		return c.failBeforeCommitLocked(err)
	}

	data, err := json.Marshal(c.entries)
	if err != nil {
		return c.failBeforeCommitLocked(err)
	}

	tmpPath := c.path + ".tmp"
	if err := writeFileFn(tmpPath, data, 0o644); err != nil {
		return c.failBeforeCommitLocked(err)
	}
	if err := syncPathFn(tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return c.failBeforeCommitLocked(err)
	}
	if err := renameFn(tmpPath, c.path); err != nil {
		_ = os.Remove(tmpPath)
		return c.failBeforeCommitLocked(err)
	}
	if err := syncDirFn(filepath.Dir(c.path)); err != nil {
		c.dirDurabilityPending = true
		return true, errors.Join(ErrAmbiguousCommit, err)
	}
	c.dirDurabilityPending = false
	if err := c.clearMarkerLocked(); err != nil {
		return true, errors.Join(ErrAmbiguousCommit, fmt.Errorf("clear recovery marker: %w", err))
	}
	return true, nil
}

func (c *Catalog) failBeforeCommitLocked(err error) (bool, error) {
	if clearErr := c.clearMarkerLocked(); clearErr != nil {
		return true, errors.Join(ErrAmbiguousCommit, err, fmt.Errorf("clear recovery marker: %w", clearErr))
	}
	return false, err
}

func (c *Catalog) writeMarkerLocked(state string) error {
	markerPath := c.path + ".state"
	if err := writeFileFn(markerPath, []byte(state), 0o644); err != nil {
		return err
	}
	return syncPathFn(markerPath)
}

func (c *Catalog) clearMarkerLocked() error {
	if err := c.writeMarkerLocked(markerClean); err != nil {
		c.markerCleanupPending = true
		return err
	}
	c.markerCleanupPending = false
	return nil
}

func (c *Catalog) finishPersistLocked(committed bool, err error, restore func()) (bool, error) {
	if !committed {
		restore()
		return false, err
	}
	c.terminalErr = err
	return true, err
}

func (c *Catalog) syncPendingDirLocked() error {
	if c.terminalErr != nil {
		return c.terminalErr
	}
	if c.markerCleanupPending {
		if err := c.clearMarkerLocked(); err != nil {
			return err
		}
	}
	if !c.dirDurabilityPending {
		return nil
	}
	if err := syncDirFn(filepath.Dir(c.path)); err != nil {
		return errors.Join(ErrAmbiguousCommit, err)
	}
	c.dirDurabilityPending = false
	return nil
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
