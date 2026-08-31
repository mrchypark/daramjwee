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
	markerClean           = "clean\n"
	markerActive          = "active\n"
	currentSnapshotFormat = 2
	snapshotMagic         = "daramjwee-objectstore-catalog"
)

type snapshot struct {
	Magic         string                `json:"_daramjwee_catalog"`
	FormatVersion int                   `json:"format_version"`
	Entries       map[string]Entry      `json:"entries"`
	UploadPlans   map[string]UploadPlan `json:"upload_plans,omitempty"`
}

type UploadPlanMember struct {
	Key              string `json:"key"`
	Generation       uint64 `json:"generation"`
	PublicationToken string `json:"publication_token"`
	Offset           int64  `json:"offset"`
	Length           int64  `json:"length"`
}

type UploadPlan struct {
	RemotePath string             `json:"remote_path"`
	Size       int64              `json:"size,omitempty"`
	SizeKnown  bool               `json:"size_known,omitempty"`
	Terminal   string             `json:"terminal,omitempty"`
	Members    []UploadPlanMember `json:"members"`
}

var (
	writeFileFn = os.WriteFile
	renameFn    = os.Rename
	syncPathFn  = syncPath
	syncDirFn   = syncDir
)

type Entry struct {
	SegmentPath            string             `json:"segment_path"`
	Offset                 int64              `json:"offset"`
	Length                 int64              `json:"length"`
	Generation             uint64             `json:"generation,omitempty"`
	Missing                bool               `json:"missing,omitempty"`
	RemotePublished        bool               `json:"remote_published,omitempty"`
	CleanupPending         bool               `json:"cleanup_pending,omitempty"`
	RemotePath             string             `json:"remote_path,omitempty"`
	RemoteOffset           int64              `json:"remote_offset,omitempty"`
	IntentCleanupPending   bool               `json:"intent_cleanup_pending,omitempty"`
	Superseded             bool               `json:"superseded,omitempty"`
	PendingRemotePath      string             `json:"pending_remote_path,omitempty"`
	PendingRemoteOffset    int64              `json:"pending_remote_offset,omitempty"`
	PendingRemoteSize      int64              `json:"pending_remote_size,omitempty"`
	PendingRemoteSizeKnown bool               `json:"pending_remote_size_known,omitempty"`
	PublicationToken       string             `json:"publication_token,omitempty"`
	RemoteVersionSet       bool               `json:"remote_version_set,omitempty"`
	RemoteVersionAbsent    bool               `json:"remote_version_absent,omitempty"`
	RemoteVersionType      int                `json:"remote_version_type,omitempty"`
	RemoteVersionValue     string             `json:"remote_version_value,omitempty"`
	Metadata               daramjwee.Metadata `json:"metadata"`
}

type Catalog struct {
	path                 string
	mu                   sync.RWMutex
	entries              map[string]Entry
	uploadPlans          map[string]UploadPlan
	dirDurabilityPending bool
	markerCleanupPending bool
	terminalErr          error
}

func Open(dir string) (*Catalog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	c := &Catalog{
		path:        filepath.Join(dir, "snapshot.json"),
		entries:     make(map[string]Entry),
		uploadPlans: make(map[string]UploadPlan),
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
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(data, &probe); err != nil {
			return nil, err
		}
		var magic string
		if raw, ok := probe["_daramjwee_catalog"]; ok {
			_ = json.Unmarshal(raw, &magic)
		}
		if magic == snapshotMagic {
			var stored snapshot
			if err := json.Unmarshal(data, &stored); err != nil {
				return nil, err
			}
			if stored.FormatVersion != currentSnapshotFormat {
				return nil, fmt.Errorf("catalog: unsupported snapshot format %d", stored.FormatVersion)
			}
			if stored.Entries != nil {
				c.entries = stored.Entries
			}
			if stored.UploadPlans != nil {
				c.uploadPlans = stored.UploadPlans
			}
			if migrateUploadPlans(c.entries, c.uploadPlans) {
				if _, err := c.persistLocked(); err != nil {
					return nil, fmt.Errorf("catalog: migrate upload plans: %w", err)
				}
			}
		} else {
			if err := json.Unmarshal(data, &c.entries); err != nil {
				return nil, err
			}
			migrateUploadPlans(c.entries, c.uploadPlans)
			if _, err := c.persistLocked(); err != nil {
				return nil, fmt.Errorf("catalog: migrate legacy snapshot: %w", err)
			}
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

func (c *Catalog) UploadPlans() map[string]UploadPlan {
	c.mu.RLock()
	defer c.mu.RUnlock()
	plans := make(map[string]UploadPlan, len(c.uploadPlans))
	for path, plan := range c.uploadPlans {
		plan.Members = append([]UploadPlanMember(nil), plan.Members...)
		plans[path] = plan
	}
	return plans
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

// UpdateManyIf atomically applies updates only while every expected entry is
// still current. It is used to persist a complete packed-upload plan before
// any remote object is created.
func (c *Catalog) UpdateManyIf(expected, updates map[string]Entry) (bool, error) {
	return c.UpdateManyIfWithPlans(expected, updates, nil, nil)
}

func (c *Catalog) UpdateManyIfWithPlans(expected, updates map[string]Entry, planUpdates map[string]UploadPlan, planDeletes []string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.syncPendingDirLocked(); err != nil {
		return false, err
	}
	for key, entry := range expected {
		current, ok := c.entries[key]
		if !ok || current != entry {
			return false, nil
		}
	}

	previous := make(map[string]Entry, len(updates))
	for key, next := range updates {
		previous[key] = c.entries[key]
		c.entries[key] = next
	}
	previousPlans := make(map[string]UploadPlan, len(planUpdates)+len(planDeletes))
	existedPlans := make(map[string]bool, len(previousPlans))
	for path, plan := range planUpdates {
		previousPlans[path], existedPlans[path] = c.uploadPlans[path]
		c.uploadPlans[path] = plan
	}
	for _, path := range planDeletes {
		if _, tracked := existedPlans[path]; !tracked {
			previousPlans[path], existedPlans[path] = c.uploadPlans[path]
		}
		delete(c.uploadPlans, path)
	}
	if committed, err := c.persistLocked(); err != nil {
		restore := func() {
			for key, prev := range previous {
				c.entries[key] = prev
			}
			for path, prev := range previousPlans {
				if existedPlans[path] {
					c.uploadPlans[path] = prev
				} else {
					delete(c.uploadPlans, path)
				}
			}
		}
		visible, err := c.finishPersistLocked(committed, err, restore)
		return visible, err
	}
	return true, nil
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

	data, err := json.Marshal(snapshot{Magic: snapshotMagic, FormatVersion: currentSnapshotFormat, Entries: c.entries, UploadPlans: c.uploadPlans})
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

func migrateUploadPlans(entries map[string]Entry, plans map[string]UploadPlan) bool {
	existing := make(map[string]struct{}, len(plans))
	for path := range plans {
		existing[path] = struct{}{}
	}
	created := make(map[string]UploadPlan)
	for key, entry := range entries {
		path := entry.PendingRemotePath
		offset := entry.PendingRemoteOffset
		if path == "" && entry.IntentCleanupPending {
			path = entry.RemotePath
			offset = entry.RemoteOffset
		}
		if path == "" {
			continue
		}
		if _, exists := existing[path]; exists {
			continue
		}
		plan := created[path]
		plan.RemotePath = path
		if entry.PendingRemoteSizeKnown {
			plan.Size = entry.PendingRemoteSize
			plan.SizeKnown = true
		}
		plan.Members = append(plan.Members, UploadPlanMember{Key: key, Generation: entry.Generation, PublicationToken: entry.PublicationToken, Offset: offset, Length: entry.Length})
		created[path] = plan
	}
	for path, plan := range created {
		plans[path] = plan
	}
	return len(created) > 0
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
