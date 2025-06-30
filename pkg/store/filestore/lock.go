package filestore

import (
	"sync"

	"github.com/zeebo/xxh3"
)

// FileLockManager provides a simple file locking mechanism using
// a fixed number of mutexes (striped locking) to avoid global contention.
type FileLockManager struct {
	locks []sync.RWMutex
	slots uint64
}

// NewFileLockManager creates a new manager with a given number of lock slots.
// If slots is 0 or less, it defaults to 2048 slots.
func NewFileLockManager(slots int) *FileLockManager {
	if slots <= 0 {
		slots = 2048 // Default to 2048 slots if invalid value is given
	}
	return &FileLockManager{
		locks: make([]sync.RWMutex, slots),
		slots: uint64(slots),
	}
}

// getSlot determines which mutex to use for a given key.
func (flm *FileLockManager) getSlot(key string) uint64 {
	return xxh3.HashString(key) % flm.slots
}

// RLock locks the mutex for the given key for reading.
func (flm *FileLockManager) RLock(key string) {
	flm.locks[flm.getSlot(key)].RLock()
}

// RUnlock unlocks the mutex for the given key for reading.
func (flm *FileLockManager) RUnlock(key string) {
	flm.locks[flm.getSlot(key)].RUnlock()
}

// Lock locks the mutex for the given key for writing.
func (flm *FileLockManager) Lock(key string) {
	flm.locks[flm.getSlot(key)].Lock()
}

// Unlock unlocks the mutex for the given key for writing.
func (flm *FileLockManager) Unlock(key string) {
	flm.locks[flm.getSlot(key)].Unlock()
}
