package filestore

import (
	"github.com/mrchypark/daramjwee/internal/stripedlock"
)

// FileLockManager provides a simple file locking mechanism using
// a fixed number of mutexes (striped locking) to avoid global contention.
type FileLockManager struct {
	inner *stripedlock.Manager
}

// NewFileLockManager creates a new manager with a given number of lock slots.
// If slots is 0 or less, it defaults to 2048 slots.
func NewFileLockManager(slots int) *FileLockManager {
	return &FileLockManager{
		inner: stripedlock.New(slots),
	}
}

// RLock locks the mutex for the given key for reading.
func (flm *FileLockManager) RLock(key string) {
	flm.inner.RLock(key)
}

// RUnlock unlocks the mutex for the given key for reading.
func (flm *FileLockManager) RUnlock(key string) {
	flm.inner.RUnlock(key)
}

// Lock locks the mutex for the given key for writing.
func (flm *FileLockManager) Lock(key string) {
	flm.inner.Lock(key)
}

// Unlock unlocks the mutex for the given key for writing.
func (flm *FileLockManager) Unlock(key string) {
	flm.inner.Unlock(key)
}

// getSlot returns the slot index for the given key.
func (flm *FileLockManager) getSlot(key string) uint64 {
	return flm.inner.Slot(key)
}
