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
func NewFileLockManager(slots int) *FileLockManager {
	if slots <= 0 {
		slots = 2048 // Default to 2048 slots if invalid value is given
	}
	return &FileLockManager{
		locks: make([]sync.RWMutex, slots),
		slots: uint64(slots),
	}
}

func (flm *FileLockManager) getSlot(key string) uint64 {
	return xxh3.HashString(key) % flm.slots
}

func (flm *FileLockManager) RLock(key string) {
	flm.locks[flm.getSlot(key)].RLock()
}

func (flm *FileLockManager) RUnlock(key string) {
	flm.locks[flm.getSlot(key)].RUnlock()
}

func (flm *FileLockManager) Lock(key string) {
	flm.locks[flm.getSlot(key)].Lock()
}

func (flm *FileLockManager) Unlock(key string) {
	flm.locks[flm.getSlot(key)].Unlock()
}
