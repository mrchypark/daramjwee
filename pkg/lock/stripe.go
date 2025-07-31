package lock

import (
	"sync"

	"github.com/zeebo/xxh3"
)

// StripeLock provides a simple file locking mechanism using
// a fixed number of mutexes (striped locking) to avoid global contention.
type StripeLock struct {
	locks []sync.RWMutex
	slots uint64
}

// NewStripeLock creates a new manager with a given number of lock slots.
// If slots is 0 or less, it defaults to 2048 slots.
func NewStripeLock(slots int) *StripeLock {
	if slots <= 0 {
		slots = 2048 // Default to 2048 slots if invalid value is given
	}
	return &StripeLock{
		locks: make([]sync.RWMutex, slots),
		slots: uint64(slots),
	}
}

// getSlot determines which mutex to use for a given key.
func (sl *StripeLock) getSlot(key string) uint64 {
	return xxh3.HashString(key) % sl.slots
}

// RLock locks the mutex for the given key for reading.
func (sl *StripeLock) RLock(key string) {
	sl.locks[sl.getSlot(key)].RLock()
}

// RUnlock unlocks the mutex for the given key for reading.
func (sl *StripeLock) RUnlock(key string) {
	sl.locks[sl.getSlot(key)].RUnlock()
}

// Lock locks the mutex for the given key for writing.
func (sl *StripeLock) Lock(key string) {
	sl.locks[sl.getSlot(key)].Lock()
}

// Unlock unlocks the mutex for the given key for writing.
func (sl *StripeLock) Unlock(key string) {
	sl.locks[sl.getSlot(key)].Unlock()
}
