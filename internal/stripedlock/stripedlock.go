// Package stripedlock provides a striped lock manager for reducing contention
// by distributing keys across a fixed number of mutexes.
package stripedlock

import (
	"sync"

	"github.com/zeebo/xxh3"
)

// Manager provides striped locking using a fixed number of RWMutexes.
// Keys are distributed across slots using xxh3 hashing.
type Manager struct {
	locks []sync.RWMutex
	slots uint64
}

// New creates a new striped lock manager with the given number of slots.
// If slots is 0 or negative, it defaults to 2048.
func New(slots int) *Manager {
	if slots <= 0 {
		slots = 2048
	}
	return &Manager{
		locks: make([]sync.RWMutex, slots),
		slots: uint64(slots),
	}
}

func (m *Manager) slot(key string) uint64 {
	return xxh3.HashString(key) % m.slots
}

// Slot returns the slot index for the given key, useful for ordered locking.
func (m *Manager) Slot(key string) uint64 {
	return m.slot(key)
}

// Lock acquires the write lock for the given key.
func (m *Manager) Lock(key string) {
	m.locks[m.slot(key)].Lock()
}

// Unlock releases the write lock for the given key.
func (m *Manager) Unlock(key string) {
	m.locks[m.slot(key)].Unlock()
}

// RLock acquires the read lock for the given key.
func (m *Manager) RLock(key string) {
	m.locks[m.slot(key)].RLock()
}

// RUnlock releases the read lock for the given key.
func (m *Manager) RUnlock(key string) {
	m.locks[m.slot(key)].RUnlock()
}
