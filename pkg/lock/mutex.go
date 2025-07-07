package lock

import "sync"

// MutexLock is a simple lock that uses a single RWMutex.
type MutexLock struct {
	mu sync.RWMutex
}

// NewMutexLock creates a new MutexLock.
func NewMutexLock() *MutexLock {
	return &MutexLock{}
}

// Lock locks the mutex for writing.
func (l *MutexLock) Lock(key string) {
	l.mu.Lock()
}

// Unlock unlocks the mutex for writing.
func (l *MutexLock) Unlock(key string) {
	l.mu.Unlock()
}

// RLock locks the mutex for reading.
func (l *MutexLock) RLock(key string) {
	l.mu.RLock()
}

// RUnlock unlocks the mutex for reading.
func (l *MutexLock) RUnlock(key string) {
	l.mu.RUnlock()
}
