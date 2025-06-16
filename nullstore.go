// Package daramjwee provides core components for a hybrid caching system.
// This file implements NullStore, a no-op (Null Object pattern) implementation
// of the Store interface.
package daramjwee

import (
	"context"
	"io"
)

// nullStore is a Null Object implementation of the Store interface.
// It performs no actual storage operations. Its methods are generally no-ops
// or return non-committal values (like ErrNotFound or nil errors).
// This is useful in configurations where a particular cache tier (e.g., ColdStore)
// is not required, allowing the system to operate without nil checks for the store.
type nullStore struct{}

// newNullStore creates a new instance of nullStore.
// It returns a Store interface type, fulfilled by the *nullStore.
func newNullStore() Store {
	return &nullStore{}
}

// Ensures at compile time that nullStore satisfies the Store interface.
var _ Store = (*nullStore)(nil)

// GetStream, for nullStore, always returns ErrNotFound, indicating that
// no object will ever be found in this store.
// Parameters:
//   ctx: The context for the operation (unused by nullStore).
//   key: The key of the object to retrieve (unused by nullStore).
// Returns:
//   nil io.ReadCloser, nil *Metadata, and ErrNotFound.
func (ns *nullStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

// SetWithWriter, for nullStore, returns a no-op io.WriteCloser that discards all data written to it.
// This simulates a successful write without actually storing anything.
// Parameters:
//   ctx: The context for the operation (unused by nullStore).
//   key: The key of the object to set (unused by nullStore).
//   etag: The ETag of the object (unused by nullStore).
// Returns:
//   A pointer to nullWriteCloser and a nil error.
func (ns *nullStore) SetWithWriter(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	return &nullWriteCloser{}, nil
}

// Delete, for nullStore, performs no operation and always returns nil,
// indicating a successful deletion (or that the item was already not present).
// Parameters:
//   ctx: The context for the operation (unused by nullStore).
//   key: The key of the object to delete (unused by nullStore).
// Returns:
//   nil error.
func (ns *nullStore) Delete(ctx context.Context, key string) error {
	return nil
}

// Stat, for nullStore, always returns ErrNotFound, indicating that
// no metadata can be retrieved for any key from this store.
// Parameters:
//   ctx: The context for the operation (unused by nullStore).
//   key: The key of the object to stat (unused by nullStore).
// Returns:
//   nil *Metadata and ErrNotFound.
func (ns *nullStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

// nullWriteCloser implements the io.WriteCloser interface but performs no operations.
// All data written to it is discarded. This is used by nullStore.SetWithWriter.
type nullWriteCloser struct{}

// Write, for nullWriteCloser, accepts the byte slice p, reports that all bytes
// were "written" successfully by returning len(p), but actually discards the data.
// Parameters:
//   p: The byte slice to "write".
// Returns:
//   The number of bytes from p (len(p)) and a nil error.
func (nwc *nullWriteCloser) Write(p []byte) (n int, err error) {
	return len(p), nil // Reports success but discards the data.
}

// Close, for nullWriteCloser, performs no operation and returns nil.
func (nwc *nullWriteCloser) Close() error {
	return nil
}
