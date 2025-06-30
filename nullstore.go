package daramjwee

import (
	"context"
	"io"
)

// nullStore is a Null Object implementation of the Store interface.
// It performs no operations and is used when a ColdStore is not configured,
// avoiding nil checks.
type nullStore struct{}

// newNullStore creates a new instance of nullStore.
func newNullStore() Store {
	return &nullStore{}
}

// GetStream always returns ErrNotFound.
func (ns *nullStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

// SetWithWriter returns an io.WriteCloser that discards all data.
func (ns *nullStore) SetWithWriter(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	return &nullWriteCloser{}, nil
}

// Delete does nothing and returns nil.
func (ns *nullStore) Delete(ctx context.Context, key string) error {
	return nil
}

// Stat always returns ErrNotFound.
func (ns *nullStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

// nullWriteCloser is an io.WriteCloser implementation that discards all written data.
type nullWriteCloser struct{}

// Write discards the provided bytes and reports success.
func (nwc *nullWriteCloser) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// Close does nothing and returns nil.
func (nwc *nullWriteCloser) Close() error {
	return nil
}
