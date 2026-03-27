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

// BeginSet returns a WriteSink that discards all data.
func (ns *nullStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	return &nullWriteSink{}, nil
}

// Delete does nothing and returns nil.
func (ns *nullStore) Delete(ctx context.Context, key string) error {
	return nil
}

// Stat always returns ErrNotFound.
func (ns *nullStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	return nil, ErrNotFound
}

func (ns *nullStore) ValidateHotStore() error {
	return &ConfigError{"unsupported hot store"}
}

// nullWriteSink is a WriteSink implementation that discards all written data.
type nullWriteSink struct {
	done bool
}

// Write discards the provided bytes and reports success.
func (nwc *nullWriteSink) Write(p []byte) (n int, err error) {
	if nwc.done {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

// Close does nothing and returns nil.
func (nwc *nullWriteSink) Close() error {
	nwc.done = true
	return nil
}

// Abort does nothing and returns nil.
func (nwc *nullWriteSink) Abort() error {
	nwc.done = true
	return nil
}
