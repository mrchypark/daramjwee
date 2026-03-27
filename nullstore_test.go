package daramjwee

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNullStore_WriteAfterCloseFails(t *testing.T) {
	store := newNullStore()

	sink, err := store.BeginSet(context.Background(), "key", &Metadata{})
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	_, err = sink.Write([]byte("value"))
	require.Error(t, err)
}

func TestNullStore_WriteAfterAbortFails(t *testing.T) {
	store := newNullStore()

	sink, err := store.BeginSet(context.Background(), "key", &Metadata{})
	require.NoError(t, err)
	require.NoError(t, sink.Abort())

	_, err = sink.Write([]byte("value"))
	require.Error(t, err)
}
