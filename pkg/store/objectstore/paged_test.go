package objectstore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_PagedReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithWholeObjectThreshold(16),
		WithPageSize(8),
	)

	payload := bytes.Repeat([]byte("0123456789abcdef"), 32)
	writer, err := store.BeginSet(ctx, "paged-stream", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "paged-stream")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, payload, body)
	assert.Equal(t, "v1", meta.ETag)
}

func TestStore_NonPositivePageSizeFallsBackToDefaultForPagedReads(t *testing.T) {
	ctx := context.Background()
	store := New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		WithWholeObjectThreshold(4),
		WithPageSize(0),
	)

	payload := bytes.Repeat([]byte("abcd"), 128)
	writer, err := store.BeginSet(ctx, "fallback-page-size", &daramjwee.Metadata{ETag: "v1"})
	require.NoError(t, err)
	_, err = writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	stream, meta, err := store.GetStream(ctx, "fallback-page-size")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, payload, body)
	assert.Equal(t, "v1", meta.ETag)
	assert.Positive(t, int(store.pageSize))
}
