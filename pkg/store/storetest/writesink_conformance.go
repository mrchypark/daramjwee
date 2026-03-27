package storetest

import (
	"context"
	"io"
	"testing"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RunWriteSinkConformance(t *testing.T, factory func(t *testing.T) daramjwee.Store) {
	t.Helper()

	t.Run("ClosePublishes", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "publish-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("value"))
		require.NoError(t, err)
		require.NoError(t, sink.Close())

		reader, meta, err := store.GetStream(context.Background(), "publish-key")
		require.NoError(t, err)
		defer reader.Close()

		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), body)
		require.NotNil(t, meta)
		assert.Equal(t, "v1", meta.ETag)
	})

	t.Run("AbortDiscards", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "abort-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("partial"))
		require.NoError(t, err)
		require.NoError(t, sink.Abort())

		_, _, err = store.GetStream(context.Background(), "abort-key")
		assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	})

	t.Run("AbortThenCloseDoesNotPublish", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "abort-close-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("partial"))
		require.NoError(t, err)
		require.NoError(t, sink.Abort())
		require.NoError(t, sink.Close())

		_, _, err = store.GetStream(context.Background(), "abort-close-key")
		assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	})

	t.Run("CloseThenAbortDoesNotUnpublish", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "close-abort-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("value"))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
		require.NoError(t, sink.Abort())

		reader, _, err := store.GetStream(context.Background(), "close-abort-key")
		require.NoError(t, err)
		defer reader.Close()

		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), body)
	})

	t.Run("RepeatedTerminalCallsAreSafe", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "repeat-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		_, err = sink.Write([]byte("value"))
		require.NoError(t, err)
		require.NoError(t, sink.Close())
		require.NoError(t, sink.Close())
		require.NoError(t, sink.Abort())
	})

	t.Run("WritesFailAfterAbort", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "write-after-abort-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		require.NoError(t, sink.Abort())

		_, err = sink.Write([]byte("value"))
		require.Error(t, err)
	})

	t.Run("WritesFailAfterClose", func(t *testing.T) {
		store := factory(t)

		sink, err := store.BeginSet(context.Background(), "write-after-close-key", &daramjwee.Metadata{ETag: "v1"})
		require.NoError(t, err)
		require.NoError(t, sink.Close())

		_, err = sink.Write([]byte("value"))
		require.Error(t, err)
	})
}
