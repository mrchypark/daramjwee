package daramjwee_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestStore_BeginSetKeepsCommittedValueVisibleUntilClose(t *testing.T) {
	type factory struct {
		name string
		new  func(t *testing.T) daramjwee.Store
	}

	cases := []factory{
		{
			name: "memstore",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				return memstore.New(0, nil)
			},
		},
		{
			name: "filestore-rename",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				store, err := filestore.New(t.TempDir(), log.NewNopLogger())
				require.NoError(t, err)
				return store
			},
		},
		{
			name: "filestore-copywrite",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				store, err := filestore.New(t.TempDir(), log.NewNopLogger(), filestore.WithCopyWrite())
				require.NoError(t, err)
				return store
			},
		},
		{
			name: "objectstore",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				return objectstore.New(objstore.NewInMemBucket(), log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.new(t)

			seed, err := store.BeginSet(ctx, "contract-key", &daramjwee.Metadata{CacheTag: "v1"})
			require.NoError(t, err)
			_, err = seed.Write([]byte("old"))
			require.NoError(t, err)
			require.NoError(t, seed.Close())

			pending, err := store.BeginSet(ctx, "contract-key", &daramjwee.Metadata{CacheTag: "v2"})
			require.NoError(t, err)
			_, err = pending.Write([]byte("new"))
			require.NoError(t, err)

			reader, meta, err := store.GetStream(ctx, "contract-key")
			require.NoError(t, err)
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, "old", string(body))
			require.Equal(t, "v1", meta.CacheTag)

			statMeta, err := store.Stat(ctx, "contract-key")
			require.NoError(t, err)
			require.Equal(t, "v1", statMeta.CacheTag)

			require.NoError(t, pending.Abort())

			reader, meta, err = store.GetStream(ctx, "contract-key")
			require.NoError(t, err)
			body, err = io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, "old", string(body))
			require.Equal(t, "v1", meta.CacheTag)
		})
	}
}

func TestStore_DeleteDoesNotWaitForPendingBeginSet(t *testing.T) {
	type factory struct {
		name string
		new  func(t *testing.T) daramjwee.Store
	}

	cases := []factory{
		{
			name: "memstore",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				return memstore.New(0, nil)
			},
		},
		{
			name: "filestore-rename",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				store, err := filestore.New(t.TempDir(), log.NewNopLogger())
				require.NoError(t, err)
				return store
			},
		},
		{
			name: "filestore-copywrite",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				store, err := filestore.New(t.TempDir(), log.NewNopLogger(), filestore.WithCopyWrite())
				require.NoError(t, err)
				return store
			},
		},
		{
			name: "objectstore",
			new: func(t *testing.T) daramjwee.Store {
				t.Helper()
				return objectstore.New(objstore.NewInMemBucket(), log.NewNopLogger(), objectstore.WithDir(t.TempDir()))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.new(t)

			seed, err := store.BeginSet(ctx, "contract-delete-key", &daramjwee.Metadata{CacheTag: "seed"})
			require.NoError(t, err)
			_, err = seed.Write([]byte("seed"))
			require.NoError(t, err)
			require.NoError(t, seed.Close())

			pending, err := store.BeginSet(ctx, "contract-delete-key", &daramjwee.Metadata{CacheTag: "pending"})
			require.NoError(t, err)
			_, err = pending.Write([]byte("pending"))
			require.NoError(t, err)

			deleteDone := make(chan error, 1)
			go func() {
				deleteDone <- store.Delete(ctx, "contract-delete-key")
			}()

			select {
			case err := <-deleteDone:
				require.NoError(t, err)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("delete blocked on pending BeginSet")
			}

			require.NoError(t, pending.Abort())
		})
	}
}
