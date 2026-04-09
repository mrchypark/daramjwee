package filestore

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
)

func FuzzFileStoreSequentialOperations(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{7, 6, 5, 4, 3, 2, 1, 0})

	f.Fuzz(func(t *testing.T, ops []byte) {
		store, err := New(t.TempDir(), log.NewNopLogger())
		if err != nil {
			t.Fatalf("new filestore: %v", err)
		}
		ctx := context.Background()
		keys := []string{"fkey-0", "nested/fkey-1", "space key 2"}
		expected := map[string]fileExpectation{}

		for i, op := range ops {
			key := keys[int(op)%len(keys)]
			value := fmt.Sprintf("value-%d-%d", i, op)
			meta := &daramjwee.Metadata{CacheTag: value}

			switch op % 5 {
			case 0:
				sink, err := store.BeginSet(ctx, key, meta)
				if err != nil {
					t.Fatalf("beginset(%q): %v", key, err)
				}
				if _, err := sink.Write([]byte(value)); err != nil {
					t.Fatalf("write(%q): %v", key, err)
				}
				if err := sink.Close(); err != nil {
					t.Fatalf("close(%q): %v", key, err)
				}
				expected[key] = fileExpectation{value: value, meta: *meta}
			case 1:
				sink, err := store.BeginSet(ctx, key, meta)
				if err != nil {
					t.Fatalf("beginset-abort(%q): %v", key, err)
				}
				if _, err := sink.Write([]byte(value)); err != nil {
					t.Fatalf("write-abort(%q): %v", key, err)
				}
				if err := sink.Abort(); err != nil {
					t.Fatalf("abort(%q): %v", key, err)
				}
			case 2:
				got, gotMeta, err := readFileStore(ctx, store, key)
				want, ok := expected[key]
				if !ok {
					if err != daramjwee.ErrNotFound {
						t.Fatalf("get(%q): expected not found, got value=%q meta=%+v err=%v", key, got, gotMeta, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("get(%q): %v", key, err)
				}
				if got != want.value || gotMeta == nil || *gotMeta != want.meta {
					t.Fatalf("get(%q): got value=%q meta=%+v want value=%q meta=%+v", key, got, gotMeta, want.value, want.meta)
				}
			case 3:
				gotMeta, err := store.Stat(ctx, key)
				want, ok := expected[key]
				if !ok {
					if err != daramjwee.ErrNotFound {
						t.Fatalf("stat(%q): expected not found, got meta=%+v err=%v", key, gotMeta, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("stat(%q): %v", key, err)
				}
				if gotMeta == nil || *gotMeta != want.meta {
					t.Fatalf("stat(%q): got meta=%+v want meta=%+v", key, gotMeta, want.meta)
				}
			case 4:
				if err := store.Delete(ctx, key); err != nil {
					t.Fatalf("delete(%q): %v", key, err)
				}
				delete(expected, key)
			}
		}
	})
}

type fileExpectation struct {
	value string
	meta  daramjwee.Metadata
}

func readFileStore(ctx context.Context, store *FileStore, key string) (string, *daramjwee.Metadata, error) {
	reader, meta, err := store.GetStream(ctx, key)
	if err != nil {
		return "", nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, err
	}
	return string(body), meta, nil
}
