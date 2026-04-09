package daramjwee_test

import (
	"context"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
)

func FuzzCacheSequentialOperations(f *testing.F) {
	f.Add([]byte{0, 0x01, 0x02, 1, 0x03, 2, 3, 0x04})
	f.Add([]byte{2, 0xaa, 0xbb, 0, 0x10, 0x11, 1})
	f.Add([]byte{3, 0, 1, 2, 3, 4, 5})

	f.Fuzz(func(t *testing.T, ops []byte) {
		hot := newMockStore()
		cache, err := daramjwee.New(
			nil,
			daramjwee.WithTiers(hot),
			daramjwee.WithOpTimeout(2*time.Second),
			daramjwee.WithFreshness(time.Hour, 0),
		)
		if err != nil {
			t.Fatalf("new cache: %v", err)
		}
		defer cache.Close()

		ctx := context.Background()
		expected := map[string]string{}
		keys := []string{"fuzz-key-0", "fuzz-key-1", "fuzz-key-2"}

		for i, op := range ops {
			key := keys[int(op)%len(keys)]
			switch op % 4 {
			case 0:
				value := hex.EncodeToString([]byte{byte(i), op, byte(len(ops))})
				writer, err := cache.Set(ctx, key, &daramjwee.Metadata{CacheTag: value})
				if err != nil {
					t.Fatalf("set(%q) begin: %v", key, err)
				}
				if _, err := writer.Write([]byte(value)); err != nil {
					t.Fatalf("set(%q) write: %v", key, err)
				}
				if err := writer.Close(); err != nil {
					t.Fatalf("set(%q) close: %v", key, err)
				}
				expected[key] = value
			case 1:
				if err := cache.Delete(ctx, key); err != nil {
					t.Fatalf("delete(%q): %v", key, err)
				}
				delete(expected, key)
			case 2:
				got, meta, err := readCurrentValue(ctx, hot, key)
				want, ok := expected[key]
				if !ok {
					if err != daramjwee.ErrNotFound {
						t.Fatalf("get(%q): expected not found, got value=%q meta=%v err=%v", key, got, meta, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("get(%q): %v", key, err)
				}
				if got != want {
					t.Fatalf("get(%q): got %q want %q", key, got, want)
				}
				if meta == nil || meta.CacheTag != want {
					t.Fatalf("get(%q): unexpected metadata %+v want cache tag %q", key, meta, want)
				}
			case 3:
				value := "fetch-" + hex.EncodeToString([]byte{op, byte(i)})
				resp, err := cache.Get(ctx, key, daramjwee.GetRequest{}, sequentialFuzzFetcher{
					value: value,
					tag:   value,
				})
				if err != nil {
					t.Fatalf("cache get(%q): %v", key, err)
				}
				body, err := io.ReadAll(resp)
				if err != nil {
					t.Fatalf("cache get read(%q): %v", key, err)
				}
				if err := resp.Close(); err != nil {
					t.Fatalf("cache get close(%q): %v", key, err)
				}
				if string(body) == "" {
					t.Fatalf("cache get(%q): empty body", key)
				}
				expected[key] = string(body)
			}
		}

		for _, key := range keys {
			got, meta, err := readCurrentValue(ctx, hot, key)
			want, ok := expected[key]
			if !ok {
				if err != daramjwee.ErrNotFound {
					t.Fatalf("final state %q: expected not found, got value=%q meta=%v err=%v", key, got, meta, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("final state %q: %v", key, err)
			}
			if got != want {
				t.Fatalf("final state %q: got %q want %q", key, got, want)
			}
			if meta == nil || meta.CacheTag != want {
				t.Fatalf("final state %q: unexpected metadata %+v want cache tag %q", key, meta, want)
			}
		}
	})
}

type sequentialFuzzFetcher struct {
	value string
	tag   string
}

func (f sequentialFuzzFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(io.Reader(io.LimitReader(&stringReader{s: f.value}, int64(len(f.value))))),
		Metadata: &daramjwee.Metadata{CacheTag: f.tag},
	}, nil
}

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

func readCurrentValue(ctx context.Context, store *mockStore, key string) (string, *daramjwee.Metadata, error) {
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
