package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

func FuzzGenericCacheSequentialStringOps(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{3, 2, 1, 0, 9, 8, 7, 6})

	f.Fuzz(func(t *testing.T, ops []byte) {
		base, err := daramjwee.New(
			nil,
			daramjwee.WithTiers(memstore.New(0, policy.NewLRU())),
			daramjwee.WithOpTimeout(2*time.Second),
			daramjwee.WithFreshness(time.Hour, 0),
		)
		if err != nil {
			t.Fatalf("new cache: %v", err)
		}
		defer base.Close()

		cache := NewGeneric[string](base)
		ctx := context.Background()
		keys := []string{"gkey-0", "gkey-1", "gkey-2"}
		expected := map[string]string{}

		for i, op := range ops {
			key := keys[int(op)%len(keys)]
			value := fmt.Sprintf("value-%d-%d", i, op)

			switch op % 4 {
			case 0:
				if err := cache.Set(ctx, key, value, &daramjwee.Metadata{CacheTag: value}); err != nil {
					t.Fatalf("set(%q): %v", key, err)
				}
				expected[key] = value
			case 1:
				got, err := cache.Get(ctx, key, func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
					return value, &daramjwee.Metadata{CacheTag: value}, nil
				})
				if err != nil {
					t.Fatalf("get(%q): %v", key, err)
				}
				want, ok := expected[key]
				if !ok {
					want = value
					expected[key] = value
				}
				if got != want {
					t.Fatalf("get(%q): got %q want %q", key, got, want)
				}
			case 2:
				got, err := cache.GetOrSet(ctx, key, func() (string, *daramjwee.Metadata, error) {
					return value, &daramjwee.Metadata{CacheTag: value}, nil
				})
				if err != nil {
					t.Fatalf("getorset(%q): %v", key, err)
				}
				want, ok := expected[key]
				if !ok {
					want = value
					expected[key] = value
				}
				if got != want {
					t.Fatalf("getorset(%q): got %q want %q", key, got, want)
				}
			case 3:
				if err := cache.Delete(ctx, key); err != nil {
					t.Fatalf("delete(%q): %v", key, err)
				}
				delete(expected, key)
			}
		}

		for _, key := range keys {
			got, err := cache.Get(ctx, key, func(_ context.Context, _ *daramjwee.Metadata) (string, *daramjwee.Metadata, error) {
				return "fallback", &daramjwee.Metadata{CacheTag: "fallback"}, nil
			})
			want, ok := expected[key]
			if !ok {
				want = "fallback"
			}
			if err != nil {
				t.Fatalf("final get(%q): %v", key, err)
			}
			if got != want {
				t.Fatalf("final get(%q): got %q want %q", key, got, want)
			}
		}
	})
}
