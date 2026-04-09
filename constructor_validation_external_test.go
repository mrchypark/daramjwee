package daramjwee_test

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/thanos-io/objstore"
)

func TestNew_RejectsTypedNilTier(t *testing.T) {
	var tier *filestore.FileStore

	require.NotPanics(t, func() {
		cache, err := daramjwee.New(nil, daramjwee.WithTiers(tier))
		require.Error(t, err)
		require.Contains(t, err.Error(), "tier cannot be nil")
		require.Nil(t, cache)
	})
}

func TestNew_RejectsCopyAndTruncateFilestoreAsTierZero(t *testing.T) {
	tier, err := filestore.New(t.TempDir(), log.NewNopLogger(), filestore.WithCopyWrite())
	require.NoError(t, err)

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(tier))
	require.Error(t, err)
	require.Contains(t, err.Error(), "WithCopyWrite mode does not support stream-through publish semantics")
	require.Nil(t, cache)
}

func TestNew_RejectsObjectstoreTierWithInitializationFailure(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "objectstore-dir-file")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	tier := objectstore.New(
		objstore.NewInMemBucket(),
		log.NewNopLogger(),
		objectstore.WithDir(file.Name()),
	)

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(tier))
	require.Error(t, err)
	require.Contains(t, err.Error(), "objectstore")
	require.Nil(t, cache)
}

func TestNew_ExposesDaramjweeCacheWithoutExportingRuntimeFields(t *testing.T) {
	cache, err := daramjwee.New(nil, daramjwee.WithTiers(&optionsCompatibleStore{}))
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	typed, ok := cache.(*daramjwee.DaramjweeCache)
	require.True(t, ok)

	cacheType := reflect.TypeOf(*typed)
	exportedRuntimeFields := []string{
		"Tiers",
		"Logger",
		"Worker",
		"OpTimeout",
		"CloseTimeout",
		"PositiveFreshness",
		"NegativeFreshness",
		"TierFreshnessOverrides",
	}
	for _, fieldName := range exportedRuntimeFields {
		_, exists := cacheType.FieldByName(fieldName)
		require.Falsef(t, exists, "runtime field %s should not be exported", fieldName)
	}
}

type optionsCompatibleStore struct{}

func (optionsCompatibleStore) GetStream(_ context.Context, _ string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (optionsCompatibleStore) BeginSet(_ context.Context, _ string, _ *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return noopWriteSink{}, nil
}

func (optionsCompatibleStore) Delete(_ context.Context, _ string) error {
	return nil
}

func (optionsCompatibleStore) Stat(_ context.Context, _ string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

type noopWriteSink struct{}

func (noopWriteSink) Write(p []byte) (int, error) { return len(p), nil }
func (noopWriteSink) Close() error                { return nil }
func (noopWriteSink) Abort() error                { return nil }
