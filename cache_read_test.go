package daramjwee

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestHandleConditionalLowerTierPromotionError_CancelsOnGenericPromotionFailure(t *testing.T) {
	cache := &DaramjweeCache{
		logger: log.NewNopLogger(),
	}
	canceled := false

	resp := cache.handleConditionalLowerTierPromotionError(
		"key",
		1,
		errors.New("promotion failed"),
		io.NopCloser(&noopReader{}),
		&Metadata{CacheTag: "cache-v1"},
		func() { canceled = true },
	)

	require.True(t, canceled)
	require.Equal(t, GetStatusNotModified, resp.Status)
	require.Nil(t, resp.Body)
	require.Equal(t, "cache-v1", resp.Metadata.CacheTag)
}

func TestFetchFromOriginRejectsNilResult(t *testing.T) {
	cache := &DaramjweeCache{}

	_, err := cache.fetchFromOrigin(context.Background(), nilResultFetcher{}, nil)

	require.ErrorContains(t, err, "fetcher returned nil result")
}

func TestFetchFromOriginRejectsNilBody(t *testing.T) {
	cache := &DaramjweeCache{}

	_, err := cache.fetchFromOrigin(context.Background(), nilBodyFetcher{}, nil)

	require.ErrorContains(t, err, "fetcher returned nil body")
}

type noopReader struct{}

func (noopReader) Read(p []byte) (int, error) { return 0, io.EOF }

type nilResultFetcher struct{}

func (nilResultFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return nil, nil
}

type nilBodyFetcher struct{}

func (nilBodyFetcher) Fetch(context.Context, *Metadata) (*FetchResult, error) {
	return &FetchResult{Metadata: &Metadata{}}, nil
}
