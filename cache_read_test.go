package daramjwee

import (
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

type noopReader struct{}

func (noopReader) Read(p []byte) (int, error) { return 0, io.EOF }
