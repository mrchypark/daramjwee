package daramjwee_test

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

func TestNew_RejectsCopyAndTruncateFilestoreAsTierZero(t *testing.T) {
	tier, err := filestore.New(t.TempDir(), log.NewNopLogger(), filestore.WithCopyAndTruncate())
	require.NoError(t, err)

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(tier))
	require.Error(t, err)
	require.Contains(t, err.Error(), "tier 0 does not support stream-through publish semantics")
	require.Nil(t, cache)
}
