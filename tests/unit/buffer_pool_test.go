package unit

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveBufferPool_BasicOperations(t *testing.T) {
	config := daramjwee.BufferPoolConfig{
		Enabled:           true,
		DefaultBufferSize: 32 * 1024,
		MaxBufferSize:     512 * 1024,
		MinBufferSize:     4 * 1024,
	}

	logger := log.NewNopLogger()
	pool, err := daramjwee.NewAdaptiveBufferPool(config, logger)
	require.NoError(t, err)
	require.NotNil(t, pool)

	// Test Get operation
	buf := pool.Get(16 * 1024)
	assert.True(t, len(buf) >= 16*1024)

	// Test Put operation
	pool.Put(buf)
}
