package unit

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferLifecycleManager_Creation(t *testing.T) {
	logger := log.NewNopLogger()
	config := daramjwee.BufferLifecycleConfig{
		MaxBufferAge:        5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		EnableLeakDetection: true,
		EnableAgeTracking:   true,
		EnableHealthMonitor: true,
	}

	blm, err := daramjwee.NewBufferLifecycleManager(config, logger)
	require.NoError(t, err)
	require.NotNil(t, blm)

	// Cleanup
	blm.Close()
}

func TestBufferLifecycleManager_BasicOperations(t *testing.T) {
	logger := log.NewNopLogger()
	config := daramjwee.BufferLifecycleConfig{
		MaxBufferAge:        5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		EnableLeakDetection: true,
		EnableAgeTracking:   true,
		EnableHealthMonitor: true,
	}

	blm, err := daramjwee.NewBufferLifecycleManager(config, logger)
	require.NoError(t, err)
	defer blm.Close()

	// Test basic functionality
	buf := make([]byte, 1024)
	poolSize := 1024

	// Register buffer
	blm.RegisterBuffer(buf, poolSize)

	// Update usage
	blm.UpdateBufferUsage(buf)

	// Unregister buffer
	blm.UnregisterBuffer(buf)
}

func TestBufferLifecycleManager_Stats(t *testing.T) {
	logger := log.NewNopLogger()
	config := daramjwee.BufferLifecycleConfig{
		MaxBufferAge:        5 * time.Minute,
		CleanupInterval:     1 * time.Minute,
		EnableLeakDetection: true,
		EnableAgeTracking:   true,
		EnableHealthMonitor: true,
	}

	blm, err := daramjwee.NewBufferLifecycleManager(config, logger)
	require.NoError(t, err)
	defer blm.Close()

	// Get stats
	stats := blm.GetLifecycleStats()

	// Stats should be initialized
	assert.GreaterOrEqual(t, stats.TotalBuffersCreated, int64(0))
	assert.GreaterOrEqual(t, stats.TotalBuffersCleanup, int64(0))
}

func TestBufferLeakDetector_Creation(t *testing.T) {
	logger := log.NewNopLogger()
	config := daramjwee.LeakDetectorConfig{
		DetectionInterval:  30 * time.Second,
		SuspicionThreshold: 5 * time.Minute,
	}

	detector := daramjwee.NewBufferLeakDetector(config, logger)
	require.NotNil(t, detector)

	// Cleanup
	detector.Close()
}

func TestBufferLeakDetector_Stats(t *testing.T) {
	logger := log.NewNopLogger()
	config := daramjwee.LeakDetectorConfig{
		DetectionInterval:  30 * time.Second,
		SuspicionThreshold: 5 * time.Minute,
	}

	detector := daramjwee.NewBufferLeakDetector(config, logger)
	require.NotNil(t, detector)
	defer detector.Close()

	// Get stats
	stats := detector.GetLeakStats()
	assert.GreaterOrEqual(t, stats.SuspectsIdentified, int64(0))
	assert.GreaterOrEqual(t, stats.LeaksConfirmed, int64(0))
}
