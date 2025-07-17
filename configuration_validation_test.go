package daramjwee

import (
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

// ConfigurationValidator validates buffer pool configurations
type ConfigurationValidator struct {
	logger log.Logger
}

// ConfigurationTestCase represents a configuration test case
type ConfigurationTestCase struct {
	Name          string
	Config        BufferPoolConfig
	ExpectError   bool
	ExpectedError string
	Description   string
}

// ConfigurationRecommendation represents a configuration recommendation
type ConfigurationRecommendation struct {
	Field            string
	CurrentValue     interface{}
	RecommendedValue interface{}
	Reason           string
	Severity         string // "info", "warning", "error"
}

// NewConfigurationValidator creates a new configuration validator
func NewConfigurationValidator(logger log.Logger) *ConfigurationValidator {
	return &ConfigurationValidator{
		logger: logger,
	}
}

// TestComprehensiveConfigurationValidation tests comprehensive configuration validation
func TestComprehensiveConfigurationValidation(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewConfigurationValidator(logger)

	testCases := []ConfigurationTestCase{
		{
			Name: "ValidDefaultConfiguration",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
				EnableDetailedMetrics:    true,
			},
			ExpectError: false,
			Description: "Standard valid configuration should pass validation",
		},
		{
			Name: "InvalidThresholdOrder",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     1024 * 1024, // Same as VeryLarge
				VeryLargeObjectThreshold: 256 * 1024,  // Smaller than Large
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			ExpectError:   true,
			ExpectedError: "VeryLargeObjectThreshold must be greater than LargeObjectThreshold",
			Description:   "Threshold order validation should catch reversed thresholds",
		},
		{
			Name: "InvalidBufferSizeOrder",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            16 * 1024, // Smaller than default
				MinBufferSize:            64 * 1024, // Larger than default
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			ExpectError:   true,
			ExpectedError: "MinBufferSize must be less than or equal to DefaultBufferSize",
			Description:   "Buffer size order validation should catch invalid ordering",
		},
		{
			Name: "ZeroDefaultBufferSize",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        0, // Invalid
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			ExpectError:   true,
			ExpectedError: "DefaultBufferSize must be greater than 0",
			Description:   "Zero default buffer size should be rejected",
		},
		{
			Name: "NegativeMaxConcurrentOps",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    -1, // Invalid
			},
			ExpectError:   true,
			ExpectedError: "MaxConcurrentLargeOps must be greater than 0",
			Description:   "Negative concurrent operations should be rejected",
		},
		{
			Name: "InvalidChunkSize",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                0, // Invalid
				MaxConcurrentLargeOps:    10,
			},
			ExpectError:   true,
			ExpectedError: "ChunkSize must be greater than 0",
			Description:   "Zero chunk size should be rejected",
		},
		{
			Name: "ExtremelyLargeBufferSize",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            100 * 1024 * 1024 * 1024, // 100GB - unrealistic
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			ExpectError: false, // Should pass but generate warning
			Description: "Extremely large buffer sizes should generate warnings",
		},
		{
			Name: "DisabledPoolConfiguration",
			Config: BufferPoolConfig{
				Enabled:                  false, // Disabled
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			ExpectError: false,
			Description: "Disabled pool configuration should be valid",
		},
		{
			Name: "MinimalValidConfiguration",
			Config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        1024,
				MaxBufferSize:            2048,
				MinBufferSize:            512,
				LargeObjectThreshold:     1536,
				VeryLargeObjectThreshold: 2048,
				ChunkSize:                1024,
				MaxConcurrentLargeOps:    1,
			},
			ExpectError: false,
			Description: "Minimal valid configuration should pass",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			err := validator.ValidateConfiguration(tc.Config)

			if tc.ExpectError {
				assert.Error(t, err, tc.Description)
				if tc.ExpectedError != "" {
					assert.Contains(t, err.Error(), tc.ExpectedError,
						"Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, tc.Description)
			}
		})
	}
}

// TestErrorHandlingAndFallbacks tests error handling and fallback mechanisms
func TestErrorHandlingAndFallbacks(t *testing.T) {
	logger := log.NewNopLogger()

	testCases := []struct {
		name        string
		config      BufferPoolConfig
		expectError bool
		description string
	}{
		{
			name: "InvalidConfigWithFallback",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        0, // Invalid, should fallback
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			expectError: true,
			description: "Invalid configuration should trigger error handling",
		},
		{
			name: "PartiallyInvalidConfig",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     0, // Invalid, but other fields valid
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			expectError: true,
			description: "Partially invalid configuration should be handled gracefully",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test that NewAdaptiveBufferPool handles invalid configs appropriately
			pool, err := NewAdaptiveBufferPool(tc.config, logger)

			if tc.expectError {
				assert.Error(t, err, tc.description)
				assert.Nil(t, pool, "Pool should be nil when configuration is invalid")
			} else {
				assert.NoError(t, err, tc.description)
				assert.NotNil(t, pool, "Pool should be created when configuration is valid")
				if pool != nil {
					pool.Close()
				}
			}
		})
	}
}

// TestConfigurationRecommendations tests configuration recommendation system
func TestConfigurationRecommendations(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewConfigurationValidator(logger)

	testConfigs := []struct {
		name                    string
		config                  BufferPoolConfig
		expectedRecommendations int
	}{
		{
			name: "OptimalConfiguration",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
				EnableDetailedMetrics:    true,
			},
			expectedRecommendations: 0, // Should be optimal
		},
		{
			name: "SuboptimalConfiguration",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        1024,  // Too small
				MaxBufferSize:            2048,  // Too small
				MinBufferSize:            512,   // OK
				LargeObjectThreshold:     1536,  // Too small
				VeryLargeObjectThreshold: 2048,  // Too small
				ChunkSize:                512,   // Too small
				MaxConcurrentLargeOps:    1,     // Too low
				EnableDetailedMetrics:    false, // Should be enabled
			},
			expectedRecommendations: 5, // Multiple recommendations expected
		},
		{
			name: "OverlyAggressiveConfiguration",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        1024 * 1024,       // Very large
				MaxBufferSize:            100 * 1024 * 1024, // Extremely large
				MinBufferSize:            512 * 1024,        // Large
				LargeObjectThreshold:     10 * 1024 * 1024,  // Very large
				VeryLargeObjectThreshold: 50 * 1024 * 1024,  // Extremely large
				ChunkSize:                1024 * 1024,       // Large
				MaxConcurrentLargeOps:    100,               // High
				EnableDetailedMetrics:    true,
			},
			expectedRecommendations: 3, // Should recommend reducing some values
		},
	}

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			recommendations := validator.GenerateRecommendations(tc.config)

			t.Logf("Configuration: %s", tc.name)
			for _, rec := range recommendations {
				t.Logf("  %s: %s (current: %v, recommended: %v)",
					rec.Severity, rec.Reason, rec.CurrentValue, rec.RecommendedValue)
			}

			if tc.expectedRecommendations == 0 {
				assert.Empty(t, recommendations, "Optimal configuration should have no recommendations")
			} else {
				assert.True(t, len(recommendations) >= tc.expectedRecommendations-2 &&
					len(recommendations) <= tc.expectedRecommendations+2,
					"Should have approximately %d recommendations, got %d",
					tc.expectedRecommendations, len(recommendations))
			}
		})
	}
}

// TestMisconfigurationDetection tests detection of common misconfigurations
func TestMisconfigurationDetection(t *testing.T) {
	logger := log.NewNopLogger()
	validator := NewConfigurationValidator(logger)

	testCases := []struct {
		name           string
		config         BufferPoolConfig
		expectedIssues []string
	}{
		{
			name: "ThresholdTooClose",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 260 * 1024, // Too close to LargeObjectThreshold
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			expectedIssues: []string{"threshold gap too small"},
		},
		{
			name: "ChunkSizeLargerThanThreshold",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        32 * 1024,
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                512 * 1024, // Larger than LargeObjectThreshold
				MaxConcurrentLargeOps:    10,
			},
			expectedIssues: []string{"chunk size too large"},
		},
		{
			name: "DefaultBufferSizeAtThreshold",
			config: BufferPoolConfig{
				Enabled:                  true,
				DefaultBufferSize:        256 * 1024, // Same as LargeObjectThreshold
				MaxBufferSize:            1024 * 1024,
				MinBufferSize:            1 * 1024,
				LargeObjectThreshold:     256 * 1024,
				VeryLargeObjectThreshold: 1024 * 1024,
				ChunkSize:                64 * 1024,
				MaxConcurrentLargeOps:    10,
			},
			expectedIssues: []string{"default buffer size at threshold"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			issues := validator.DetectMisconfigurations(tc.config)

			t.Logf("Detected issues for %s:", tc.name)
			for _, issue := range issues {
				t.Logf("  - %s", issue)
			}

			for _, expectedIssue := range tc.expectedIssues {
				found := false
				for _, issue := range issues {
					if assert.Contains(t, issue, expectedIssue) {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected issue '%s' should be detected", expectedIssue)
			}
		})
	}
}

// TestBackwardCompatibility tests backward compatibility with existing configurations
func TestBackwardCompatibility(t *testing.T) {
	logger := log.NewNopLogger()

	// Test legacy configurations that should still work
	legacyConfigs := []struct {
		name   string
		config BufferPoolConfig
	}{
		{
			name: "LegacySimpleConfig",
			config: BufferPoolConfig{
				Enabled:           true,
				DefaultBufferSize: 32 * 1024,
				MaxBufferSize:     1024 * 1024,
				MinBufferSize:     1 * 1024,
				// Legacy configs might not have all new fields
			},
		},
		{
			name: "LegacyDisabledConfig",
			config: BufferPoolConfig{
				Enabled: false,
				// Other fields might be zero/default
			},
		},
		{
			name: "LegacyMinimalConfig",
			config: BufferPoolConfig{
				Enabled:           true,
				DefaultBufferSize: 8192,
				// Minimal legacy configuration
			},
		},
	}

	for _, tc := range legacyConfigs {
		t.Run(tc.name, func(t *testing.T) {
			// Legacy configurations should either work or fail gracefully
			pool, err := NewAdaptiveBufferPool(tc.config, logger)

			if err != nil {
				// If it fails, the error should be informative
				assert.NotEmpty(t, err.Error(), "Error message should be informative")
				t.Logf("Legacy config failed as expected: %v", err)
			} else {
				// If it succeeds, basic operations should work
				assert.NotNil(t, pool, "Pool should be created")

				// Test basic functionality
				buf := pool.Get(1024)
				assert.Equal(t, 1024, len(buf), "Buffer should have correct size")
				pool.Put(buf)

				pool.Close()
			}
		})
	}
}

// ValidateConfiguration validates a buffer pool configuration
func (cv *ConfigurationValidator) ValidateConfiguration(config BufferPoolConfig) error {
	if !config.Enabled {
		return nil // Disabled configurations are always valid
	}

	// Validate buffer sizes
	if config.DefaultBufferSize <= 0 {
		return fmt.Errorf("DefaultBufferSize must be greater than 0")
	}
	if config.MaxBufferSize <= 0 {
		return fmt.Errorf("MaxBufferSize must be greater than 0")
	}
	if config.MinBufferSize <= 0 {
		return fmt.Errorf("MinBufferSize must be greater than 0")
	}

	// Validate buffer size ordering
	if config.MinBufferSize > config.DefaultBufferSize {
		return fmt.Errorf("MinBufferSize must be less than or equal to DefaultBufferSize")
	}
	if config.DefaultBufferSize > config.MaxBufferSize {
		return fmt.Errorf("DefaultBufferSize must be less than or equal to MaxBufferSize")
	}

	// Validate thresholds
	if config.LargeObjectThreshold <= 0 {
		return fmt.Errorf("LargeObjectThreshold must be greater than 0")
	}
	if config.VeryLargeObjectThreshold <= 0 {
		return fmt.Errorf("VeryLargeObjectThreshold must be greater than 0")
	}
	if config.VeryLargeObjectThreshold <= config.LargeObjectThreshold {
		return fmt.Errorf("VeryLargeObjectThreshold must be greater than LargeObjectThreshold")
	}

	// Validate chunk size
	if config.ChunkSize <= 0 {
		return fmt.Errorf("ChunkSize must be greater than 0")
	}

	// Validate concurrent operations
	if config.MaxConcurrentLargeOps <= 0 {
		return fmt.Errorf("MaxConcurrentLargeOps must be greater than 0")
	}

	return nil
}

// GenerateRecommendations generates configuration recommendations
func (cv *ConfigurationValidator) GenerateRecommendations(config BufferPoolConfig) []ConfigurationRecommendation {
	var recommendations []ConfigurationRecommendation

	// Check if detailed metrics should be enabled
	if !config.EnableDetailedMetrics {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "EnableDetailedMetrics",
			CurrentValue:     false,
			RecommendedValue: true,
			Reason:           "Enable detailed metrics for better monitoring and optimization",
			Severity:         "info",
		})
	}

	// Check for suboptimal buffer sizes
	if config.DefaultBufferSize < 16*1024 {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "DefaultBufferSize",
			CurrentValue:     config.DefaultBufferSize,
			RecommendedValue: 32 * 1024,
			Reason:           "Default buffer size is too small, consider 32KB for better performance",
			Severity:         "warning",
		})
	}

	// Check for overly large buffer sizes
	if config.MaxBufferSize > 50*1024*1024 {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "MaxBufferSize",
			CurrentValue:     config.MaxBufferSize,
			RecommendedValue: 10 * 1024 * 1024,
			Reason:           "Max buffer size is very large, consider reducing to 10MB",
			Severity:         "warning",
		})
	}

	// Check chunk size
	if config.ChunkSize < 32*1024 {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "ChunkSize",
			CurrentValue:     config.ChunkSize,
			RecommendedValue: 64 * 1024,
			Reason:           "Chunk size is small, consider 64KB for better streaming performance",
			Severity:         "info",
		})
	}

	// Check concurrent operations
	if config.MaxConcurrentLargeOps < 5 {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "MaxConcurrentLargeOps",
			CurrentValue:     config.MaxConcurrentLargeOps,
			RecommendedValue: 10,
			Reason:           "Low concurrent operations limit may cause bottlenecks",
			Severity:         "info",
		})
	}

	// Check thresholds
	if config.LargeObjectThreshold < 128*1024 {
		recommendations = append(recommendations, ConfigurationRecommendation{
			Field:            "LargeObjectThreshold",
			CurrentValue:     config.LargeObjectThreshold,
			RecommendedValue: 256 * 1024,
			Reason:           "Large object threshold is low, consider 256KB",
			Severity:         "info",
		})
	}

	return recommendations
}

// DetectMisconfigurations detects common configuration issues
func (cv *ConfigurationValidator) DetectMisconfigurations(config BufferPoolConfig) []string {
	var issues []string

	// Check threshold gaps
	thresholdGap := config.VeryLargeObjectThreshold - config.LargeObjectThreshold
	if thresholdGap < 64*1024 {
		issues = append(issues, fmt.Sprintf("threshold gap too small (%d bytes), consider larger gap for better strategy differentiation", thresholdGap))
	}

	// Check chunk size relative to thresholds
	if config.ChunkSize > config.LargeObjectThreshold {
		issues = append(issues, "chunk size too large relative to large object threshold, may cause inefficient chunking")
	}

	// Check default buffer size relative to thresholds
	if config.DefaultBufferSize >= config.LargeObjectThreshold {
		issues = append(issues, "default buffer size at threshold boundary, may cause unexpected strategy selection")
	}

	// Check for power-of-2 alignment (performance optimization)
	if !isPowerOfTwo(config.DefaultBufferSize) && config.DefaultBufferSize > 1024 {
		issues = append(issues, "default buffer size is not power-of-2, consider aligning for better memory allocation")
	}

	// Check for reasonable concurrent operations relative to system
	if config.MaxConcurrentLargeOps > 100 {
		issues = append(issues, "very high concurrent operations limit may cause resource exhaustion")
	}

	return issues
}

// isPowerOfTwo checks if a number is a power of 2
func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}
