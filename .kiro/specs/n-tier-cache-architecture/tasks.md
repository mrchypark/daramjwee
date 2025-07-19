# Implementation Plan

- [x] 1. Update configuration structure and validation
  - Add Stores []Store field to Config struct
  - Implement configuration validation logic for new field
  - Add validation to prevent mixing legacy and new configuration options
  - _Requirements: 6.1, 6.2, 8.1, 8.2, 8.3, 8.4_

- [x] 2. Implement new WithStores configuration option
  - Create WithStores(stores ...Store) Option function
  - Add validation for empty stores slice and nil stores
  - Implement error handling with descriptive ConfigError messages
  - Write unit tests for WithStores option validation
  - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 3. Add backward compatibility for legacy configuration options
  - Modify WithHotStore to detect conflicts with WithStores
  - Modify WithColdStore to detect conflicts with WithStores
  - Implement automatic conversion from legacy fields to Stores slice
  - Write unit tests for backward compatibility scenarios
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 4. Refactor DaramjweeCache struct to use Stores slice
  - Replace HotStore and ColdStore fields with Stores []Store
  - Update cache constructor to use new configuration structure
  - Ensure all existing functionality works with new structure
  - Write unit tests for struct initialization
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 5. Implement sequential lookup algorithm in Get method
  - Replace existing hot/cold/miss logic with loop-based sequential lookup
  - Add tier-specific logging for cache hits and misses
  - Implement proper error handling for store operation failures
  - Write unit tests for sequential lookup behavior
  - _Requirements: 2.1, 2.5, 7.1, 7.2_

- [x] 6. Implement cache hit handling for primary tier (stores[0])
  - Create handlePrimaryHit method for stores[0] hits
  - Maintain existing staleness detection and background refresh logic
  - Ensure negative cache entry handling works correctly
  - Write unit tests for primary tier hit scenarios
  - _Requirements: 2.1, 7.1, 7.2_

- [x] 7. Implement promotion logic for lower tier hits
  - Create handleTierHit method for stores[i] where i > 0
  - Implement metadata copying and timestamp updating for promotion
  - Add tier-specific logging for promotion operations
  - Write unit tests for tier hit detection and metadata handling
  - _Requirements: 2.2, 7.2, 7.3_

- [x] 8. Implement multi-tier promotion using TeeReader
  - Create promoteToUpperTiers method for simultaneous promotion
  - Use io.MultiWriter to write to multiple upper tiers concurrently
  - Integrate BufferPool.TeeReader for efficient streaming
  - Handle promotion failures gracefully without blocking client response
  - Write unit tests for promotion streaming and error handling
  - _Requirements: 2.2, 2.3, 3.1, 3.2, 3.3, 3.4_

- [x] 9. Update Set method to target primary tier
  - Modify Set method to write only to stores[0]
  - Update error handling for missing stores configuration
  - Ensure metadata handling remains consistent
  - Write unit tests for Set method with new architecture
  - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [x] 10. Update Delete method for multi-tier deletion
  - Modify Delete method to iterate through all stores
  - Implement continue-on-failure logic for partial deletion errors
  - Add proper error aggregation and logging
  - Write unit tests for multi-tier deletion scenarios
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 11. Update background refresh operations
  - Ensure ScheduleRefresh targets stores[0] for refreshed data
  - Update scheduleSetToStore to work with new architecture
  - Maintain existing refresh behavior and error handling
  - Write unit tests for background refresh with N-tier architecture
  - _Requirements: 4.2, 7.3_

- [x] 12. Update negative cache handling
  - Ensure handleNegativeCache writes to stores[0]
  - Maintain existing negative cache TTL behavior
  - Update logging to reflect new architecture
  - Write unit tests for negative cache with N-tier setup
  - _Requirements: 4.3_

- [x] 13. Implement comprehensive error handling
  - Create TierError type for tier-specific error reporting
  - Update all store operations to include tier information in errors
  - Implement proper error propagation and logging
  - Write unit tests for various error scenarios
  - _Requirements: 2.5, 5.3, 5.4_

- [x] 14. Add configuration validation and conversion logic
  - Implement Config.validate() method for legacy-to-new conversion
  - Add validation for store slice integrity
  - Implement clear error messages for configuration conflicts
  - Write unit tests for configuration validation scenarios
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 8.4_

- [x] 15. Update multiCloser to handle variable number of closers
  - Modify multiCloser to accept slice of closers instead of varargs
  - Ensure proper resource cleanup for promotion operations
  - Update newMultiCloser function signature and usage
  - Write unit tests for multiCloser with multiple promotion targets
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 16. Write integration tests for N-tier scenarios
  - Test 3-tier setup (memory → file → cloud)
  - Test single-tier configuration
  - Test promotion behavior across multiple tiers
  - Test failure scenarios and recovery
  - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3_

- [x] 17. Write performance benchmarks
  - Benchmark N-tier vs 2-tier performance
  - Profile memory allocation patterns
  - Test concurrent access and promotion scenarios
  - Compare with existing benchmark results
  - _Requirements: 3.1, 3.2, 3.3_

- [x] 18. Write backward compatibility tests
  - Test existing WithHotStore/WithColdStore configuration
  - Verify identical behavior with legacy configuration
  - Test migration from legacy to new configuration
  - Ensure no performance regression
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 19. Update documentation and examples
  - Update code comments to reflect new architecture
  - Create examples for N-tier configuration
  - Document migration path from legacy configuration
  - Update README with new configuration options
  - _Requirements: 8.1, 8.2, 8.3_

- [x] 20. Add comprehensive logging for debugging
  - Add tier-specific logging throughout cache operations
  - Include tier information in all cache operation logs
  - Add promotion success/failure logging
  - Ensure log levels are appropriate for production use
  - _Requirements: 7.4_