#!/bin/bash
# This script fixes syntax errors in test files.

# List of test files
TEST_FILES=(
    "tests/integration/adaptive_buffer_pool_integration_test.go"
    "tests/benchmark/adaptive_buffer_pool_benchmark_test.go"
    "tests/stress/stress_test.go"
    "tests/stress/concurrent_safety_test.go"
    "tests/unit/cache_test.go"
    "tests/unit/cache_struct_test.go"
)

# Replacements to be made
declare -A REPLACEMENTS=(
    ["newMockStore"]="daramjwee.newMockStore"
    ["WithLargeObjectOptimization"]="daramjwee.WithLargeObjectOptimization"
    ["daramjweeCache\.daramjwee"]="daramjweeCache.Daramjwee"
    ["mockFetcher"]="daramjwee.mockFetcher"
    ["WithBufferPoolMetrics"]="daramjwee.WithBufferPoolMetrics"
    ["BufferPoolStrategy"]="daramjwee.BufferPoolStrategy"
    ["BufferPool"]="daramjwee.BufferPool"
    ["NewAdaptiveBufferPoolImpl"]="daramjwee.NewAdaptiveBufferPoolImpl"
    ["NewDefaultBufferPooldaramjwee"]="daramjwee.NewDefaultBufferPooldaramjwee"
    ["mockStore"]="daramjwee.mockStore"
    ["rand\.daramjwee"]="rand.Daramjwee"
    ["pool\.getChunk"]="pool.GetChunk"
    ["pool\.putChunk"]="pool.PutChunk"
    ["pool\.acquireLargeOpSlot"]="pool.AcquireLargeOpSlot"
    ["pool\.releaseLargeOpSlot"]="pool.ReleaseLargeOpSlot"
    ["errors"]="daramjwee.errors"
    ["ErrCacheableNotFound"]="daramjwee.ErrCacheableNotFound"
    ["WithLogger"]="daramjwee.WithLogger"
)

# Loop through the files and apply replacements
for FILE in "${TEST_FILES[@]}"; do
    if [ -f "$FILE" ]; then
        for OLD in "${!REPLACEMENTS[@]}"; do
            NEW=${REPLACEMENTS[$OLD]}
            sed -i "" "s/$OLD/$NEW/g" "$FILE"
        done
    else
        echo "File not found: $FILE"
    fi
done

echo "Syntax correction finished."