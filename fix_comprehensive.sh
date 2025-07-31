#!/bin/bash

echo "Comprehensive test import fixes..."

# List of types that need daramjwee prefix
TYPES=(
    "Cache" "Store" "Fetcher" "EvictionPolicy" "Locker" "Compressor" "BufferPool"
    "Metadata" "FetchResult" "TierError" "BufferPoolConfig" "BufferPoolStats"
    "BufferPoolStrategy" "ObjectSizeCategory" "CompressionType" "CompressionMetadata"
    "SimpleFetcher" "BufferLifecycleManager" "BufferLifecycleConfig" "BufferMetadata"
    "PoolHealthMonitor" "AdaptiveBufferPool" "ChunkedTeeReader" "DaramjweeCache"
    "StrategyAdaptive" "StrategyPooled" "SizeCategorySmall" "SizeCategoryMedium" "SizeCategoryLarge"
)

# List of functions that need daramjwee prefix
FUNCTIONS=(
    "New" "NewAdaptiveBufferPool" "WithHotStore" "WithColdStore" "WithLogger" 
    "WithBufferPool" "WithEvictionPolicy" "WithCompression"
)

# List of constants/errors that need daramjwee prefix
CONSTANTS=(
    "ErrNotFound" "ErrNotModified" "ErrTierUnavailable"
)

for dir in tests/unit tests/integration tests/benchmark tests/stress; do
    if [ -d "$dir" ]; then
        echo "Processing directory: $dir"
        
        for file in "$dir"/*.go; do
            if [ -f "$file" ]; then
                echo "  Processing file: $file"
                
                # Fix types - but be careful not to double-prefix
                for type in "${TYPES[@]}"; do
                    # Only add prefix if not already prefixed and not in interface/struct definitions
                    sed -i '' "s/\([^a-zA-Z0-9_]\)${type}\([^a-zA-Z0-9_]\)/\1daramjwee.${type}\2/g" "$file"
                    sed -i '' "s/^${type}\([^a-zA-Z0-9_]\)/daramjwee.${type}\1/g" "$file"
                done
                
                # Fix functions
                for func in "${FUNCTIONS[@]}"; do
                    sed -i '' "s/\([^a-zA-Z0-9_]\)${func}(/\1daramjwee.${func}(/g" "$file"
                    sed -i '' "s/^${func}(/daramjwee.${func}(/g" "$file"
                done
                
                # Fix constants/errors
                for const in "${CONSTANTS[@]}"; do
                    sed -i '' "s/\([^a-zA-Z0-9_]\)${const}\([^a-zA-Z0-9_]\)/\1daramjwee.${const}\2/g" "$file"
                    sed -i '' "s/^${const}\([^a-zA-Z0-9_]\)/daramjwee.${const}\1/g" "$file"
                done
                
                # Clean up double prefixes
                sed -i '' 's/daramjwee\.daramjwee\./daramjwee./g' "$file"
                
                # Fix interface implementations - remove daramjwee prefix from interface names in type definitions
                sed -i '' 's/type mock.*daramjwee\.\(Cache\|Store\|Fetcher\)/type mock\1/g' "$file"
                
                # Fix function names that got incorrectly prefixed
                sed -i '' 's/func Test.*daramjwee\./func Test/g' "$file"
                sed -i '' 's/func Benchmark.*daramjwee\./func Benchmark/g' "$file"
                
                # Fix method receivers
                sed -i '' 's/func (.*) .*daramjwee\./func (\1) /g' "$file"
            fi
        done
    fi
done

echo "Comprehensive fixes completed!"