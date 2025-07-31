#!/bin/bash

# Fix test imports script

echo "Fixing test package declarations and imports..."

# Fix unit tests
for file in tests/unit/*.go; do
    if [ -f "$file" ]; then
        echo "Processing $file"
        
        # Change package declaration to unit
        sed -i '' 's/^package daramjwee$/package unit/' "$file"
        
        # Add daramjwee import if not present
        if ! grep -q 'github.com/mrchypark/daramjwee' "$file"; then
            # Find the import block and add daramjwee import
            sed -i '' '/^import (/a\
\	"github.com/mrchypark/daramjwee"
' "$file"
        fi
        
        # Fix type references
        sed -i '' 's/\*Metadata/\*daramjwee.Metadata/g' "$file"
        sed -i '' 's/\*FetchResult/\*daramjwee.FetchResult/g' "$file"
        sed -i '' 's/&Metadata{/\&daramjwee.Metadata{/g' "$file"
        sed -i '' 's/&FetchResult{/\&daramjwee.FetchResult{/g' "$file"
        sed -i '' 's/BufferPoolConfig/daramjwee.BufferPoolConfig/g' "$file"
        sed -i '' 's/ErrNotFound/daramjwee.ErrNotFound/g' "$file"
        sed -i '' 's/ErrNotModified/daramjwee.ErrNotModified/g' "$file"
        sed -i '' 's/Cache/daramjwee.Cache/g' "$file"
        sed -i '' 's/Store/daramjwee.Store/g' "$file"
        sed -i '' 's/Fetcher/daramjwee.Fetcher/g' "$file"
        sed -i '' 's/Option/daramjwee.Option/g' "$file"
        sed -i '' 's/DaramjweeCache/daramjwee.DaramjweeCache/g' "$file"
        
        # Fix function calls
        sed -i '' 's/New(/daramjwee.New(/g' "$file"
        sed -i '' 's/WithHotStore(/daramjwee.WithHotStore(/g' "$file"
        sed -i '' 's/WithColdStore(/daramjwee.WithColdStore(/g' "$file"
        sed -i '' 's/WithLogger(/daramjwee.WithLogger(/g' "$file"
        sed -i '' 's/WithBufferPool(/daramjwee.WithBufferPool(/g' "$file"
    fi
done

# Fix integration tests
for file in tests/integration/*.go; do
    if [ -f "$file" ]; then
        echo "Processing $file"
        
        # Change package declaration to integration
        sed -i '' 's/^package daramjwee$/package integration/' "$file"
        
        # Add daramjwee import if not present
        if ! grep -q 'github.com/mrchypark/daramjwee' "$file"; then
            sed -i '' '/^import (/a\
\	"github.com/mrchypark/daramjwee"
' "$file"
        fi
        
        # Fix type references (same as unit tests)
        sed -i '' 's/\*Metadata/\*daramjwee.Metadata/g' "$file"
        sed -i '' 's/\*FetchResult/\*daramjwee.FetchResult/g' "$file"
        sed -i '' 's/&Metadata{/\&daramjwee.Metadata{/g' "$file"
        sed -i '' 's/&FetchResult{/\&daramjwee.FetchResult{/g' "$file"
        sed -i '' 's/BufferPoolConfig/daramjwee.BufferPoolConfig/g' "$file"
        sed -i '' 's/ErrNotFound/daramjwee.ErrNotFound/g' "$file"
        sed -i '' 's/ErrNotModified/daramjwee.ErrNotModified/g' "$file"
        sed -i '' 's/Cache/daramjwee.Cache/g' "$file"
        sed -i '' 's/Store/daramjwee.Store/g' "$file"
        sed -i '' 's/Fetcher/daramjwee.Fetcher/g' "$file"
        sed -i '' 's/Option/daramjwee.Option/g' "$file"
        sed -i '' 's/DaramjweeCache/daramjwee.DaramjweeCache/g' "$file"
        
        # Fix function calls
        sed -i '' 's/New(/daramjwee.New(/g' "$file"
        sed -i '' 's/WithHotStore(/daramjwee.WithHotStore(/g' "$file"
        sed -i '' 's/WithColdStore(/daramjwee.WithColdStore(/g' "$file"
        sed -i '' 's/WithLogger(/daramjwee.WithLogger(/g' "$file"
        sed -i '' 's/WithBufferPool(/daramjwee.WithBufferPool(/g' "$file"
    fi
done

# Fix benchmark tests
for file in tests/benchmark/*.go; do
    if [ -f "$file" ]; then
        echo "Processing $file"
        
        # Change package declaration to benchmark
        sed -i '' 's/^package daramjwee$/package benchmark/' "$file"
        
        # Add daramjwee import if not present
        if ! grep -q 'github.com/mrchypark/daramjwee' "$file"; then
            sed -i '' '/^import (/a\
\	"github.com/mrchypark/daramjwee"
' "$file"
        fi
        
        # Fix type references (same as unit tests)
        sed -i '' 's/\*Metadata/\*daramjwee.Metadata/g' "$file"
        sed -i '' 's/\*FetchResult/\*daramjwee.FetchResult/g' "$file"
        sed -i '' 's/&Metadata{/\&daramjwee.Metadata{/g' "$file"
        sed -i '' 's/&FetchResult{/\&daramjwee.FetchResult{/g' "$file"
        sed -i '' 's/BufferPoolConfig/daramjwee.BufferPoolConfig/g' "$file"
        sed -i '' 's/ErrNotFound/daramjwee.ErrNotFound/g' "$file"
        sed -i '' 's/ErrNotModified/daramjwee.ErrNotModified/g' "$file"
        sed -i '' 's/Cache/daramjwee.Cache/g' "$file"
        sed -i '' 's/Store/daramjwee.Store/g' "$file"
        sed -i '' 's/Fetcher/daramjwee.Fetcher/g' "$file"
        sed -i '' 's/Option/daramjwee.Option/g' "$file"
        sed -i '' 's/DaramjweeCache/daramjwee.DaramjweeCache/g' "$file"
        
        # Fix function calls
        sed -i '' 's/New(/daramjwee.New(/g' "$file"
        sed -i '' 's/WithHotStore(/daramjwee.WithHotStore(/g' "$file"
        sed -i '' 's/WithColdStore(/daramjwee.WithColdStore(/g' "$file"
        sed -i '' 's/WithLogger(/daramjwee.WithLogger(/g' "$file"
        sed -i '' 's/WithBufferPool(/daramjwee.WithBufferPool(/g' "$file"
    fi
done

# Fix stress tests
for file in tests/stress/*.go; do
    if [ -f "$file" ]; then
        echo "Processing $file"
        
        # Change package declaration to stress
        sed -i '' 's/^package daramjwee$/package stress/' "$file"
        
        # Add daramjwee import if not present
        if ! grep -q 'github.com/mrchypark/daramjwee' "$file"; then
            sed -i '' '/^import (/a\
\	"github.com/mrchypark/daramjwee"
' "$file"
        fi
        
        # Fix type references (same as unit tests)
        sed -i '' 's/\*Metadata/\*daramjwee.Metadata/g' "$file"
        sed -i '' 's/\*FetchResult/\*daramjwee.FetchResult/g' "$file"
        sed -i '' 's/&Metadata{/\&daramjwee.Metadata{/g' "$file"
        sed -i '' 's/&FetchResult{/\&daramjwee.FetchResult{/g' "$file"
        sed -i '' 's/BufferPoolConfig/daramjwee.BufferPoolConfig/g' "$file"
        sed -i '' 's/ErrNotFound/daramjwee.ErrNotFound/g' "$file"
        sed -i '' 's/ErrNotModified/daramjwee.ErrNotModified/g' "$file"
        sed -i '' 's/Cache/daramjwee.Cache/g' "$file"
        sed -i '' 's/Store/daramjwee.Store/g' "$file"
        sed -i '' 's/Fetcher/daramjwee.Fetcher/g' "$file"
        sed -i '' 's/Option/daramjwee.Option/g' "$file"
        sed -i '' 's/DaramjweeCache/daramjwee.DaramjweeCache/g' "$file"
        
        # Fix function calls
        sed -i '' 's/New(/daramjwee.New(/g' "$file"
        sed -i '' 's/WithHotStore(/daramjwee.WithHotStore(/g' "$file"
        sed -i '' 's/WithColdStore(/daramjwee.WithColdStore(/g' "$file"
        sed -i '' 's/WithLogger(/daramjwee.WithLogger(/g' "$file"
        sed -i '' 's/WithBufferPool(/daramjwee.WithBufferPool(/g' "$file"
    fi
done

echo "Test import fixes completed!"