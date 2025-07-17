# Custom Fetcher Implementation Guidelines

## Overview

This document provides comprehensive guidelines for implementing custom Fetcher interfaces for the daramjwee caching library. A Fetcher is responsible for retrieving data from origin sources when cache misses occur or when refresh operations are needed. It supports conditional requests, handles various error scenarios, and integrates with different origin types.

## Fetcher Interface Contract

The Fetcher interface defines a single method that must be implemented:

```go
type Fetcher interface {
    Fetch(ctx context.Context, oldMetadata *Metadata) (*FetchResult, error)
}
```

### Method Implementation Requirements

#### Fetch Method

**Purpose**: Retrieve data from the origin source, supporting conditional requests for optimization.

**Implementation Requirements**:
- Handle conditional requests using `oldMetadata` for ETag-based optimization
- Return appropriate error types for different failure scenarios
- Support context cancellation and timeouts
- Provide streaming access to fetched data
- Return accurate metadata with the fetched data

**Parameters**:
- `ctx`: Context for cancellation and timeout control
- `oldMetadata`: Previously cached metadata for conditional requests (may be nil)

**Returns**:
- `*FetchResult`: Contains data stream and fresh metadata
- `error`: Various error types depending on the fetch outcome

**Error Return Patterns**:
- `ErrNotModified`: Data hasn't changed (conditional request success)
- `ErrCacheableNotFound`: Resource doesn't exist but state should be cached
- `ErrNotFound`: Resource doesn't exist and state should not be cached
- Origin-specific errors: Wrapped errors for other failure conditions

## HTTP Fetcher Implementation

### Basic HTTP Fetcher

```go
package fetcher

import (
    "context"
    "errors"
    "fmt"
    "io"
    "net/http"
    "time"
    
    "github.com/mrchypark/daramjwee"
)

// HTTPFetcher implements Fetcher for HTTP-based origins
type HTTPFetcher struct {
    URL     string
    Client  *http.Client
    Headers map[string]string
}

// NewHTTPFetcher creates a new HTTP fetcher
func NewHTTPFetcher(url string, options ...HTTPOption) *HTTPFetcher {
    fetcher := &HTTPFetcher{
        URL:     url,
        Client:  &http.Client{Timeout: 30 * time.Second},
        Headers: make(map[string]string),
    }
    
    for _, opt := range options {
        opt(fetcher)
    }
    
    return fetcher
}

// Fetch retrieves data from HTTP origin with conditional request support
func (f *HTTPFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    req, err := http.NewRequestWithContext(ctx, "GET", f.URL, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create HTTP request: %w", err)
    }
    
    // Add custom headers
    for key, value := range f.Headers {
        req.Header.Set(key, value)
    }
    
    // Add conditional request headers
    if oldMetadata != nil && oldMetadata.ETag != "" {
        req.Header.Set("If-None-Match", oldMetadata.ETag)
    }
    
    resp, err := f.Client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("HTTP request failed: %w", err)
    }
    
    return f.handleResponse(resp)
}

// handleResponse processes HTTP response and returns appropriate result or error
func (f *HTTPFetcher) handleResponse(resp *http.Response) (*daramjwee.FetchResult, error) {
    switch resp.StatusCode {
    case http.StatusOK:
        // Success - return data and metadata
        metadata := &daramjwee.Metadata{
            ETag:     resp.Header.Get("ETag"),
            CachedAt: time.Now(),
        }
        
        return &daramjwee.FetchResult{
            Body:     resp.Body,
            Metadata: metadata,
        }, nil
        
    case http.StatusNotModified:
        // Conditional request success - data hasn't changed
        resp.Body.Close()
        return nil, daramjwee.ErrNotModified
        
    case http.StatusNotFound:
        // Resource not found - cache this state
        resp.Body.Close()
        return nil, daramjwee.ErrCacheableNotFound
        
    case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
        // Temporary failures - don't cache
        resp.Body.Close()
        return nil, daramjwee.ErrNotFound
        
    default:
        // Other HTTP errors
        resp.Body.Close()
        return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
    }
}

// HTTPOption configures HTTP fetcher behavior
type HTTPOption func(*HTTPFetcher)

// WithTimeout sets request timeout
func WithTimeout(timeout time.Duration) HTTPOption {
    return func(f *HTTPFetcher) {
        f.Client.Timeout = timeout
    }
}

// WithHeader adds custom header
func WithHeader(key, value string) HTTPOption {
    return func(f *HTTPFetcher) {
        f.Headers[key] = value
    }
}

// WithClient sets custom HTTP client
func WithClient(client *http.Client) HTTPOption {
    return func(f *HTTPFetcher) {
        f.Client = client
    }
}
```

### Advanced HTTP Fetcher with Retry Logic

```go
// RetryHTTPFetcher implements HTTP fetcher with retry logic
type RetryHTTPFetcher struct {
    *HTTPFetcher
    MaxRetries int
    BackoffFunc func(attempt int) time.Duration
}

// NewRetryHTTPFetcher creates HTTP fetcher with retry capability
func NewRetryHTTPFetcher(url string, maxRetries int, options ...HTTPOption) *RetryHTTPFetcher {
    return &RetryHTTPFetcher{
        HTTPFetcher: NewHTTPFetcher(url, options...),
        MaxRetries:  maxRetries,
        BackoffFunc: exponentialBackoff,
    }
}

// Fetch implements retry logic for HTTP requests
func (f *RetryHTTPFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    var lastErr error
    
    for attempt := 0; attempt <= f.MaxRetries; attempt++ {
        if attempt > 0 {
            // Wait before retry
            backoff := f.BackoffFunc(attempt)
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(backoff):
            }
        }
        
        result, err := f.HTTPFetcher.Fetch(ctx, oldMetadata)
        if err == nil {
            return result, nil
        }
        
        lastErr = err
        
        // Don't retry certain errors
        if errors.Is(err, daramjwee.ErrNotModified) ||
           errors.Is(err, daramjwee.ErrCacheableNotFound) {
            return nil, err
        }
        
        // Don't retry context cancellation
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            return nil, err
        }
    }
    
    return nil, fmt.Errorf("HTTP fetch failed after %d attempts: %w", f.MaxRetries+1, lastErr)
}

// exponentialBackoff implements exponential backoff strategy
func exponentialBackoff(attempt int) time.Duration {
    base := time.Second
    return base * time.Duration(1<<uint(attempt))
}
```

## Database Fetcher Implementation

### SQL Database Fetcher

```go
package fetcher

import (
    "context"
    "database/sql"
    "fmt"
    "io"
    "strings"
    "time"
    
    "github.com/mrchypark/daramjwee"
)

// SQLFetcher implements Fetcher for SQL database origins
type SQLFetcher struct {
    DB           *sql.DB
    Query        string
    Args         []interface{}
    VersionQuery string  // Query to check data version/timestamp
    VersionArgs  []interface{}
}

// NewSQLFetcher creates a new SQL database fetcher
func NewSQLFetcher(db *sql.DB, query string, args ...interface{}) *SQLFetcher {
    return &SQLFetcher{
        DB:    db,
        Query: query,
        Args:  args,
    }
}

// WithVersionCheck adds version checking for conditional requests
func (f *SQLFetcher) WithVersionCheck(query string, args ...interface{}) *SQLFetcher {
    f.VersionQuery = query
    f.VersionArgs = args
    return f
}

// Fetch retrieves data from SQL database with change detection
func (f *SQLFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // Check if data has changed using version query
    if f.VersionQuery != "" && oldMetadata != nil {
        changed, err := f.hasChanged(ctx, oldMetadata)
        if err != nil {
            return nil, fmt.Errorf("version check failed: %w", err)
        }
        if !changed {
            return nil, daramjwee.ErrNotModified
        }
    }
    
    // Execute main query
    rows, err := f.DB.QueryContext(ctx, f.Query, f.Args...)
    if err != nil {
        if err == sql.ErrNoRows {
            return nil, daramjwee.ErrCacheableNotFound
        }
        return nil, fmt.Errorf("database query failed: %w", err)
    }
    
    // Convert rows to JSON or other format
    data, err := f.rowsToJSON(rows)
    if err != nil {
        return nil, fmt.Errorf("failed to serialize query results: %w", err)
    }
    
    if len(data) == 0 {
        return nil, daramjwee.ErrCacheableNotFound
    }
    
    // Generate ETag from data
    etag := f.generateETag(data)
    
    metadata := &daramjwee.Metadata{
        ETag:     etag,
        CachedAt: time.Now(),
    }
    
    return &daramjwee.FetchResult{
        Body:     io.NopCloser(strings.NewReader(string(data))),
        Metadata: metadata,
    }, nil
}

// hasChanged checks if data has changed since last fetch
func (f *SQLFetcher) hasChanged(ctx context.Context, oldMetadata *daramjwee.Metadata) (bool, error) {
    var currentVersion string
    err := f.DB.QueryRowContext(ctx, f.VersionQuery, f.VersionArgs...).Scan(&currentVersion)
    if err != nil {
        if err == sql.ErrNoRows {
            return true, nil  // Data might have been deleted
        }
        return false, err
    }
    
    return currentVersion != oldMetadata.ETag, nil
}

// rowsToJSON converts SQL rows to JSON format
func (f *SQLFetcher) rowsToJSON(rows *sql.Rows) ([]byte, error) {
    defer rows.Close()
    
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }
    
    var results []map[string]interface{}
    
    for rows.Next() {
        values := make([]interface{}, len(columns))
        valuePtrs := make([]interface{}, len(columns))
        
        for i := range values {
            valuePtrs[i] = &values[i]
        }
        
        if err := rows.Scan(valuePtrs...); err != nil {
            return nil, err
        }
        
        row := make(map[string]interface{})
        for i, col := range columns {
            row[col] = values[i]
        }
        
        results = append(results, row)
    }
    
    if err := rows.Err(); err != nil {
        return nil, err
    }
    
    return json.Marshal(results)
}

// generateETag creates ETag from data
func (f *SQLFetcher) generateETag(data []byte) string {
    hash := sha256.Sum256(data)
    return fmt.Sprintf(`"%x"`, hash)
}
```

### NoSQL Database Fetcher (MongoDB Example)

```go
package fetcher

import (
    "context"
    "fmt"
    "io"
    "strings"
    "time"
    
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "github.com/mrchypark/daramjwee"
)

// MongoFetcher implements Fetcher for MongoDB origins
type MongoFetcher struct {
    Collection *mongo.Collection
    Filter     bson.M
    Options    *options.FindOptions
}

// NewMongoFetcher creates a new MongoDB fetcher
func NewMongoFetcher(collection *mongo.Collection, filter bson.M) *MongoFetcher {
    return &MongoFetcher{
        Collection: collection,
        Filter:     filter,
    }
}

// Fetch retrieves data from MongoDB with change detection
func (f *MongoFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // Check if data has changed using modification time
    if oldMetadata != nil {
        changed, err := f.hasChanged(ctx, oldMetadata.CachedAt)
        if err != nil {
            return nil, fmt.Errorf("change detection failed: %w", err)
        }
        if !changed {
            return nil, daramjwee.ErrNotModified
        }
    }
    
    // Execute query
    cursor, err := f.Collection.Find(ctx, f.Filter, f.Options)
    if err != nil {
        return nil, fmt.Errorf("MongoDB query failed: %w", err)
    }
    defer cursor.Close(ctx)
    
    // Collect results
    var results []bson.M
    if err := cursor.All(ctx, &results); err != nil {
        return nil, fmt.Errorf("failed to decode MongoDB results: %w", err)
    }
    
    if len(results) == 0 {
        return nil, daramjwee.ErrCacheableNotFound
    }
    
    // Convert to JSON
    data, err := bson.MarshalExtJSON(results, true, true)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal MongoDB results: %w", err)
    }
    
    // Generate ETag
    etag := f.generateETag(data)
    
    metadata := &daramjwee.Metadata{
        ETag:     etag,
        CachedAt: time.Now(),
    }
    
    return &daramjwee.FetchResult{
        Body:     io.NopCloser(strings.NewReader(string(data))),
        Metadata: metadata,
    }, nil
}

// hasChanged checks if any documents have been modified since last fetch
func (f *MongoFetcher) hasChanged(ctx context.Context, lastFetch time.Time) (bool, error) {
    // Add modification time filter
    changeFilter := bson.M{}
    for k, v := range f.Filter {
        changeFilter[k] = v
    }
    changeFilter["updatedAt"] = bson.M{"$gt": lastFetch}
    
    count, err := f.Collection.CountDocuments(ctx, changeFilter)
    if err != nil {
        return false, err
    }
    
    return count > 0, nil
}

// generateETag creates ETag from data
func (f *MongoFetcher) generateETag(data []byte) string {
    hash := sha256.Sum256(data)
    return fmt.Sprintf(`"%x"`, hash)
}
```

## File System Fetcher Implementation

```go
package fetcher

import (
    "context"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "time"
    
    "github.com/mrchypark/daramjwee"
)

// FileFetcher implements Fetcher for file system origins
type FileFetcher struct {
    FilePath string
}

// NewFileFetcher creates a new file system fetcher
func NewFileFetcher(filePath string) *FileFetcher {
    return &FileFetcher{
        FilePath: filePath,
    }
}

// Fetch retrieves data from file system with modification time checking
func (f *FileFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // Check context cancellation
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    default:
    }
    
    // Get file info
    fileInfo, err := os.Stat(f.FilePath)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, daramjwee.ErrCacheableNotFound
        }
        return nil, fmt.Errorf("failed to stat file: %w", err)
    }
    
    // Check if file has been modified
    modTime := fileInfo.ModTime()
    etag := fmt.Sprintf(`"%d-%d"`, modTime.Unix(), fileInfo.Size())
    
    if oldMetadata != nil && oldMetadata.ETag == etag {
        return nil, daramjwee.ErrNotModified
    }
    
    // Open file
    file, err := os.Open(f.FilePath)
    if err != nil {
        if os.IsNotExist(err) {
            return nil, daramjwee.ErrCacheableNotFound
        }
        return nil, fmt.Errorf("failed to open file: %w", err)
    }
    
    metadata := &daramjwee.Metadata{
        ETag:     etag,
        CachedAt: time.Now(),
    }
    
    return &daramjwee.FetchResult{
        Body:     file,
        Metadata: metadata,
    }, nil
}
```

## Advanced Fetcher Patterns

### Composite Fetcher

```go
// CompositeFetcher tries multiple fetchers in order
type CompositeFetcher struct {
    Fetchers []daramjwee.Fetcher
}

// NewCompositeFetcher creates a composite fetcher
func NewCompositeFetcher(fetchers ...daramjwee.Fetcher) *CompositeFetcher {
    return &CompositeFetcher{
        Fetchers: fetchers,
    }
}

// Fetch tries each fetcher until one succeeds
func (f *CompositeFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    var lastErr error
    
    for i, fetcher := range f.Fetchers {
        result, err := fetcher.Fetch(ctx, oldMetadata)
        if err == nil {
            return result, nil
        }
        
        lastErr = err
        
        // Don't try other fetchers for certain errors
        if errors.Is(err, daramjwee.ErrNotModified) {
            return nil, err
        }
        
        // Log attempt failure (except for last attempt)
        if i < len(f.Fetchers)-1 {
            log.Printf("Fetcher %d failed, trying next: %v", i, err)
        }
    }
    
    return nil, fmt.Errorf("all fetchers failed, last error: %w", lastErr)
}
```

### Caching Fetcher Wrapper

```go
// CachingFetcher wraps another fetcher with local caching
type CachingFetcher struct {
    Base      daramjwee.Fetcher
    Cache     map[string]*cachedResult
    TTL       time.Duration
    mu        sync.RWMutex
}

type cachedResult struct {
    Data      []byte
    Metadata  *daramjwee.Metadata
    ExpiresAt time.Time
}

// NewCachingFetcher creates a caching fetcher wrapper
func NewCachingFetcher(base daramjwee.Fetcher, ttl time.Duration) *CachingFetcher {
    return &CachingFetcher{
        Base:  base,
        Cache: make(map[string]*cachedResult),
        TTL:   ttl,
    }
}

// Fetch implements local caching with TTL
func (f *CachingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    cacheKey := f.generateCacheKey(ctx, oldMetadata)
    
    // Check local cache first
    f.mu.RLock()
    cached, exists := f.Cache[cacheKey]
    f.mu.RUnlock()
    
    if exists && time.Now().Before(cached.ExpiresAt) {
        // Return cached result
        return &daramjwee.FetchResult{
            Body:     io.NopCloser(bytes.NewReader(cached.Data)),
            Metadata: cached.Metadata,
        }, nil
    }
    
    // Fetch from base fetcher
    result, err := f.Base.Fetch(ctx, oldMetadata)
    if err != nil {
        return nil, err
    }
    
    // Read and cache the data
    data, err := io.ReadAll(result.Body)
    result.Body.Close()
    if err != nil {
        return nil, fmt.Errorf("failed to read fetch result: %w", err)
    }
    
    // Store in local cache
    f.mu.Lock()
    f.Cache[cacheKey] = &cachedResult{
        Data:      data,
        Metadata:  result.Metadata,
        ExpiresAt: time.Now().Add(f.TTL),
    }
    f.mu.Unlock()
    
    // Return fresh result
    return &daramjwee.FetchResult{
        Body:     io.NopCloser(bytes.NewReader(data)),
        Metadata: result.Metadata,
    }, nil
}

func (f *CachingFetcher) generateCacheKey(ctx context.Context, oldMetadata *daramjwee.Metadata) string {
    // Generate cache key based on context and metadata
    // Implementation depends on your specific needs
    return fmt.Sprintf("cache_key_%p", oldMetadata)
}
```

### Rate-Limited Fetcher

```go
// RateLimitedFetcher implements rate limiting for fetch operations
type RateLimitedFetcher struct {
    Base    daramjwee.Fetcher
    Limiter *rate.Limiter
}

// NewRateLimitedFetcher creates a rate-limited fetcher
func NewRateLimitedFetcher(base daramjwee.Fetcher, requestsPerSecond float64) *RateLimitedFetcher {
    return &RateLimitedFetcher{
        Base:    base,
        Limiter: rate.NewLimiter(rate.Limit(requestsPerSecond), 1),
    }
}

// Fetch implements rate limiting before calling base fetcher
func (f *RateLimitedFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // Wait for rate limiter
    if err := f.Limiter.Wait(ctx); err != nil {
        return nil, fmt.Errorf("rate limiter wait failed: %w", err)
    }
    
    return f.Base.Fetch(ctx, oldMetadata)
}
```

## Error Handling Strategies

### Comprehensive Error Handling

```go
// RobustFetcher implements comprehensive error handling
type RobustFetcher struct {
    Base           daramjwee.Fetcher
    MaxRetries     int
    RetryableErrors []error
    CircuitBreaker *CircuitBreaker
}

// Fetch implements robust error handling with circuit breaker
func (f *RobustFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
    // Check circuit breaker
    if f.CircuitBreaker.IsOpen() {
        return nil, fmt.Errorf("circuit breaker is open")
    }
    
    var lastErr error
    
    for attempt := 0; attempt <= f.MaxRetries; attempt++ {
        result, err := f.Base.Fetch(ctx, oldMetadata)
        if err == nil {
            f.CircuitBreaker.RecordSuccess()
            return result, nil
        }
        
        lastErr = err
        f.CircuitBreaker.RecordFailure()
        
        // Check if error is retryable
        if !f.isRetryableError(err) {
            break
        }
        
        // Don't retry on context cancellation
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            break
        }
        
        // Wait before retry
        if attempt < f.MaxRetries {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(f.calculateBackoff(attempt)):
            }
        }
    }
    
    return nil, fmt.Errorf("fetch failed after %d attempts: %w", f.MaxRetries+1, lastErr)
}

func (f *RobustFetcher) isRetryableError(err error) bool {
    for _, retryableErr := range f.RetryableErrors {
        if errors.Is(err, retryableErr) {
            return true
        }
    }
    return false
}

func (f *RobustFetcher) calculateBackoff(attempt int) time.Duration {
    base := time.Second
    return base * time.Duration(1<<uint(attempt))
}
```

### Circuit Breaker Implementation

```go
// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
    MaxFailures   int
    ResetTimeout  time.Duration
    failures      int
    lastFailTime  time.Time
    state         CircuitState
    mu            sync.RWMutex
}

type CircuitState int

const (
    CircuitClosed CircuitState = iota
    CircuitOpen
    CircuitHalfOpen
)

// IsOpen checks if circuit breaker is open
func (cb *CircuitBreaker) IsOpen() bool {
    cb.mu.RLock()
    defer cb.mu.RUnlock()
    
    if cb.state == CircuitOpen {
        if time.Since(cb.lastFailTime) > cb.ResetTimeout {
            cb.state = CircuitHalfOpen
            return false
        }
        return true
    }
    
    return false
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    
    cb.failures = 0
    cb.state = CircuitClosed
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    
    cb.failures++
    cb.lastFailTime = time.Now()
    
    if cb.failures >= cb.MaxFailures {
        cb.state = CircuitOpen
    }
}
```

## Testing Strategies

### Unit Testing Framework

```go
func TestHTTPFetcher_BasicOperations(t *testing.T) {
    // Create test server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("ETag", `"test-etag"`)
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("test data"))
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // Test successful fetch
    result, err := fetcher.Fetch(context.Background(), nil)
    assert.NoError(t, err)
    assert.NotNil(t, result)
    
    data, err := io.ReadAll(result.Body)
    result.Body.Close()
    assert.NoError(t, err)
    assert.Equal(t, "test data", string(data))
    assert.Equal(t, `"test-etag"`, result.Metadata.ETag)
}

func TestHTTPFetcher_ConditionalRequests(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Header.Get("If-None-Match") == `"test-etag"` {
            w.WriteHeader(http.StatusNotModified)
            return
        }
        
        w.Header().Set("ETag", `"test-etag"`)
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("test data"))
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // First fetch
    result, err := fetcher.Fetch(context.Background(), nil)
    assert.NoError(t, err)
    result.Body.Close()
    
    // Second fetch with metadata (should return ErrNotModified)
    _, err = fetcher.Fetch(context.Background(), result.Metadata)
    assert.ErrorIs(t, err, daramjwee.ErrNotModified)
}

func TestHTTPFetcher_ErrorHandling(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusNotFound)
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    // Test 404 response
    _, err := fetcher.Fetch(context.Background(), nil)
    assert.ErrorIs(t, err, daramjwee.ErrCacheableNotFound)
}
```

### Integration Testing

```go
func TestFetcher_WithDaramjwee(t *testing.T) {
    // Create test HTTP server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("ETag", `"integration-test"`)
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("integration test data"))
    }))
    defer server.Close()
    
    // Create fetcher
    fetcher := NewHTTPFetcher(server.URL)
    
    // Create cache
    cache, err := daramjwee.New(
        logger,
        daramjwee.WithHotStore(memstore.New(1024*1024, policy.NewLRU())),
    )
    require.NoError(t, err)
    defer cache.Close()
    
    // Test cache integration
    stream, err := cache.Get(context.Background(), "test-key", fetcher)
    require.NoError(t, err)
    defer stream.Close()
    
    data, err := io.ReadAll(stream)
    require.NoError(t, err)
    assert.Equal(t, "integration test data", string(data))
}
```

### Performance Testing

```go
func BenchmarkHTTPFetcher_Fetch(b *testing.B) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write(make([]byte, 1024))  // 1KB response
    }))
    defer server.Close()
    
    fetcher := NewHTTPFetcher(server.URL)
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            result, err := fetcher.Fetch(context.Background(), nil)
            if err != nil {
                b.Fatal(err)
            }
            result.Body.Close()
        }
    })
}
```

## Best Practices

### Design Principles

1. **Support conditional requests** using ETag and metadata for optimization
2. **Return appropriate error types** for different failure scenarios
3. **Handle context cancellation** and timeouts properly
4. **Provide streaming access** to avoid loading large data into memory
5. **Implement proper resource cleanup** (close response bodies, connections)

### Performance Optimization

1. **Use connection pooling** for HTTP clients
2. **Implement caching** at the fetcher level when appropriate
3. **Support parallel fetching** for composite operations
4. **Use streaming operations** for large data sets
5. **Implement rate limiting** to avoid overwhelming origin systems

### Error Handling

1. **Distinguish between retryable and non-retryable errors**
2. **Implement circuit breaker patterns** for failing origins
3. **Provide detailed error messages** for debugging
4. **Handle network timeouts** and connection failures gracefully
5. **Support fallback mechanisms** for high availability

### Security Considerations

1. **Validate and sanitize inputs** to prevent injection attacks
2. **Use secure HTTP clients** with proper TLS configuration
3. **Implement authentication** and authorization as needed
4. **Avoid logging sensitive data** in error messages
5. **Handle credentials securely** (use environment variables, secret managers)

## Common Pitfalls

1. **Not handling conditional requests** properly (missing ETag support)
2. **Resource leaks** from not closing response bodies or connections
3. **Poor error classification** (not using appropriate daramjwee error types)
4. **Ignoring context cancellation** leading to resource waste
5. **Not implementing retry logic** for transient failures
6. **Blocking operations** that don't respect timeouts
7. **Memory leaks** from accumulating data instead of streaming
8. **Security vulnerabilities** from improper input validation

This comprehensive guide should help you implement robust, efficient, and well-tested custom Fetcher implementations for various origin types in the daramjwee caching library.