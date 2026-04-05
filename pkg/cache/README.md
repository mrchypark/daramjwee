# Generic Cache

The Generic Cache package provides type-safe operations on top of daramjwee.Cache, eliminating the need for manual stream handling and JSON marshaling/unmarshaling for common use cases.

Because JSON unmarshaling needs the full payload, `GenericCache.Get` drains the underlying cache stream to EOF before returning. That means it preserves correctness for daramjwee's stream-through fills, but it does not preserve first-byte streaming to the caller the way the raw `daramjwee.Cache` API does.

## Features

- **Type Safety**: Compile-time type checking for cached values
- **Automatic Serialization**: JSON marshaling/unmarshaling handled automatically
- **Tier Integration**: Seamlessly works with daramjwee's ordered-tier caching model
- **Convenience Methods**: Must*, GetOrSet, GetWithDefault patterns
- **Zero Configuration**: Works with any existing daramjwee.Cache instance

## Usage

### Basic Setup

```go
import (
    "github.com/mrchypark/daramjwee"
    "github.com/mrchypark/daramjwee/pkg/cache"
    "github.com/mrchypark/daramjwee/pkg/store/memstore"
    "github.com/mrchypark/daramjwee/pkg/policy"
)

// Create underlying daramjwee cache
memStore := memstore.New(1*1024*1024, policy.NewLRU())
baseCache, err := daramjwee.New(
    logger,
    daramjwee.WithTiers(memStore),
)

// Create type-safe cache for your struct
type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

userCache := cache.NewGeneric[User](baseCache)
```

### Basic Operations

```go
ctx := context.Background()
metadata := &daramjwee.Metadata{CacheTag: "v1"}

// Set a value
user := User{ID: 1, Name: "John Doe"}
err := userCache.Set(ctx, "user:1", user, metadata)

// Get a value with fetcher (for cache miss)
fetcher := cache.GenericFetcher[User](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (User, *daramjwee.Metadata, error) {
    // Fetch from database, API, etc.
    return User{ID: 1, Name: "John Doe"}, metadata, nil
})

user, err := userCache.Get(ctx, "user:1", fetcher)
```

### Convenience Methods

```go
// GetOrSet - get from cache or set using factory function
user, err := userCache.GetOrSet(ctx, "user:1", func() (User, *daramjwee.Metadata, error) {
    return fetchUserFromDB(1)
})

// GetWithDefault - return default value on error
user := userCache.GetWithDefault(ctx, "user:1", User{}, fetcher)

// Must methods - panic on error (use when confident)
userCache.MustSet(ctx, "user:1", user, metadata)
user = userCache.MustGet(ctx, "user:1", fetcher)
```

### Working with Different Types

```go
// String cache
stringCache := cache.NewGeneric[string](baseCache)
stringCache.Set(ctx, "greeting", "Hello World", metadata)

// Slice cache
type UserList []User
userListCache := cache.NewGeneric[UserList](baseCache)

// Map cache
type UserMap map[string]User
userMapCache := cache.NewGeneric[UserMap](baseCache)

// Any JSON-serializable type works
type Config struct {
    AppName string `json:"app_name"`
    Debug   bool   `json:"debug"`
}
configCache := cache.NewGeneric[Config](baseCache)
```

### Advanced Features

```go
// Background refresh
err := userCache.ScheduleRefresh(ctx, "user:1", fetcher)

// Delete
err := userCache.Delete(ctx, "user:1")

// Close (closes underlying cache)
userCache.Close()
```

## GenericFetcher

The `GenericFetcher[T]` type is a function that returns a value of type T along with metadata. It's called when:

1. The key is not found in any cache tier (cache miss)
2. The cached value is stale and needs refreshing
3. Background refresh is triggered

```go
type GenericFetcher[T any] func(ctx context.Context, oldMetadata *daramjwee.Metadata) (T, *daramjwee.Metadata, error)

// Example: Database fetcher
userFetcher := cache.GenericFetcher[User](func(ctx context.Context, oldMetadata *daramjwee.Metadata) (User, *daramjwee.Metadata, error) {
    user, err := db.GetUser(ctx, userID)
    if err != nil {
        return User{}, nil, err
    }
    
    return user, &daramjwee.Metadata{
        CacheTag: fmt.Sprintf("user-%d-v%d", user.ID, user.Version),
    }, nil
})
```

## Error Handling

The generic cache preserves the public `daramjwee.Cache` error surface:

- `daramjwee.ErrNotFound`: Key not found and the wrapped fetcher could not supply a value
- Fetchers may internally return `daramjwee.ErrNotModified` or `daramjwee.ErrCacheableNotFound`, but those are normalized by the base cache before `GenericCache.Get` returns

```go
user, err := userCache.Get(ctx, "user:1", fetcher)
if errors.Is(err, daramjwee.ErrNotFound) {
    // Handle not found case
}
```

## Performance Considerations

- JSON marshaling/unmarshaling adds overhead compared to raw streams
- Reads are fully buffered before `json.Unmarshal`, so this wrapper is not suitable when caller-visible first-byte streaming matters
- Best suited for small to medium-sized objects
- For large binary data, consider using the raw daramjwee.Cache interface
- The generic cache is optimized for developer productivity over raw performance

## Integration with Ordered Tiers

The generic cache automatically works with daramjwee's ordered-tier architecture:

```go
// Setup with both memory (tier 0) and file (tier 1) stores
memStore := memstore.New(1*1024*1024, policy.NewLRU())
fileStore, err := filestore.New("/tmp/cache", logger)
if err != nil {
    // handle error
}

baseCache, err := daramjwee.New(
    logger,
    daramjwee.WithTiers(memStore, fileStore),
)
if err != nil {
    // handle error
}

// Generic cache automatically uses both tiers
userCache := cache.NewGeneric[User](baseCache)

// Data flows: Tier 0 -> Tier 1 -> Origin (via fetcher)
user, err := userCache.Get(ctx, "user:1", fetcher)
```

## Examples

See `examples/generic_cache/main.go` for a complete working example demonstrating:

- Multiple data types (User, Config, Product, string)
- Ordered-tier integration
- All convenience methods
- Error handling patterns
- Background operations
