# Custom EvictionPolicy Implementation Guidelines

## Overview

This document provides comprehensive guidelines for implementing custom eviction policies for the daramjwee caching library. An EvictionPolicy determines which cached objects should be removed when storage capacity is exceeded, implementing various algorithms to optimize cache hit rates and performance.

## EvictionPolicy Interface Contract

The EvictionPolicy interface defines four core methods that must be implemented:

```go
type EvictionPolicy interface {
    Touch(key string)
    Add(key string, size int64)
    Remove(key string)
    Evict() []string
}
```

### Method Implementation Requirements

#### Touch Method

**Purpose**: Record access to an existing cached object for eviction tracking.

**Implementation Requirements**:
- Update access tracking data structures (timestamps, counters, positions)
- Handle non-existent keys gracefully (no-op behavior)
- Optimize for high-frequency calls (called on every cache hit)
- Maintain thread safety if required by your implementation

**Example Implementation Patterns**:

```go
// LRU-style access tracking
func (p *CustomPolicy) Touch(key string) {
    if elem, ok := p.cache[key]; ok {
        // Move to front of access-ordered list
        p.accessList.MoveToFront(elem)
        
        // Update timestamp for time-based policies
        elem.Value.(*entry).lastAccess = time.Now()
    }
}

// Counter-based access tracking
func (p *CustomPolicy) Touch(key string) {
    if entry, ok := p.entries[key]; ok {
        // Increment access counter
        entry.accessCount++
        
        // Update frequency-based data structures
        p.updateFrequencyTracking(key, entry.accessCount)
    }
}

// Bit-based access tracking (SIEVE-style)
func (p *CustomPolicy) Touch(key string) {
    if entry, ok := p.entries[key]; ok {
        // Set visited bit for scan-based eviction
        entry.visited = true
    }
}
```

#### Add Method

**Purpose**: Record addition of a new object to the cache with size information.

**Implementation Requirements**:
- Initialize tracking state for new objects
- Update existing objects if key already exists
- Maintain accurate size accounting for capacity management
- Update internal data structures (lists, heaps, trees)

**Example Implementation Patterns**:

```go
// List-based policy (LRU, FIFO)
func (p *CustomPolicy) Add(key string, size int64) {
    if elem, ok := p.cache[key]; ok {
        // Update existing entry
        entry := elem.Value.(*entry)
        p.totalSize -= entry.size  // Subtract old size
        entry.size = size
        p.totalSize += size        // Add new size
        
        // Move to appropriate position (front for LRU)
        p.accessList.MoveToFront(elem)
        return
    }
    
    // Add new entry
    entry := &entry{
        key:        key,
        size:       size,
        addedAt:    time.Now(),
        lastAccess: time.Now(),
    }
    
    elem := p.accessList.PushFront(entry)
    p.cache[key] = elem
    p.totalSize += size
}

// Heap-based policy (size-based, frequency-based)
func (p *CustomPolicy) Add(key string, size int64) {
    if entry, ok := p.entries[key]; ok {
        // Update existing entry
        p.totalSize -= entry.size
        entry.size = size
        p.totalSize += size
        
        // Update heap position
        heap.Fix(&p.heap, entry.heapIndex)
        return
    }
    
    // Add new entry
    entry := &entry{
        key:  key,
        size: size,
    }
    
    p.entries[key] = entry
    heap.Push(&p.heap, entry)
    p.totalSize += size
}
```

#### Remove Method

**Purpose**: Clean up tracking state when an object is explicitly deleted.

**Implementation Requirements**:
- Remove all tracking data for the specified key
- Update size accounting
- Clean up internal data structures
- Handle non-existent keys gracefully (idempotent)

**Example Implementation Patterns**:

```go
// List-based cleanup
func (p *CustomPolicy) Remove(key string) {
    if elem, ok := p.cache[key]; ok {
        entry := elem.Value.(*entry)
        p.totalSize -= entry.size
        
        // Remove from list and map
        p.accessList.Remove(elem)
        delete(p.cache, key)
        
        // Update hand pointer if necessary (for scan-based policies)
        if elem == p.hand {
            p.hand = elem.Prev()
            if p.hand == nil {
                p.hand = p.accessList.Back()
            }
        }
    }
}

// Heap-based cleanup
func (p *CustomPolicy) Remove(key string) {
    if entry, ok := p.entries[key]; ok {
        p.totalSize -= entry.size
        
        // Remove from heap
        heap.Remove(&p.heap, entry.heapIndex)
        delete(p.entries, key)
    }
}
```

#### Evict Method

**Purpose**: Determine which objects should be removed to free space.

**Implementation Requirements**:
- Implement core eviction algorithm logic
- Return keys in eviction order (most suitable victims first)
- Update internal state to reflect eviction decisions
- Handle empty cache gracefully (return empty slice)

**Example Implementation Patterns**:

```go
// LRU eviction
func (p *CustomPolicy) Evict() []string {
    elem := p.accessList.Back()  // Least recently used
    if elem == nil {
        return nil
    }
    
    entry := elem.Value.(*entry)
    p.removeElement(elem)
    
    return []string{entry.key}
}

// Size-based eviction (largest first)
func (p *CustomPolicy) Evict() []string {
    if p.heap.Len() == 0 {
        return nil
    }
    
    // Pop largest item
    entry := heap.Pop(&p.heap).(*entry)
    delete(p.entries, entry.key)
    p.totalSize -= entry.size
    
    return []string{entry.key}
}

// Scan-based eviction (SIEVE-style)
func (p *CustomPolicy) Evict() []string {
    if p.accessList.Len() == 0 {
        return nil
    }
    
    // Start scanning from hand position
    current := p.hand
    if current == nil {
        current = p.accessList.Back()
    }
    
    // Scan for eviction candidate
    for {
        entry := current.Value.(*entry)
        if entry.visited {
            // Give second chance
            entry.visited = false
            current = current.Prev()
            if current == nil {
                current = p.accessList.Back()
            }
        } else {
            // Found victim
            break
        }
    }
    
    // Update hand and remove victim
    p.hand = current.Prev()
    if p.hand == nil {
        p.hand = p.accessList.Back()
    }
    
    victim := p.removeElement(current)
    return []string{victim.key}
}
```

## Thread Safety Considerations

### State Management Patterns

**Important**: The daramjwee library handles thread safety at the Store level, so EvictionPolicy implementations are **NOT required to be thread-safe**. However, if you plan to use your policy in other contexts, consider these patterns:

#### Mutex-Based Protection

```go
type ThreadSafePolicy struct {
    mu     sync.RWMutex
    policy *UnsafePolicy
}

func (p *ThreadSafePolicy) Touch(key string) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.policy.Touch(key)
}

func (p *ThreadSafePolicy) Add(key string, size int64) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.policy.Add(key, size)
}

func (p *ThreadSafePolicy) Remove(key string) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.policy.Remove(key)
}

func (p *ThreadSafePolicy) Evict() []string {
    p.mu.Lock()
    defer p.mu.Unlock()
    return p.policy.Evict()
}
```

#### Lock-Free Approaches

```go
// Using atomic operations for counters
type LockFreePolicy struct {
    entries sync.Map  // key -> *entry
    counter int64     // atomic counter
}

func (p *LockFreePolicy) Touch(key string) {
    if val, ok := p.entries.Load(key); ok {
        entry := val.(*entry)
        atomic.StoreInt64(&entry.lastAccess, atomic.AddInt64(&p.counter, 1))
    }
}
```

### Performance Optimization Strategies

#### Data Structure Selection

**Lists vs Maps vs Heaps**:

```go
// Fast access, slow eviction (O(1) access, O(n) eviction)
type MapBasedPolicy struct {
    entries map[string]*entry
}

// Balanced access and eviction (O(1) access, O(1) eviction)
type ListBasedPolicy struct {
    list  *list.List
    cache map[string]*list.Element
}

// Slow access, fast eviction (O(log n) access, O(log n) eviction)
type HeapBasedPolicy struct {
    heap    entryHeap
    entries map[string]*entry
}
```

#### Memory Optimization

```go
// Pool frequently allocated objects
var entryPool = sync.Pool{
    New: func() interface{} {
        return &entry{}
    },
}

func (p *CustomPolicy) newEntry(key string, size int64) *entry {
    entry := entryPool.Get().(*entry)
    entry.key = key
    entry.size = size
    entry.reset()  // Reset other fields
    return entry
}

func (p *CustomPolicy) releaseEntry(entry *entry) {
    entryPool.Put(entry)
}
```

#### Algorithm-Specific Optimizations

```go
// Batch eviction for better performance
func (p *CustomPolicy) Evict() []string {
    const batchSize = 10
    victims := make([]string, 0, batchSize)
    
    for len(victims) < batchSize && p.hasEvictionCandidates() {
        victim := p.selectVictim()
        if victim != "" {
            victims = append(victims, victim)
        }
    }
    
    return victims
}

// Lazy cleanup to reduce overhead
func (p *CustomPolicy) Remove(key string) {
    if entry, ok := p.entries[key]; ok {
        entry.deleted = true  // Mark for lazy cleanup
        // Actual cleanup happens during eviction
    }
}
```

## Advanced Algorithm Examples

### Time-Based Eviction Policy

```go
type TTLPolicy struct {
    entries map[string]*ttlEntry
    heap    ttlHeap
}

type ttlEntry struct {
    key       string
    size      int64
    expiresAt time.Time
    heapIndex int
}

func NewTTLPolicy(defaultTTL time.Duration) *TTLPolicy {
    return &TTLPolicy{
        entries: make(map[string]*ttlEntry),
        heap:    make(ttlHeap, 0),
    }
}

func (p *TTLPolicy) Add(key string, size int64) {
    now := time.Now()
    expiresAt := now.Add(p.defaultTTL)
    
    if entry, ok := p.entries[key]; ok {
        // Update existing entry
        entry.size = size
        entry.expiresAt = expiresAt
        heap.Fix(&p.heap, entry.heapIndex)
        return
    }
    
    // Add new entry
    entry := &ttlEntry{
        key:       key,
        size:      size,
        expiresAt: expiresAt,
    }
    
    p.entries[key] = entry
    heap.Push(&p.heap, entry)
}

func (p *TTLPolicy) Evict() []string {
    now := time.Now()
    var victims []string
    
    // Evict all expired entries
    for p.heap.Len() > 0 {
        entry := p.heap[0]
        if entry.expiresAt.After(now) {
            break  // No more expired entries
        }
        
        heap.Pop(&p.heap)
        delete(p.entries, entry.key)
        victims = append(victims, entry.key)
    }
    
    return victims
}
```

### Frequency-Based Eviction Policy

```go
type LFUPolicy struct {
    entries   map[string]*lfuEntry
    freqLists map[int]*list.List  // frequency -> list of entries
    minFreq   int
}

type lfuEntry struct {
    key       string
    size      int64
    frequency int
    listElem  *list.Element
}

func NewLFUPolicy() *LFUPolicy {
    return &LFUPolicy{
        entries:   make(map[string]*lfuEntry),
        freqLists: make(map[int]*list.List),
        minFreq:   1,
    }
}

func (p *LFUPolicy) Touch(key string) {
    if entry, ok := p.entries[key]; ok {
        p.incrementFrequency(entry)
    }
}

func (p *LFUPolicy) incrementFrequency(entry *lfuEntry) {
    oldFreq := entry.frequency
    newFreq := oldFreq + 1
    
    // Remove from old frequency list
    oldList := p.freqLists[oldFreq]
    oldList.Remove(entry.listElem)
    
    // Update minimum frequency if necessary
    if oldFreq == p.minFreq && oldList.Len() == 0 {
        p.minFreq++
    }
    
    // Add to new frequency list
    if p.freqLists[newFreq] == nil {
        p.freqLists[newFreq] = list.New()
    }
    newList := p.freqLists[newFreq]
    entry.listElem = newList.PushFront(entry)
    entry.frequency = newFreq
}

func (p *LFUPolicy) Evict() []string {
    if len(p.entries) == 0 {
        return nil
    }
    
    // Find minimum frequency list
    minList := p.freqLists[p.minFreq]
    if minList == nil || minList.Len() == 0 {
        return nil
    }
    
    // Remove least frequently used item
    elem := minList.Back()
    entry := elem.Value.(*lfuEntry)
    
    minList.Remove(elem)
    delete(p.entries, entry.key)
    
    return []string{entry.key}
}
```

### Adaptive Replacement Cache (ARC) Policy

```go
type ARCPolicy struct {
    c    int  // Cache capacity
    p    int  // Adaptive parameter
    
    // Four lists as per ARC algorithm
    t1   *list.List  // Recent cache misses
    t2   *list.List  // Frequent items
    b1   *list.List  // Ghost entries for t1
    b2   *list.List  // Ghost entries for t2
    
    cache map[string]*arcEntry
}

type arcEntry struct {
    key      string
    size     int64
    listType int  // 1=T1, 2=T2, 3=B1, 4=B2
    elem     *list.Element
}

func NewARCPolicy(capacity int) *ARCPolicy {
    return &ARCPolicy{
        c:     capacity,
        p:     0,
        t1:    list.New(),
        t2:    list.New(),
        b1:    list.New(),
        b2:    list.New(),
        cache: make(map[string]*arcEntry),
    }
}

func (p *ARCPolicy) Touch(key string) {
    entry, ok := p.cache[key]
    if !ok {
        return
    }
    
    switch entry.listType {
    case 1: // T1 -> T2
        p.t1.Remove(entry.elem)
        entry.elem = p.t2.PushFront(entry)
        entry.listType = 2
    case 2: // T2 -> front of T2
        p.t2.MoveToFront(entry.elem)
    }
}

func (p *ARCPolicy) Add(key string, size int64) {
    if entry, ok := p.cache[key]; ok {
        // Update existing entry
        entry.size = size
        p.Touch(key)
        return
    }
    
    // Add new entry to T1
    entry := &arcEntry{
        key:      key,
        size:     size,
        listType: 1,
    }
    entry.elem = p.t1.PushFront(entry)
    p.cache[key] = entry
    
    // Maintain cache size
    p.replace()
}

func (p *ARCPolicy) replace() {
    // ARC replacement algorithm implementation
    // (Simplified version - full implementation would handle all ARC cases)
    
    if p.t1.Len() > 0 && p.t1.Len() > p.p {
        // Move LRU from T1 to B1
        elem := p.t1.Back()
        entry := elem.Value.(*arcEntry)
        
        p.t1.Remove(elem)
        entry.elem = p.b1.PushFront(entry)
        entry.listType = 3
    } else if p.t2.Len() > 0 {
        // Move LRU from T2 to B2
        elem := p.t2.Back()
        entry := elem.Value.(*arcEntry)
        
        p.t2.Remove(elem)
        entry.elem = p.b2.PushFront(entry)
        entry.listType = 4
    }
}

func (p *ARCPolicy) Evict() []string {
    // Evict from T1 or T2 based on ARC algorithm
    var victim *arcEntry
    
    if p.t1.Len() > 0 && p.t1.Len() > p.p {
        elem := p.t1.Back()
        victim = elem.Value.(*arcEntry)
        p.t1.Remove(elem)
    } else if p.t2.Len() > 0 {
        elem := p.t2.Back()
        victim = elem.Value.(*arcEntry)
        p.t2.Remove(elem)
    }
    
    if victim != nil {
        delete(p.cache, victim.key)
        return []string{victim.key}
    }
    
    return nil
}
```

## Integration Patterns with Store Implementations

### Policy Factory Pattern

```go
type PolicyFactory interface {
    CreatePolicy(config PolicyConfig) daramjwee.EvictionPolicy
}

type PolicyConfig struct {
    Algorithm string
    Capacity  int64
    TTL       time.Duration
    Options   map[string]interface{}
}

type DefaultPolicyFactory struct{}

func (f *DefaultPolicyFactory) CreatePolicy(config PolicyConfig) daramjwee.EvictionPolicy {
    switch config.Algorithm {
    case "lru":
        return NewLRUPolicy()
    case "lfu":
        return NewLFUPolicy()
    case "ttl":
        return NewTTLPolicy(config.TTL)
    case "arc":
        return NewARCPolicy(int(config.Capacity))
    default:
        return NewLRUPolicy()  // Default fallback
    }
}
```

### Policy Composition

```go
// Composite policy that combines multiple strategies
type CompositePolicy struct {
    primary   daramjwee.EvictionPolicy
    secondary daramjwee.EvictionPolicy
    strategy  CompositionStrategy
}

type CompositionStrategy int

const (
    PrimaryFirst CompositionStrategy = iota
    WeightedRandom
    AlternatingRounds
)

func (p *CompositePolicy) Evict() []string {
    switch p.strategy {
    case PrimaryFirst:
        victims := p.primary.Evict()
        if len(victims) == 0 {
            victims = p.secondary.Evict()
        }
        return victims
        
    case WeightedRandom:
        if rand.Float64() < 0.7 {  // 70% primary, 30% secondary
            return p.primary.Evict()
        }
        return p.secondary.Evict()
        
    case AlternatingRounds:
        // Implementation for alternating between policies
        return p.alternateEviction()
    }
    
    return nil
}
```

## Testing Strategies

### Unit Testing Framework

```go
func TestCustomPolicy_BasicOperations(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Test Add
    policy.Add("key1", 100)
    policy.Add("key2", 200)
    
    // Test Touch
    policy.Touch("key1")
    
    // Test Evict
    victims := policy.Evict()
    assert.Equal(t, []string{"key2"}, victims)  // key1 was touched, key2 should be evicted
    
    // Test Remove
    policy.Remove("key1")
    victims = policy.Evict()
    assert.Empty(t, victims)  // No more items to evict
}

func TestCustomPolicy_EdgeCases(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Test empty cache
    victims := policy.Evict()
    assert.Empty(t, victims)
    
    // Test non-existent key operations
    policy.Touch("nonexistent")  // Should not panic
    policy.Remove("nonexistent") // Should not panic
    
    // Test duplicate adds
    policy.Add("key1", 100)
    policy.Add("key1", 200)  // Should update, not duplicate
    
    victims = policy.Evict()
    assert.Equal(t, []string{"key1"}, victims)
}
```

### Performance Testing

```go
func BenchmarkCustomPolicy_Touch(b *testing.B) {
    policy := NewCustomPolicy()
    
    // Setup test data
    for i := 0; i < 10000; i++ {
        policy.Add(fmt.Sprintf("key%d", i), int64(i))
    }
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        i := 0
        for pb.Next() {
            policy.Touch(fmt.Sprintf("key%d", i%10000))
            i++
        }
    })
}

func BenchmarkCustomPolicy_Evict(b *testing.B) {
    policy := NewCustomPolicy()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // Add item
        policy.Add(fmt.Sprintf("key%d", i), 100)
        
        // Evict item
        victims := policy.Evict()
        if len(victims) == 0 {
            b.Fatal("Expected eviction victim")
        }
    }
}
```

### Integration Testing

```go
func TestCustomPolicy_WithMemStore(t *testing.T) {
    policy := NewCustomPolicy()
    store := memstore.New(1024, policy)  // 1KB capacity
    
    cache, err := daramjwee.New(
        logger,
        daramjwee.WithHotStore(store),
    )
    require.NoError(t, err)
    defer cache.Close()
    
    // Test cache operations trigger policy methods
    fetcher := &testFetcher{data: strings.Repeat("x", 512)}  // 512 bytes
    
    // Fill cache to capacity
    stream1, err := cache.Get(context.Background(), "key1", fetcher)
    require.NoError(t, err)
    stream1.Close()
    
    stream2, err := cache.Get(context.Background(), "key2", fetcher)
    require.NoError(t, err)
    stream2.Close()
    
    // This should trigger eviction
    stream3, err := cache.Get(context.Background(), "key3", fetcher)
    require.NoError(t, err)
    stream3.Close()
    
    // Verify eviction occurred
    // (Implementation depends on your policy's behavior)
}
```

### Correctness Testing

```go
func TestCustomPolicy_EvictionCorrectness(t *testing.T) {
    policy := NewCustomPolicy()
    
    // Add items with known access pattern
    policy.Add("frequent", 100)
    policy.Add("infrequent", 100)
    
    // Create access pattern
    for i := 0; i < 10; i++ {
        policy.Touch("frequent")
    }
    policy.Touch("infrequent")  // Only once
    
    // Verify eviction order
    victims := policy.Evict()
    assert.Equal(t, []string{"infrequent"}, victims)
    
    victims = policy.Evict()
    assert.Equal(t, []string{"frequent"}, victims)
}
```

## Best Practices

### Algorithm Design

1. **Choose appropriate data structures** based on your algorithm's access patterns
2. **Minimize overhead** for Touch operations (called on every cache hit)
3. **Batch evictions** when possible to reduce overhead
4. **Handle edge cases** gracefully (empty cache, non-existent keys)
5. **Consider memory usage** of tracking data structures

### Performance Optimization

1. **Profile your implementation** under realistic workloads
2. **Use object pooling** for frequently allocated structures
3. **Implement lazy cleanup** to reduce immediate overhead
4. **Consider lock-free approaches** for high-concurrency scenarios
5. **Optimize for common cases** (e.g., cache hits vs. misses)

### Code Quality

1. **Document algorithm behavior** and complexity characteristics
2. **Provide configuration options** for tuning parameters
3. **Implement comprehensive tests** including edge cases
4. **Follow Go idioms** and conventions
5. **Handle errors gracefully** and provide meaningful error messages

### Integration Considerations

1. **Test with actual Store implementations** to verify integration
2. **Consider different workload patterns** (read-heavy, write-heavy, mixed)
3. **Provide factory functions** for easy instantiation
4. **Support configuration** through options or parameters
5. **Document performance characteristics** and trade-offs

## Common Pitfalls

1. **Not handling non-existent keys** in Touch and Remove methods
2. **Memory leaks** from not cleaning up internal data structures
3. **Incorrect size accounting** leading to capacity management issues
4. **Race conditions** in thread-safe implementations
5. **Poor performance** due to inappropriate data structure choices
6. **Not testing edge cases** like empty cache or single-item cache
7. **Ignoring the frequency** of Touch calls in performance optimization
8. **Complex algorithms** that don't provide significant benefits over simpler ones

## Algorithm Selection Guidelines

### When to Use Different Algorithms

**LRU (Least Recently Used)**:
- General-purpose caching with temporal locality
- Predictable behavior and easy to understand
- Good baseline for most applications

**LFU (Least Frequently Used)**:
- Workloads with strong frequency patterns
- Long-running caches where frequency matters more than recency
- Applications with stable access patterns

**SIEVE**:
- Memory-constrained environments
- High-throughput scenarios where overhead matters
- Good balance of simplicity and effectiveness

**S3-FIFO**:
- Modern web applications with mixed access patterns
- Scenarios with both one-hit wonders and popular items
- When you need better hit rates than LRU

**TTL-based**:
- Time-sensitive data with natural expiration
- Compliance requirements for data retention
- Scenarios where freshness is more important than popularity

**ARC (Adaptive Replacement Cache)**:
- Workloads with changing access patterns
- When you need adaptive behavior without manual tuning
- High-performance scenarios where complexity is justified

This comprehensive guide should help you implement robust, efficient, and well-tested custom eviction policies for the daramjwee caching library.