package daramjwee

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrTopWriteInvalidated = errors.New("daramjwee: top-tier write invalidated")

type topWriteManager struct {
	coords sync.Map
}

type fanoutWriteManager struct {
	locks sync.Map
}

type writeCoordinator struct {
	manager     *topWriteManager
	key         string
	refMu       sync.Mutex
	references  int
	initOnce    sync.Once
	leaseOnce   sync.Once
	writeLease  chan struct{}
	commitLease chan struct{}
	stateMu     sync.RWMutex
	// committedGeneration is the latest generation visible in the top store.
	// Uses atomic for lock-free reads on the hot path (currentTopWriteGeneration).
	committedGeneration atomic.Uint64
	nextGeneration      uint64
	activeReservations  map[uint64]struct{}
	activeDeletes       int
	activeDeletesDone   chan struct{}
	activeFill          *topFillSink
	fillPreemptions     int
}

type topWriteGeneration struct {
	coord      *writeCoordinator
	generation uint64
	once       sync.Once
}
type fanoutWriteLock struct {
	mu    sync.Mutex
	refMu sync.Mutex
	refs  int
}

type fanoutLockKey struct {
	destTierIndex int
	key           string
}

func (m *topWriteManager) coordinator(key string) *writeCoordinator {
	for {
		if coord := m.acquireCoordinatorIfPresent(key); coord != nil {
			return coord
		}

		coord := &writeCoordinator{
			manager:    m,
			key:        key,
			references: 1,
		}
		coord.init()
		if _, loaded := m.coords.LoadOrStore(key, coord); !loaded {
			return coord
		}
	}
}

func (m *topWriteManager) acquireCoordinatorIfPresent(key string) *writeCoordinator {
	for {
		value, ok := m.coords.Load(key)
		if !ok {
			return nil
		}
		coord, _ := value.(*writeCoordinator)
		coord.refMu.Lock()
		current, stillPresent := m.coords.Load(key)
		if !stillPresent || current != value {
			coord.refMu.Unlock()
			continue
		}
		coord.references++
		coord.refMu.Unlock()
		return coord
	}
}

func (c *writeCoordinator) releaseReference() {
	if c.manager == nil {
		return
	}
	c.refMu.Lock()
	defer c.refMu.Unlock()
	c.references--
	if c.references < 0 {
		panic("daramjwee: released top-write coordinator too many times")
	}
	if c.references == 0 {
		c.manager.coords.CompareAndDelete(c.key, c)
	}
}

func (c *writeCoordinator) retainReference() bool {
	if c.manager == nil {
		return true
	}
	c.refMu.Lock()
	defer c.refMu.Unlock()
	if c.references <= 0 {
		return false
	}
	c.references++
	return true
}

func (g *topWriteGeneration) retain() *topWriteGeneration {
	if g == nil {
		return nil
	}
	if !g.coord.retainReference() {
		return nil
	}
	return &topWriteGeneration{coord: g.coord, generation: g.generation}
}

func (g *topWriteGeneration) release() {
	if g == nil {
		return
	}
	g.once.Do(g.coord.releaseReference)
}

func (m *topWriteManager) coordinatorForWrite(key string, expected *topWriteGeneration) (*writeCoordinator, *uint64, error) {
	if expected == nil {
		return m.coordinator(key), nil, nil
	}
	if expected.coord.manager != m || expected.coord.key != key {
		return nil, nil, ErrTopWriteInvalidated
	}
	if !expected.coord.retainReference() {
		return nil, nil, ErrTopWriteInvalidated
	}
	generation := expected.generation
	return expected.coord, &generation, nil
}

func (m *fanoutWriteManager) lock(destTierIndex int, key string) func() {
	lockKey := fanoutLockKey{destTierIndex: destTierIndex, key: key}
	lock := m.acquire(lockKey)
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.release(lockKey, lock)
	}
}

func (m *fanoutWriteManager) acquire(lockKey fanoutLockKey) *fanoutWriteLock {
	for {
		if existing, ok := m.locks.Load(lockKey); ok {
			lock, _ := existing.(*fanoutWriteLock)
			lock.refMu.Lock()
			current, stillPresent := m.locks.Load(lockKey)
			if !stillPresent || current != existing {
				lock.refMu.Unlock()
				continue
			}
			lock.refs++
			lock.refMu.Unlock()
			return lock
		}

		lock := &fanoutWriteLock{refs: 1}
		actual, loaded := m.locks.LoadOrStore(lockKey, lock)
		if !loaded {
			return lock
		}
		resolved, _ := actual.(*fanoutWriteLock)
		resolved.refMu.Lock()
		current, stillPresent := m.locks.Load(lockKey)
		if !stillPresent || current != actual {
			resolved.refMu.Unlock()
			continue
		}
		resolved.refs++
		resolved.refMu.Unlock()
		return resolved
	}
}

func (m *fanoutWriteManager) release(lockKey fanoutLockKey, lock *fanoutWriteLock) {
	lock.refMu.Lock()
	defer lock.refMu.Unlock()
	lock.refs--
	if lock.refs == 0 {
		m.locks.CompareAndDelete(lockKey, lock)
	}
}

func (m *topWriteManager) currentGeneration(key string) *topWriteGeneration {
	coord := m.coordinator(key)
	return &topWriteGeneration{coord: coord, generation: coord.current()}
}

func (c *writeCoordinator) current() uint64 {
	c.init()
	return c.committedGeneration.Load()
}

func (c *writeCoordinator) canAttemptExpectedTopWrite(expectedGeneration uint64) bool {
	c.init()
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.activeDeletes == 0 &&
		c.committedGeneration.Load() == expectedGeneration
}

func (c *writeCoordinator) ensureReservationsLocked() {
	if c.activeReservations == nil {
		c.activeReservations = make(map[uint64]struct{})
	}
}

func (c *writeCoordinator) latestGenerationLocked() uint64 {
	latest := c.committedGeneration.Load()
	if len(c.activeReservations) == 0 {
		return latest
	}
	// nextGeneration is only an assignment high-water mark. Rolled-back
	// reservations must not invalidate conditional writes, so compare only
	// generations that are committed or still active.
	for generation := range c.activeReservations {
		if generation > latest {
			latest = generation
		}
	}
	return latest
}

func (c *writeCoordinator) reserveGenerationLocked() uint64 {
	c.ensureReservationsLocked()
	latest := c.latestGenerationLocked()
	if c.nextGeneration < latest {
		c.nextGeneration = latest
	}
	c.nextGeneration++
	generation := c.nextGeneration
	c.activeReservations[generation] = struct{}{}
	return generation
}

func (c *writeCoordinator) advanceCommittedLocked() {
	generation := c.reserveGenerationLocked()
	c.committedGeneration.Store(generation)
	c.pruneReservationsThroughLocked(generation)
}

func (c *writeCoordinator) removeReservationLocked(generation uint64) {
	if c.activeReservations == nil {
		return
	}
	delete(c.activeReservations, generation)
}

func (c *writeCoordinator) pruneReservationsThroughLocked(generation uint64) {
	c.ensureReservationsLocked()
	for reserved := range c.activeReservations {
		if reserved <= generation {
			delete(c.activeReservations, reserved)
		}
	}
}

func (c *writeCoordinator) reserve(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if expected == nil {
		if err := c.waitForNoActiveDeletesLocked(ctx); err != nil {
			return 0, err
		}
	} else if c.activeDeletes > 0 {
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if expected != nil && c.latestGenerationLocked() != *expected {
		return 0, ErrTopWriteInvalidated
	}
	generation := c.reserveGenerationLocked()
	return generation, nil
}

func (c *writeCoordinator) reserveBestEffort(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.activeFill != nil || c.activeDeletes > 0 || c.fillPreemptions > 0 {
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if expected != nil && c.latestGenerationLocked() != *expected {
		return 0, ErrTopWriteInvalidated
	}
	return c.reserveGenerationLocked(), nil
}

func (c *writeCoordinator) reserveWithFill(ctx context.Context, expected uint64, fill *topFillSink) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.activeFill != nil || c.activeDeletes > 0 || c.fillPreemptions > 0 {
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c.latestGenerationLocked() != expected {
		return 0, ErrTopWriteInvalidated
	}
	generation := c.reserveGenerationLocked()
	if fill != nil {
		fill.registered = true
		c.activeFill = fill
	}
	return generation, nil
}

func (c *writeCoordinator) waitForNoActiveDeletes(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.init()
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.waitForNoActiveDeletesLocked(ctx)
}

func (c *writeCoordinator) waitForNoActiveDeletesLocked(ctx context.Context) error {
	for c.activeDeletes > 0 {
		done := c.activeDeletesDone
		c.stateMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			c.stateMu.Lock()
			return ctx.Err()
		}
		c.stateMu.Lock()
	}
	return ctx.Err()
}

func (c *writeCoordinator) lockCommitWhenNoActiveDeletes(ctx context.Context) error {
	for {
		if err := c.waitForNoActiveDeletes(ctx); err != nil {
			return err
		}
		if err := c.acquireCommit(ctx); err != nil {
			return err
		}
		c.stateMu.Lock()
		if c.activeDeletes == 0 {
			c.stateMu.Unlock()
			return nil
		}
		c.stateMu.Unlock()
		c.releaseCommit()
	}
}

func newCoordinatorWaitContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (c *writeCoordinator) unregisterReservation(generation uint64) {
	c.init()
	c.stateMu.Lock()
	c.removeReservationLocked(generation)
	c.stateMu.Unlock()
}

func (c *writeCoordinator) init() {
	// Coordination state is created lazily: reservation maps are allocated by
	// ensureReservationsLocked on first reservation, and activeDeletesDone is
	// allocated by beginDelete on first delete. Read-only hot-path
	// coordinators therefore stay allocation-free after construction.
	c.initOnce.Do(func() {})
}

func (c *writeCoordinator) initLease() {
	c.init()
	c.leaseOnce.Do(func() {
		c.writeLease = make(chan struct{}, 1)
		c.writeLease <- struct{}{}
		c.commitLease = make(chan struct{}, 1)
		c.commitLease <- struct{}{}
	})
}

func (c *writeCoordinator) acquireWrite(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initLease()
	select {
	case <-c.writeLease:
		if err := ctx.Err(); err != nil {
			c.releaseWrite()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *writeCoordinator) tryAcquireWrite(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initLease()
	select {
	case <-c.writeLease:
		if err := ctx.Err(); err != nil {
			c.releaseWrite()
			return err
		}
		return nil
	default:
		return ErrTopWriteInvalidated
	}
}

func (c *writeCoordinator) acquireCommit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.initLease()
	select {
	case <-c.commitLease:
		if err := ctx.Err(); err != nil {
			c.releaseCommit()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *writeCoordinator) releaseCommit() {
	c.commitLease <- struct{}{}
}

func (c *writeCoordinator) releaseWrite() {
	c.writeLease <- struct{}{}
}

func (c *writeCoordinator) beginBestEffort(ctx context.Context, expected *uint64) (uint64, error) {
	if err := c.tryAcquireWrite(ctx); err != nil {
		return 0, err
	}
	generation, err := c.reserveBestEffort(ctx, expected)
	if err != nil {
		c.releaseWrite()
		return 0, err
	}
	return generation, nil
}

func (c *writeCoordinator) beginWithFill(ctx context.Context, expected uint64, fill *topFillSink) (uint64, error) {
	if err := c.tryAcquireWrite(ctx); err != nil {
		return 0, err
	}
	generation, err := c.reserveWithFill(ctx, expected, fill)
	if err != nil {
		c.releaseWrite()
		return 0, err
	}
	return generation, nil
}

// begin serializes same-key top writes for stores that cannot stage separately.
// It reserves a generation while holding the write lease for the writer
// lifetime, preserving the legacy compatibility path for external stores.
func (c *writeCoordinator) begin(ctx context.Context, expected *uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.acquireWrite(ctx); err != nil {
		return 0, err
	}
	c.stateMu.Lock()
	if expected == nil {
		if err := c.waitForNoActiveDeletesLocked(ctx); err != nil {
			c.stateMu.Unlock()
			c.releaseWrite()
			return 0, err
		}
	} else if c.activeDeletes > 0 {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, ErrTopWriteInvalidated
	}
	if err := ctx.Err(); err != nil {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, err
	}
	if expected != nil && c.latestGenerationLocked() != *expected {
		c.stateMu.Unlock()
		c.releaseWrite()
		return 0, ErrTopWriteInvalidated
	}
	generation := c.reserveGenerationLocked()
	c.stateMu.Unlock()
	return generation, nil
}

func (c *writeCoordinator) unregisterActiveFill(fill *topFillSink) {
	c.init()
	c.stateMu.Lock()
	if c.activeFill == fill {
		c.activeFill = nil
	}
	c.stateMu.Unlock()
}

func (c *writeCoordinator) preemptActiveFill() {
	c.init()
	c.stateMu.Lock()
	fill := c.activeFill
	c.stateMu.Unlock()
	if fill != nil {
		_ = fill.Preempt()
	}
}

func (c *writeCoordinator) preemptActiveFillForWrite() func() {
	c.init()
	c.stateMu.Lock()
	fill := c.activeFill
	if fill != nil {
		c.fillPreemptions++
	}
	c.stateMu.Unlock()
	if fill != nil {
		_ = fill.Preempt()
	}
	return func() {
		if fill == nil {
			return
		}
		c.stateMu.Lock()
		if c.fillPreemptions > 0 {
			c.fillPreemptions--
		}
		c.stateMu.Unlock()
	}
}

func (c *writeCoordinator) rollbackAndUnlock(generation uint64) {
	c.unregisterReservation(generation)
	c.releaseWrite()
}

func (c *writeCoordinator) beginDelete(ctx context.Context) error {
	if err := c.acquireCommit(ctx); err != nil {
		return err
	}
	c.init()
	c.stateMu.Lock()
	if c.activeDeletes == 0 {
		c.activeDeletesDone = make(chan struct{})
	}
	c.activeDeletes++
	c.stateMu.Unlock()
	c.preemptActiveFill()
	return nil
}

func (c *writeCoordinator) finishDelete(success bool) {
	c.init()
	c.stateMu.Lock()
	if success {
		c.advanceCommittedLocked()
	}
	if c.activeDeletes > 0 {
		c.activeDeletes--
		if c.activeDeletes == 0 {
			close(c.activeDeletesDone)
		}
	}
	c.stateMu.Unlock()
	c.releaseCommit()
}

func (c *DaramjweeCache) currentTopWriteGeneration(key string) *topWriteGeneration {
	return c.topWrites.currentGeneration(key)
}

func (c *DaramjweeCache) noteTopWriteGeneration(key string) {
	coord := c.topWrites.coordinator(key)
	defer coord.releaseReference()
	coord.stateMu.Lock()
	coord.advanceCommittedLocked()
	coord.stateMu.Unlock()
}

func (c *DaramjweeCache) setStreamToTopStoreWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *topWriteGeneration) (WriteSink, error) {
	store := c.topWriteStore()
	coord, expected, err := c.topWrites.coordinatorForWrite(key, expectedGeneration)
	if err != nil {
		return nil, err
	}
	if expectedGeneration == nil {
		unblockFills := coord.preemptActiveFillForWrite()
		defer unblockFills()
	}
	if staging, ok := store.(StagingStore); ok {
		generation, err := coord.reserve(ctx, expected) //nolint:govet // shadow: sequential error handling in same block
		if err != nil {
			coord.releaseReference()
			return nil, err
		}
		sink, err := staging.BeginStagedSet(ctx, key, metadata) //nolint:govet // shadow: sequential error handling in same block
		if err != nil {
			coord.unregisterReservation(generation)
			coord.releaseReference()
			return nil, err
		}
		return &coordinatedStagedTopWriteSink{
			sink:          sink,
			coord:         coord,
			generation:    generation,
			waitTimeout:   c.config.closeTimeout,
			onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		}, nil
	}
	generation, err := coord.begin(ctx, expected)
	if err != nil {
		coord.releaseReference()
		return nil, err
	}

	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		coord.rollbackAndUnlock(generation)
		coord.releaseReference()
		return nil, err
	}

	return &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		generation:    generation,
		waitTimeout:   c.config.closeTimeout,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
	}, nil
}

func (c *DaramjweeCache) setStreamToTopStoreBestEffortWithGeneration(ctx context.Context, key string, metadata *Metadata, expectedGeneration *topWriteGeneration) (WriteSink, error) {
	store := c.topWriteStore()
	coord, expected, err := c.topWrites.coordinatorForWrite(key, expectedGeneration)
	if err != nil {
		return nil, err
	}
	if staging, ok := store.(StagingStore); ok {
		generation, err := coord.reserveBestEffort(ctx, expected) //nolint:govet // shadow: sequential error handling in same block
		if err != nil {
			coord.releaseReference()
			return nil, err
		}
		sink, err := staging.BeginStagedSet(ctx, key, metadata) //nolint:govet // shadow: sequential error handling in same block
		if err != nil {
			coord.unregisterReservation(generation)
			coord.releaseReference()
			return nil, err
		}
		return &coordinatedStagedTopWriteSink{
			sink:          sink,
			coord:         coord,
			generation:    generation,
			waitTimeout:   c.config.closeTimeout,
			onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		}, nil
	}

	generation, err := coord.beginBestEffort(ctx, expected)
	if err != nil {
		coord.releaseReference()
		return nil, err
	}
	sink, err := store.BeginSet(ctx, key, metadata)
	if err != nil {
		coord.rollbackAndUnlock(generation)
		coord.releaseReference()
		return nil, err
	}
	return &coordinatedTopWriteSink{
		WriteSink:     sink,
		coord:         coord,
		generation:    generation,
		waitTimeout:   c.config.closeTimeout,
		onInvalidated: func() error { return c.deleteTopStoreKey(key) },
	}, nil
}

func (c *DaramjweeCache) setStreamToTopStoreForFill(ctx context.Context, key string, metadata *Metadata, expectedGeneration *topWriteGeneration) (WriteSink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	store := c.topWriteStore()
	coord, expected, err := c.topWrites.coordinatorForWrite(key, expectedGeneration)
	if err != nil {
		return nil, err
	}
	if expected == nil {
		coord.releaseReference()
		return nil, ErrTopWriteInvalidated
	}
	var generation uint64

	if staging, ok := store.(StagingStore); ok {
		fillCtx, cancelFill := context.WithCancel(ctx)
		fill := newPendingTopFillSink(coord, func() {
			coord.unregisterReservation(generation)
			coord.releaseReference()
		}, cancelFill)
		fill.reportPreemptOnClose = true
		generation, err = coord.reserveWithFill(fillCtx, *expected, fill)
		if err != nil {
			cancelFill()
			coord.releaseReference()
			return nil, err
		}
		sink, err := staging.BeginStagedSet(fillCtx, key, metadata) //nolint:govet // shadow: sequential error handling in same block
		if err != nil {
			fill.failBeginSet(err)
			return nil, err
		}
		topWriter := &coordinatedStagedTopWriteSink{
			sink:          sink,
			coord:         coord,
			generation:    generation,
			waitTimeout:   c.config.closeTimeout,
			onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		}
		if fill.attach(topWriter) {
			fill.startLease(c.config.fillLeaseTimeout)
		} else {
			_ = fill.finishPreemptedAttach(topWriter)
		}
		return fill, nil
	}

	fillCtx, cancelFill := context.WithCancel(ctx)
	fill := newPendingTopFillSink(coord, func() {
		coord.rollbackAndUnlock(generation)
		coord.releaseReference()
	}, cancelFill)
	generation, err = coord.beginWithFill(fillCtx, *expected, fill)
	if err != nil {
		cancelFill()
		coord.releaseReference()
		return nil, err
	}
	type beginResult struct {
		sink WriteSink
		err  error
	}
	beginDone := make(chan beginResult, 1)
	go func() {
		sink, err := store.BeginSet(fillCtx, key, metadata)
		beginDone <- beginResult{sink: sink, err: err}
	}()

	finishBegin := func(result beginResult) error {
		if result.err != nil {
			fill.failBeginSet(result.err)
			if fill.isPreempted() {
				return nil
			}
			return result.err
		}
		topWriter := &coordinatedTopWriteSink{
			WriteSink:     result.sink,
			coord:         coord,
			generation:    generation,
			waitTimeout:   c.config.closeTimeout,
			onInvalidated: func() error { return c.deleteTopStoreKey(key) },
		}
		if fill.attach(topWriter) {
			fill.startLease(c.config.fillLeaseTimeout)
		} else {
			_ = fill.finishPreemptedAttach(topWriter)
		}
		return nil
	}

	select {
	case result := <-beginDone:
		if err := finishBegin(result); err != nil {
			return nil, err
		}
	case <-fill.preemptedSignal():
		go func() {
			_ = finishBegin(<-beginDone)
		}()
	case <-fillCtx.Done():
		_ = fill.Preempt()
		go func() {
			_ = finishBegin(<-beginDone)
		}()
		return nil, fillCtx.Err()
	}
	return fill, nil
}

func (c *DaramjweeCache) deleteTopStoreKey(key string) error {
	store := c.topWriteStore()
	if !hasRealStore(store) {
		return nil
	}
	ctx, cancel := c.newCtxWithTimeout(context.Background())
	defer cancel()
	err := c.deleteFromStore(ctx, store, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}
