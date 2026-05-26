package daramjwee

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// CacheGroup owns a shared background runtime for multiple caches.
type CacheGroup interface {
	NewCache(name string, opts ...Option) (Cache, error)
	Close()
}

type cacheGroup struct {
	logger log.Logger
	cfg    GroupConfig
	rt     *groupRuntime

	closed         atomic.Bool
	registrationMu sync.Mutex
	mu             sync.Mutex
	cond           *sync.Cond
	constructing   int
	caches         map[string]*DaramjweeCache
}

var _ CacheGroup = (*cacheGroup)(nil)

// NewGroup constructs a CacheGroup with a shared bounded runtime.
func NewGroup(logger log.Logger, opts ...GroupOption) (CacheGroup, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cfg := GroupConfig{
		Workers:            1,
		WorkerTimeout:      30 * time.Second,
		WorkerQueueDefault: 500,
		CloseTimeout:       30 * time.Second,
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	group := &cacheGroup{
		logger: logger,
		cfg:    cfg,
		rt:     newGroupRuntime(logger, cfg.Workers, cfg.WorkerTimeout),
		caches: make(map[string]*DaramjweeCache),
	}
	group.cond = sync.NewCond(&group.mu)
	return group, nil
}

func (g *cacheGroup) NewCache(name string, opts ...Option) (Cache, error) {
	if g.closed.Load() {
		return nil, &ConfigError{"cache group is closed"}
	}
	if name == "" {
		return nil, &ConfigError{"cache name cannot be empty"}
	}

	g.mu.Lock()
	if g.closed.Load() {
		g.mu.Unlock()
		return nil, &ConfigError{"cache group is closed"}
	}
	if _, exists := g.caches[name]; exists {
		g.mu.Unlock()
		return nil, &ConfigError{fmt.Sprintf("duplicate cache name %q", name)}
	}
	g.caches[name] = nil
	g.constructing++
	g.mu.Unlock()
	cleanupConstruction := true
	defer func() {
		if !cleanupConstruction {
			return
		}
		g.mu.Lock()
		if current, exists := g.caches[name]; exists && current == nil {
			delete(g.caches, name)
		}
		g.constructing--
		g.cond.Broadcast()
		g.mu.Unlock()
	}()

	cfg, fillLeaseTimeout, err := buildCacheConfig(cacheConstructionGroup, &g.cfg, opts...)
	if err != nil {
		return nil, err
	}

	g.registrationMu.Lock()
	g.mu.Lock()
	if g.closed.Load() {
		g.mu.Unlock()
		g.registrationMu.Unlock()
		return nil, &ConfigError{"cache group is closed"}
	}
	g.mu.Unlock()
	cache, err := newCacheFromConfig(g.logger, g.rt, name, cfg, fillLeaseTimeout)
	g.registrationMu.Unlock()
	if err != nil {
		return nil, err
	}

	typed, ok := cache.(*DaramjweeCache)
	if !ok {
		return nil, &ConfigError{"unexpected cache implementation"}
	}
	typed.closeHook = func() {
		g.mu.Lock()
		if g.caches[name] == typed {
			delete(g.caches, name)
		}
		g.mu.Unlock()
	}

	g.mu.Lock()
	if g.closed.Load() {
		g.mu.Unlock()
		typed.Close()
		return nil, &ConfigError{"cache group is closed"}
	}
	if current, exists := g.caches[name]; !exists || current != nil {
		g.mu.Unlock()
		typed.Close()
		return nil, &ConfigError{fmt.Sprintf("duplicate cache name %q", name)}
	}
	g.caches[name] = typed
	cleanupConstruction = false
	g.constructing--
	g.cond.Broadcast()
	g.mu.Unlock()
	return cache, nil
}

func (g *cacheGroup) Close() {
	g.registrationMu.Lock()
	if g.closed.Swap(true) {
		g.registrationMu.Unlock()
		return
	}
	g.registrationMu.Unlock()
	g.mu.Lock()
	for g.constructing > 0 {
		g.cond.Wait()
	}
	caches := make([]*DaramjweeCache, 0, len(g.caches))
	for _, cache := range g.caches {
		if cache != nil {
			caches = append(caches, cache)
		}
	}
	g.mu.Unlock()

	if g.rt != nil {
		if err := g.rt.Shutdown(g.cfg.CloseTimeout); err != nil {
			level.Warn(g.logger).Log("msg", "cache group shutdown failed", "err", err)
		}
	}

	for _, cache := range caches {
		cache.Close()
	}
}
