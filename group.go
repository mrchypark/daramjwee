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

	closed atomic.Bool
	mu     sync.Mutex
	caches map[string]*DaramjweeCache
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

	rt, err := newGroupRuntime(logger, cfg.Workers, cfg.WorkerQueueDefault, cfg.WorkerTimeout)
	if err != nil {
		return nil, err
	}

	return &cacheGroup{
		logger: logger,
		cfg:    cfg,
		rt:     rt,
		caches: make(map[string]*DaramjweeCache),
	}, nil
}

func (g *cacheGroup) NewCache(name string, opts ...Option) (Cache, error) {
	if g.closed.Load() {
		return nil, &ConfigError{"cache group is closed"}
	}
	if name == "" {
		return nil, &ConfigError{"cache name cannot be empty"}
	}

	g.mu.Lock()
	if _, exists := g.caches[name]; exists {
		g.mu.Unlock()
		return nil, &ConfigError{fmt.Sprintf("duplicate cache name %q", name)}
	}
	g.mu.Unlock()

	cfg, err := buildCacheConfig(cacheConstructionGroup, &g.cfg, opts...)
	if err != nil {
		return nil, err
	}

	cache, err := newCacheFromConfig(g.logger, g.rt, name, cfg)
	if err != nil {
		return nil, err
	}

	typed, ok := cache.(*DaramjweeCache)
	if !ok {
		return nil, &ConfigError{"unexpected cache implementation"}
	}
	typed.closeHook = func() {
		g.mu.Lock()
		delete(g.caches, name)
		g.mu.Unlock()
	}

	g.mu.Lock()
	if g.closed.Load() {
		g.mu.Unlock()
		typed.Close()
		return nil, &ConfigError{"cache group is closed"}
	}
	g.caches[name] = typed
	g.mu.Unlock()
	return cache, nil
}

func (g *cacheGroup) Close() {
	if g.closed.Swap(true) {
		return
	}
	g.mu.Lock()
	caches := make([]*DaramjweeCache, 0, len(g.caches))
	for _, cache := range g.caches {
		caches = append(caches, cache)
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
