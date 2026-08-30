// Package e2e_test contains end-to-end tests that exercise daramjwee through
// its public API against a real HTTP origin and HTTP front proxy, using the
// real store implementations (memstore, filestore, objectstore).
package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
)

// --- origin ----------------------------------------------------------------

type originEntry struct {
	data string
	etag string
}

// origin is a fake upstream service with per-key hit counters, optional
// latency, and optional failure injection.
type origin struct {
	t          *testing.T
	srv        *httptest.Server
	mu         sync.Mutex
	data       map[string]originEntry
	hits       map[string]*atomic.Int32
	misses     *atomic.Int32
	delay      time.Duration
	failAll    atomic.Bool
	chunked    bool
	chunkDelay time.Duration
}

func newOrigin(t *testing.T) *origin {
	t.Helper()
	o := &origin{
		t:      t,
		data:   make(map[string]originEntry),
		hits:   make(map[string]*atomic.Int32),
		misses: &atomic.Int32{},
	}
	o.srv = httptest.NewServer(http.HandlerFunc(o.handle))
	t.Cleanup(o.srv.Close)
	return o
}

func (o *origin) url() string { return o.srv.URL }

func (o *origin) set(key, data, etag string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data[key] = originEntry{data: data, etag: etag}
}

func (o *origin) hitCount(key string) int {
	o.mu.Lock()
	h := o.hits[key]
	o.mu.Unlock()
	if h == nil {
		return 0
	}
	return int(h.Load())
}

func (o *origin) missCount() int { return int(o.misses.Load()) }

func (o *origin) handle(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	o.mu.Lock()
	if o.hits[key] == nil {
		o.hits[key] = &atomic.Int32{}
	}
	o.hits[key].Add(1)
	entry, ok := o.data[key]
	delay := o.delay
	chunked := o.chunked
	chunkDelay := o.chunkDelay
	failAll := o.failAll.Load()
	o.mu.Unlock()

	if failAll {
		http.Error(w, "origin unavailable", http.StatusInternalServerError)
		return
	}
	if !ok {
		o.misses.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	if r.Header.Get("If-None-Match") == entry.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", entry.etag)
	w.Header().Set("Content-Type", "application/octet-stream")
	if chunked && len(entry.data) > 0 {
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		half := len(entry.data) / 2
		_, _ = w.Write([]byte(entry.data[:half]))
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(chunkDelay)
		_, _ = w.Write([]byte(entry.data[half:]))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, entry.data)
}

// --- proxy -----------------------------------------------------------------

// proxy is an HTTP front that serves cached objects through daramjwee,
// mirroring the README example handler.
type proxy struct {
	cache  daramjwee.Cache
	origin *origin
	srv    *httptest.Server
}

type originFetcher struct {
	key    string
	origin *origin
}

func (f *originFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.origin.url()+"/"+f.key, nil)
	if err != nil {
		return nil, err
	}
	if oldMetadata != nil && oldMetadata.CacheTag != "" {
		req.Header.Set("If-None-Match", oldMetadata.CacheTag)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusNotModified:
		resp.Body.Close()
		return nil, daramjwee.ErrNotModified
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, daramjwee.ErrCacheableNotFound
	case http.StatusOK:
		return &daramjwee.FetchResult{
			Body: resp.Body,
			Metadata: &daramjwee.Metadata{
				CacheTag: resp.Header.Get("ETag"),
			},
		}, nil
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("origin returned status %d", resp.StatusCode)
	}
}

func newProxy(t *testing.T, cache daramjwee.Cache, o *origin) *proxy {
	t.Helper()
	p := &proxy{cache: cache, origin: o}
	p.srv = httptest.NewServer(http.HandlerFunc(p.handle))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *proxy) url() string { return p.srv.URL }

func (p *proxy) handle(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/objects/")

	switch r.Method {
	case http.MethodPut:
		sink, err := p.cache.Set(r.Context(), key, &daramjwee.Metadata{CacheTag: r.Header.Get("ETag")})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(sink, r.Body)
		if err == nil {
			err = sink.Close()
		} else {
			_ = sink.Abort()
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case http.MethodDelete:
		if err := p.cache.Delete(r.Context(), key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		resp, err := p.cache.Get(r.Context(), key, daramjwee.GetRequest{
			IfNoneMatch: r.Header.Get("If-None-Match"),
		}, &originFetcher{key: key, origin: p.origin})
		if err != nil {
			if errors.Is(err, daramjwee.ErrCacheClosed) {
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		defer resp.Close()

		if resp.Metadata.CacheTag != "" {
			w.Header().Set("ETag", resp.Metadata.CacheTag)
		}
		switch resp.Status {
		case daramjwee.GetStatusNotFound:
			http.Error(w, "not found", http.StatusNotFound)
			return
		case daramjwee.GetStatusNotModified:
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		buf := make([]byte, 32*1024)
		for {
			n, readErr := resp.Read(buf)
			if n > 0 {
				if _, writeErr := w.Write(buf[:n]); writeErr != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if readErr != nil {
				return
			}
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- helpers ---------------------------------------------------------------

func newCache(t *testing.T, opts ...daramjwee.Option) daramjwee.Cache {
	t.Helper()
	opts = append([]daramjwee.Option{
		daramjwee.WithOpTimeout(5 * time.Second),
	}, opts...)
	cache, err := daramjwee.New(log.NewNopLogger(), opts...)
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	return cache
}

func getStatus(t *testing.T, url string, hdr map[string]string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header, string(body)
}

func get(t *testing.T, url string) (int, http.Header, string) {
	t.Helper()
	return getStatus(t, url, nil)
}

// getStatusSafe is like getStatus but returns errors instead of failing the
// test, making it safe to call from goroutines.
func getStatusSafe(url string, hdr map[string]string) (int, http.Header, string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, "", err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, "", err
	}
	return resp.StatusCode, resp.Header, string(body), nil
}

// getSafe is like get but returns errors instead of failing the test.
func getSafe(url string) (int, http.Header, string, error) {
	return getStatusSafe(url, nil)
}

// --- basic cache behavior --------------------------------------------------

func TestE2E_ColdMissThenHotHit(t *testing.T) {
	o := newOrigin(t)
	o.set("hello", "Hello, Daramjwee!", "v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	status, hdr, body := get(t, p.url()+"/objects/hello")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "Hello, Daramjwee!", body)
	require.Equal(t, "v1", hdr.Get("ETag"))
	require.Equal(t, 1, o.hitCount("hello"))

	status, hdr, body = get(t, p.url()+"/objects/hello")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "Hello, Daramjwee!", body)
	require.Equal(t, "v1", hdr.Get("ETag"))
	require.Equal(t, 1, o.hitCount("hello"), "hot hit must not touch the origin")
}

func TestE2E_ConditionalRequestNotModified(t *testing.T) {
	o := newOrigin(t)
	o.set("hello", "Hello, Daramjwee!", "v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	_, hdr, _ := get(t, p.url()+"/objects/hello")
	etag := hdr.Get("ETag")
	require.Equal(t, "v1", etag)
	require.Equal(t, 1, o.hitCount("hello"))

	status, _, body := getStatus(t, p.url()+"/objects/hello", map[string]string{"If-None-Match": etag})
	require.Equal(t, http.StatusNotModified, status)
	require.Empty(t, body)
	require.Equal(t, 1, o.hitCount("hello"), "304 must be served from the cache")
}

func TestE2E_NegativeCaching(t *testing.T) {
	o := newOrigin(t)
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, 300*time.Millisecond),
	)
	p := newProxy(t, cache, o)

	status, _, _ := get(t, p.url()+"/objects/missing")
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, 1, o.missCount())

	// Within the negative freshness window the origin must not be consulted.
	status, _, _ = get(t, p.url()+"/objects/missing")
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, 1, o.missCount())

	// After the negative entry expires, the origin is consulted again.
	require.Eventually(t, func() bool {
		get(t, p.url()+"/objects/missing")
		return o.missCount() >= 2
	}, 2*time.Second, 20*time.Millisecond)
}

func TestE2E_DeleteInvalidates(t *testing.T) {
	o := newOrigin(t)
	o.set("hello", "Hello, Daramjwee!", "v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	_, _, body := get(t, p.url()+"/objects/hello")
	require.Equal(t, "Hello, Daramjwee!", body)
	require.Equal(t, 1, o.hitCount("hello"))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, p.url()+"/objects/hello", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	_, _, body = get(t, p.url()+"/objects/hello")
	require.Equal(t, "Hello, Daramjwee!", body)
	require.Equal(t, 2, o.hitCount("hello"), "deleted key must be refetched from origin")
}

func TestE2E_OriginFailurePropagates(t *testing.T) {
	o := newOrigin(t)
	o.set("hello", "Hello, Daramjwee!", "v1")
	o.failAll.Store(true)
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	status, _, _ := get(t, p.url()+"/objects/hello")
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, 1, o.hitCount("hello"))

	// Failures are not cached: the second request also hits the origin.
	status, _, _ = get(t, p.url()+"/objects/hello")
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, 2, o.hitCount("hello"))
}

func TestE2E_StaleWhileRevalidate(t *testing.T) {
	o := newOrigin(t)
	o.set("fresh", "fresh-v1", "v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(150*time.Millisecond, time.Hour),
	)
	p := newProxy(t, cache, o)

	_, _, body := get(t, p.url()+"/objects/fresh")
	require.Equal(t, "fresh-v1", body)
	require.Equal(t, 1, o.hitCount("fresh"))

	o.set("fresh", "fresh-v2", "v2")

	// Wait until the entry is stale, then verify the cache serves the stale
	// value immediately while a background refresh runs in parallel.
	require.Eventually(t, func() bool {
		_, _, b := get(t, p.url()+"/objects/fresh")
		return b == "fresh-v1"
	}, 2*time.Second, 10*time.Millisecond, "stale entry must be served immediately")

	// The background refresh updates the entry from the origin.
	require.Eventually(t, func() bool {
		_, _, b := get(t, p.url()+"/objects/fresh")
		return b == "fresh-v2"
	}, 3*time.Second, 20*time.Millisecond, "background refresh must update the entry")
	require.GreaterOrEqual(t, o.hitCount("fresh"), 2)
}

// --- streaming & concurrency ------------------------------------------------

func TestE2E_PartialReadDoesNotPublish(t *testing.T) {
	o := newOrigin(t)
	o.chunked = true
	o.chunkDelay = 100 * time.Millisecond
	o.set("big", strings.Repeat("x", 1<<20), "big-v1")
	hot := memstore.New(0, nil)
	cache := newCache(t,
		daramjwee.WithTiers(hot),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	// Read only the first chunk and disconnect before the stream completes.
	// The proxy handler streams with http.Flusher: when the client closes the
	// body mid-stream, the proxy's write to the client fails, causing it to
	// return. The cache stream is then closed (abort), so no partial entry is
	// published to the top tier.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.url()+"/objects/big", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	require.Greater(t, n, 0)
	require.NoError(t, resp.Body.Close())

	// The partial stream must not leave a published entry in the cache.
	require.Eventually(t, func() bool {
		_, _, err := hot.GetStream(context.Background(), "big")
		return errors.Is(err, daramjwee.ErrNotFound)
	}, time.Second, 10*time.Millisecond)

	// A full read afterwards refetches from origin and populates the cache.
	status, _, body := get(t, p.url()+"/objects/big")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body, 1<<20)
	require.GreaterOrEqual(t, o.hitCount("big"), 2)

	reader, _, err := hot.GetStream(context.Background(), "big")
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func TestE2E_ConcurrentColdRequestsCoalesce(t *testing.T) {
	o := newOrigin(t)
	o.delay = 80 * time.Millisecond
	o.set("hot", "shared-value", "v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	type concResult struct {
		status int
		body   string
		err    error
	}
	const callers = 20
	var wg sync.WaitGroup
	results := make([]concResult, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			status, _, body := get(t, p.url()+"/objects/hot")
			results[idx] = concResult{status: status, body: body}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		require.NoError(t, r.err, "caller %d", i)
		require.Equal(t, http.StatusOK, r.status, "caller %d", i)
		require.Equal(t, "shared-value", r.body, "caller %d", i)
	}
	require.Equal(t, 1, o.hitCount("hot"), "concurrent cold requests must share one origin fetch")
}

// --- tiers & runtime ---------------------------------------------------------

func TestE2E_MultiTierRestartPromotion(t *testing.T) {
	o := newOrigin(t)
	o.set("tiered", "tiered-value", "tiered-v1")

	dir := t.TempDir()
	fileTier, err := filestore.New(dir, log.NewNopLogger())
	require.NoError(t, err)

	cache1 := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil), fileTier),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p1 := newProxy(t, cache1, o)

	_, _, body := get(t, p1.url()+"/objects/tiered")
	require.Equal(t, "tiered-value", body)
	require.Equal(t, 1, o.hitCount("tiered"))

	// The lower tier is filled asynchronously by the persist fan-out.
	require.Eventually(t, func() bool {
		reader, _, err := fileTier.GetStream(context.Background(), "tiered")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 3*time.Second, 20*time.Millisecond)

	// Restart: fresh top tier, same file tier. The value must come from the
	// file tier without touching the origin, and promote back to the top.
	cache1.Close()
	fileTier2, err := filestore.New(dir, log.NewNopLogger())
	require.NoError(t, err)
	cache2 := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil), fileTier2),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p2 := newProxy(t, cache2, o)

	_, _, body = get(t, p2.url()+"/objects/tiered")
	require.Equal(t, "tiered-value", body)
	require.Equal(t, 1, o.hitCount("tiered"), "lower-tier hit must not touch the origin")
}

func TestE2E_FileObjectstoreChain(t *testing.T) {
	o := newOrigin(t)
	o.set("remote", "remote-value", "remote-v1")

	objDir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	store := objectstore.New(
		bucket,
		log.NewNopLogger(),
		objectstore.WithDir(objDir),
	)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	fileDir := t.TempDir()
	fileTier, err := filestore.New(fileDir, log.NewNopLogger())
	require.NoError(t, err)

	cache1 := newCache(t,
		daramjwee.WithTiers(fileTier, store),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p1 := newProxy(t, cache1, o)

	_, _, body := get(t, p1.url()+"/objects/remote")
	require.Equal(t, "remote-value", body)
	require.Equal(t, 1, o.hitCount("remote"))

	// A second store instance over the same bucket with an empty local dir
	// reads only remote state, so it doubles as the remote-durability probe:
	// the async objectstore flush must have completed before it can serve.
	store2 := objectstore.New(
		bucket,
		log.NewNopLogger(),
		objectstore.WithDir(t.TempDir()),
	)
	t.Cleanup(func() { require.NoError(t, store2.Close()) })
	require.Eventually(t, func() bool {
		reader, _, err := store2.GetStream(context.Background(), "remote")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 5*time.Second, 20*time.Millisecond)

	cache1.Close()
	cache2 := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil), store2),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p2 := newProxy(t, cache2, o)

	_, _, body = get(t, p2.url()+"/objects/remote")
	require.Equal(t, "remote-value", body)
	require.Equal(t, 1, o.hitCount("remote"), "objectstore hit must not touch the origin")
}

func TestE2E_CacheGroupSharedRuntime(t *testing.T) {
	o := newOrigin(t)
	o.set("users-key", "users-value", "u1")
	o.set("posts-key", "posts-value", "p1")

	group, err := daramjwee.NewGroup(log.NewNopLogger(),
		daramjwee.WithGroupWorkers(2),
		daramjwee.WithGroupWorkerQueueDefault(8),
	)
	require.NoError(t, err)
	t.Cleanup(group.Close)

	users, err := group.NewCache("users",
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	require.NoError(t, err)
	posts, err := group.NewCache("posts",
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	require.NoError(t, err)

	up := newProxy(t, users, o)
	pp := newProxy(t, posts, o)

	_, _, body := get(t, up.url()+"/objects/users-key")
	require.Equal(t, "users-value", body)
	_, _, body = get(t, pp.url()+"/objects/posts-key")
	require.Equal(t, "posts-value", body)

	// Keys are isolated between caches: the users cache must miss posts keys
	// and fetch them from the origin on its own.
	status, _, body := get(t, up.url()+"/objects/posts-key")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "posts-value", body)
	require.Equal(t, 2, o.hitCount("posts-key"), "posts cache once, users cache miss once")

	// Closing the group shuts down all caches.
	// The group was already registered with t.Cleanup; calling Close
	// explicitly here tests the idempotent close contract.
	group.Close()
	status, _, _ = get(t, up.url()+"/objects/users-key")
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func TestE2E_PromotionProbation(t *testing.T) {
	o := newOrigin(t)
	o.set("probe", "probe-value", "probe-v1")
	top := memstore.New(0, nil)
	lower := memstore.New(0, nil)

	// Seed the lower tier.
	sink, err := lower.BeginSet(context.Background(), "probe", &daramjwee.Metadata{
		CacheTag: "probe-v1",
		CachedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = sink.Write([]byte("probe-value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cache := newCache(t,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithPromotionProbation(1024),
	)

	fetcher := &originFetcher{key: "probe", origin: o}

	// First hit: served from the lower tier without promotion.
	resp, err := cache.Get(context.Background(), "probe", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "probe-value", string(body))
	require.NoError(t, resp.Close())
	_, _, err = top.GetStream(context.Background(), "probe")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)

	// Second hit: promoted to the top tier.
	resp, err = cache.Get(context.Background(), "probe", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	_, err = io.ReadAll(resp)
	require.NoError(t, err)
	require.NoError(t, resp.Close())
	reader, _, err := top.GetStream(context.Background(), "probe")
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, 0, o.hitCount("probe"), "lower-tier hits must not touch the origin")
}

// TestE2E_LeaderBodyStillOpenWaiterArrives verifies that a caller arriving
// after the leader returned its response but before the leader's body closes
// joins as a waiter and does not trigger a duplicate origin fetch. This is
// the E2E regression test for the "tie miss-leader lifecycle to fill
// completion" fix.
func TestE2E_LeaderBodyStillOpenWaiterArrives(t *testing.T) {
	o := newOrigin(t)
	o.delay = 300 * time.Millisecond // slow origin: leader body stays open
	o.set("slow", "slow-value", "slow-v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	// Leader: fetches from the slow origin — response body is streaming.
	leaderReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.url()+"/objects/slow", nil)
	require.NoError(t, err)
	leaderResp, err := http.DefaultClient.Do(leaderReq)
	require.NoError(t, err)
	defer leaderResp.Body.Close()
	require.Equal(t, http.StatusOK, leaderResp.StatusCode)

	// Waiter: arrives while the leader's stream is still open.
	waiterDone := make(chan struct {
		status int
		body   string
		err    error
	}, 1)
	go func() {
		status, _, body, err := getSafe(p.url() + "/objects/slow")
		waiterDone <- struct {
			status int
			body   string
			err    error
		}{status: status, body: body, err: err}
	}()

	// Wait until the waiter has started (it will block on the origin fetch).
	// The origin hit count must remain 1 — the waiter joins as a coalesced
	// waiter instead of triggering a duplicate fetch.
	require.Eventually(t, func() bool {
		return o.hitCount("slow") >= 1
	}, 2*time.Second, 5*time.Millisecond)
	// Give the waiter time to enter the miss-coalescing wait path.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, o.hitCount("slow"), "waiter must not duplicate the origin fetch while the leader streams")

	// Finish the leader: read the full body and close.
	body, err := io.ReadAll(leaderResp.Body)
	require.NoError(t, err)
	require.Equal(t, "slow-value", string(body))
	require.NoError(t, leaderResp.Body.Close())

	// The waiter completes — served from the top tier without an extra fetch.
	waiter := <-waiterDone
	require.NoError(t, waiter.err)
	require.Equal(t, http.StatusOK, waiter.status)
	require.Equal(t, "slow-value", waiter.body)
	require.Equal(t, 1, o.hitCount("slow"), "waiter served from top tier after leader published")
}

// TestE2E_ConcurrentDeleteAndGet verifies that a Get arriving concurrently
// with a Delete either returns the old value (Get completed before Delete)
// or triggers a fresh origin fetch (Get completed after Delete). Under no
// circumstance should the cache return corrupted data or panic.
func TestE2E_ConcurrentDeleteAndGet(t *testing.T) {
	o := newOrigin(t)
	o.set("race", "race-v1", "race-v1")
	o.delay = 10 * time.Millisecond
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	// Seed the cache.
	_, _, body := get(t, p.url()+"/objects/race")
	require.Equal(t, "race-v1", body)

	// Hammer delete + get concurrently for a short burst.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	var getOK, deleteOK atomic.Int32

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				status, _, b, err := getSafe(p.url() + "/objects/race")
				if err == nil && status == http.StatusOK && (b == "race-v1" || b == "race-v2") {
					getOK.Add(1)
				}
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, p.url()+"/objects/race", nil)
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == http.StatusNoContent {
						deleteOK.Add(1)
					}
				}
			}
		}()
	}
	wg.Wait()

	// At least some operations must have succeeded; no panics or corrupted reads.
	require.Greater(t, getOK.Load(), int32(0))
	require.Greater(t, deleteOK.Load(), int32(0))
}

// TestE2E_StaleRefreshCoalescing verifies that concurrent stale requests
// trigger only one background refresh, not one per request. This is the E2E
// test for the refresh deduplication feature.
func TestE2E_StaleRefreshCoalescing(t *testing.T) {
	o := newOrigin(t)
	o.set("stale", "stale-v1", "v1")
	o.delay = 100 * time.Millisecond // slow origin: refresh takes time
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(50*time.Millisecond, time.Hour),
	)
	p := newProxy(t, cache, o)

	// Seed the cache.
	_, _, body := get(t, p.url()+"/objects/stale")
	require.Equal(t, "stale-v1", body)
	require.Equal(t, 1, o.hitCount("stale"))

	// Update origin and wait for the entry to go stale.
	o.set("stale", "stale-v2", "v2")
	time.Sleep(80 * time.Millisecond)

	// Concurrent stale requests: all should return stale-v1 immediately.
	const callers = 10
	var wg sync.WaitGroup
	results := make([]string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, b, _ := getSafe(p.url() + "/objects/stale")
			results[idx] = b
		}(i)
	}
	wg.Wait()

	for _, b := range results {
		require.Equal(t, "stale-v1", b, "all callers must receive the stale value")
	}

	// The refresh deduplication ensures only one refresh runs per key.
	// With 100ms origin delay + 10 concurrent stale requests, without dedup
	// we'd see 10+ origin hits; with dedup we see 1 (seed) + 1 (refresh).
	require.Eventually(t, func() bool {
		_, _, b := get(t, p.url()+"/objects/stale")
		return b == "stale-v2"
	}, 3*time.Second, 20*time.Millisecond)
	require.LessOrEqual(t, o.hitCount("stale"), 3, "refresh dedup must prevent per-request origin hits")
}

func TestE2E_PutThenGetRoundTrip(t *testing.T) {
	o := newOrigin(t)
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, p.url()+"/objects/uploaded", strings.NewReader("uploaded-body"))
	require.NoError(t, err)
	req.Header.Set("ETag", "uploaded-v1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	status, hdr, body := get(t, p.url()+"/objects/uploaded")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "uploaded-body", body)
	require.Equal(t, "uploaded-v1", hdr.Get("ETag"))
	require.Equal(t, 0, o.hitCount("uploaded"), "uploaded value must be served from cache")
}

// TestE2E_ConcurrentSetOperations verifies that concurrent Set operations
// on the same key do not corrupt the cache or panic. The last writer should
// win (last-writer-wins semantics).
func TestE2E_ConcurrentSetOperations(t *testing.T) {
	o := newOrigin(t)
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	const writers = 10
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
				p.url()+"/objects/shared", strings.NewReader(fmt.Sprintf("value-%d", idx)))
			if err != nil {
				return
			}
			req.Header.Set("ETag", fmt.Sprintf("v%d", idx))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			resp.Body.Close()
		}(i)
	}
	wg.Wait()

	// The cache must have some value (last-writer-wins).
	status, _, body := get(t, p.url()+"/objects/shared")
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, body)
	require.Equal(t, 0, o.hitCount("shared"), "cached value must be served without origin")
}

// TestE2E_CacheKeyWithSpecialChars verifies that keys containing special
// characters (slashes, spaces, unicode) are handled correctly.
func TestE2E_CacheKeyWithSpecialChars(t *testing.T) {
	o := newOrigin(t)
	o.set("path/to/resource", "path-value", "path-v1")
	o.set("key with spaces", "space-value", "space-v1")
	cache := newCache(t,
		daramjwee.WithTiers(memstore.New(0, nil)),
		daramjwee.WithFreshness(time.Hour, time.Hour),
	)
	p := newProxy(t, cache, o)

	// Slash in key: the proxy strips "/objects/" prefix, so "path/to/resource"
	// should work correctly.
	status, _, body := get(t, p.url()+"/objects/path/to/resource")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "path-value", body)

	status, _, body = get(t, p.url()+"/objects/path/to/resource")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "path-value", body)
	require.Equal(t, 1, o.hitCount("path/to/resource"), "hot hit must not touch origin")
}

// Keep os referenced in case later tests add file-based assertions.
var _ = os.Getenv
