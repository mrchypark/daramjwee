// Package cache contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
	"golang.org/x/sync/errgroup"
	// "golang.org/x/sync/singleflight" // TODO: 추후 singleflight.Group 추가
)

// DaramjweeCache는 Cache 인터페이스의 구체적인 구현체입니다.
type DaramjweeCache struct {
	HotStore       Store
	ColdStore      Store // Optional
	Logger         log.Logger
	Worker         *worker.Manager
	DefaultTimeout time.Duration
	// flightGroup      singleflight.Group
}

// 컴파일 타임에 DaramjweeCache가 Cache 인터페이스를 만족하는지 확인합니다.
var _ Cache = (*DaramjweeCache)(nil)

// Get은 Hot Tier(로컬)에서 데이터를 찾고, 없으면 Fetcher를 통해 원본에서 가져옵니다.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	if c.HotStore != nil {
		stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key) // meta (etag) is not used here directly
		if err == nil {
			level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)
			return stream, nil
		}
		if err != ErrNotFound {
			level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
		}
	}

	level.Debug(c.Logger).Log("msg", "cache miss, fetching from origin", "key", key)

	var oldETag string
	if c.HotStore != nil {
		if etag, err := c.statFromStore(ctx, c.HotStore, key); err == nil {
			oldETag = etag
		}
	}

	result, err := fetcher.Fetch(ctx, oldETag)
	if err != nil {
		if err == ErrNotModified {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from cache", "key", key)
			// Attempt to serve from hot cache again, assuming it might still be there.
			// This happens if the origin says 304 Not Modified.
			stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err == nil {
				return stream, nil
			}
			level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
			return nil, ErrNotFound // Or a more specific error if not found after 304
		}
		return nil, err
	}

	return c.cacheAndTeeStream(ctx, key, result)
}

// Set은 Hot Store에 스트림으로 데이터를 직접 쓸 수 있는 WriteCloser를 반환합니다.
func (c *DaramjweeCache) Set(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	if c.HotStore == nil {
		return nil, &ConfigError{"hotStore is not configured"}
	}
	streamingStore, ok := c.HotStore.(Store)
	if !ok {
		return nil, &ConfigError{"hotStore does not support streaming writes"}
	}

	ctx, cancel := c.newCtxWithTimeout(ctx)

	var wc io.WriteCloser
	var err error

	if caStore, ok := streamingStore.(ContextAwareStore); ok {
		wc, err = caStore.SetWithWriterContext(ctx, key, etag)
	} else {
		wc, err = streamingStore.SetWithWriter(key, etag)
	}

	if err != nil {
		cancel()
		return nil, err
	}

	return newCancelWriteCloser(wc, cancel), nil
}

// Delete는 모든 캐시 티어에서 객체를 동시에 삭제합니다.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)

	if c.HotStore != nil {
		g.Go(func() error {
			err := c.deleteFromStore(gCtx, c.HotStore, key)
			if err != nil {
				level.Error(c.Logger).Log("msg", "failed to delete from hot store", "key", key, "err", err)
				return err
			}
			return nil
		})
	}

	if c.ColdStore != nil {
		g.Go(func() error {
			err := c.deleteFromStore(gCtx, c.ColdStore, key)
			if err != nil {
				level.Error(c.Logger).Log("msg", "failed to delete from cold store", "key", key, "err", err)
				return err
			}
			return nil
		})
	}

	return g.Wait()
}

// ScheduleRefresh는 백그라운드 캐시 갱신 작업을 워커에게 제출합니다.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	if c.Worker == nil {
		return &ConfigError{"worker is not configured"}
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		var oldETag string
		if c.HotStore != nil {
			if etag, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil {
				oldETag = etag
			}
		}

		result, err := fetcher.Fetch(jobCtx, oldETag)
		if err != nil {
			if err != ErrNotModified {
				level.Error(c.Logger).Log("msg", "background fetch failed", "key", key, "err", err)
			}
			return
		}
		defer result.Body.Close()

		if c.HotStore != nil {
			if streamingStore, ok := c.HotStore.(Store); ok {
				writer, err := streamingStore.SetWithWriter(key, result.ETag)
				if err != nil {
					level.Error(c.Logger).Log("msg", "failed to get cache writer for refresh", "key", key, "err", err)
					return
				}
				_, copyErr := io.Copy(writer, result.Body)
				closeErr := writer.Close()
				if copyErr != nil || closeErr != nil {
					level.Error(c.Logger).Log("msg", "failed to write to cache during refresh", "key", key, "copyErr", copyErr, "closeErr", closeErr)
				} else {
					level.Info(c.Logger).Log("msg", "background refresh successful", "key", key)
				}
			}
		}
	}

	c.Worker.Submit(job)
	return nil
}

// Close는 워커를 안전하게 종료시킵니다.
func (c *DaramjweeCache) Close() {
	if c.Worker != nil {
		level.Info(c.Logger).Log("msg", "shutting down daramjwee cache")
		c.Worker.Shutdown()
	}
}

// --- 내부 헬퍼 메서드 및 타입 ---

func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, string, error) {
	if caStore, ok := store.(ContextAwareStore); ok {
		return caStore.GetStreamContext(ctx, key)
	}
	return store.GetStream(key)
}

func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	if caStore, ok := store.(ContextAwareStore); ok {
		return caStore.DeleteContext(ctx, key)
	}
	return store.Delete(key)
}

func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (string, error) {
	if caStore, ok := store.(ContextAwareStore); ok {
		return caStore.StatContext(ctx, key)
	}
	return store.Stat(key)
}

func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if c.HotStore != nil {
		if streamingStore, ok := c.HotStore.(Store); ok {
			var cacheWriter io.WriteCloser
			var err error
			if caStore, ok := streamingStore.(ContextAwareStore); ok {
				cacheWriter, err = caStore.SetWithWriterContext(ctx, key, result.ETag)
			} else {
				cacheWriter, err = streamingStore.SetWithWriter(key, result.ETag)
			}
			if err != nil {
				level.Error(c.Logger).Log("msg", "failed to get cache writer", "key", key, "err", err)
				return result.Body, nil
			}
			teeReader := io.TeeReader(result.Body, cacheWriter)
			return newMultiCloser(teeReader, result.Body, cacheWriter), nil
		}
	}
	return result.Body, nil
}

// multiCloser는 여러 io.Closer를 순서대로 닫아주는 헬퍼입니다.
type multiCloser struct {
	reader  io.Reader
	closers []io.Closer
}

func newMultiCloser(r io.Reader, closers ...io.Closer) io.ReadCloser {
	return &multiCloser{reader: r, closers: closers}
}
func (mc *multiCloser) Read(p []byte) (n int, err error) { return mc.reader.Read(p) }
func (mc *multiCloser) Close() error {
	var firstErr error
	for _, c := range mc.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// cancelWriteCloser는 쓰기 작업이 완료(Close)될 때 context를 cancel 해주는 헬퍼입니다.
type cancelWriteCloser struct {
	io.WriteCloser
	cancel context.CancelFunc
}

func newCancelWriteCloser(wc io.WriteCloser, cancel context.CancelFunc) io.WriteCloser {
	return &cancelWriteCloser{WriteCloser: wc, cancel: cancel}
}
func (cwc *cancelWriteCloser) Close() error {
	defer cwc.cancel()
	return cwc.WriteCloser.Close()
}
