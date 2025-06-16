// Package cache contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
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

// Get은 요청된 캐싱 전략에 따라 데이터를 가져옵니다.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Hot 캐시 확인
	hotStream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)
		c.ScheduleRefresh(context.Background(), key, fetcher)
		return hotStream, nil
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Cold 캐시 확인
	coldStream, coldEtag, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)
		return c.promoteAndTeeStream(ctx, key, coldEtag, coldStream)
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Origin에서 Fetch
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

	var oldETag string
	if etag, err := c.statFromStore(ctx, c.HotStore, key); err == nil {
		oldETag = etag
	}

	result, err := fetcher.Fetch(ctx, oldETag)
	if err != nil {
		if err == ErrNotModified {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			return stream, nil
		}
		return nil, err
	}

	hotTeeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		return result.Body, nil
	}

	// cold에 set 을 worker에 요청
	c.scheduleSetToStore(context.Background(), c.ColdStore, key)

	return hotTeeStream, nil
}

// Set은 Hot Store에 스트림으로 데이터를 직접 쓸 수 있는 WriteCloser를 반환합니다.
func (c *DaramjweeCache) Set(ctx context.Context, key string, etag string) (io.WriteCloser, error) {
	if c.HotStore == nil {
		return nil, &ConfigError{"hotStore is not configured"}
	}

	ctx, cancel := c.newCtxWithTimeout(ctx)

	wc, err := c.setStreamToStore(ctx, c.HotStore, key, etag)
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

	// HotStore 삭제
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

	// ColdStore 삭제
	g.Go(func() error {
		err := c.deleteFromStore(gCtx, c.ColdStore, key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to delete from cold store", "key", key, "err", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

// ScheduleRefresh는 백그라운드 캐시 갱신 작업을 워커에게 제출합니다.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule refresh", "key", key)
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		var oldETag string
		if etag, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil {
			oldETag = etag
		}

		result, err := fetcher.Fetch(jobCtx, oldETag)
		if err != nil {
			if err == ErrNotModified {
				level.Debug(c.Logger).Log("msg", "background refresh: object not modified", "key", key)
			} else {
				level.Error(c.Logger).Log("msg", "background fetch failed", "key", key, "err", err)
			}
			return
		}
		defer result.Body.Close()

		writer, err := c.setStreamToStore(jobCtx, c.HotStore, key, result.ETag)
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
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

// 타입 단언이 필요 없어지고 코드가 매우 간결해졌습니다.
func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, string, error) {
	return store.GetStream(ctx, key)
}

func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, etag string) (io.WriteCloser, error) {
	return store.SetWithWriter(ctx, key, etag)
}

func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (string, error) {
	return store.Stat(ctx, key)
}

func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set", "key", key, "dest", "cold")

		srcStream, etag, err := c.getStreamFromStore(jobCtx, c.HotStore, key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
			return
		}
		defer srcStream.Close()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, etag)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get writer for dest store for background set", "key", key, "err", err)
			return
		}

		_, copyErr := io.Copy(destWriter, srcStream)
		closeErr := destWriter.Close()

		if copyErr != nil || closeErr != nil {
			level.Error(c.Logger).Log("msg", "failed background set", "key", key, "copyErr", copyErr, "closeErr", closeErr)
		} else {
			level.Info(c.Logger).Log("msg", "background set successful", "key", key, "dest", "cold")
		}
	}

	c.Worker.Submit(job)
}

func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key, etag string, coldStream io.ReadCloser) (io.ReadCloser, error) {
	hotWriter, err := c.setStreamToStore(ctx, c.HotStore, key, etag)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get hot store writer for promotion", "key", key, "err", err)
		return coldStream, nil
	}

	teeReader := io.TeeReader(coldStream, hotWriter)
	return newMultiCloser(teeReader, coldStream, hotWriter), nil
}

func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if c.HotStore != nil {
		cacheWriter, err := c.setStreamToStore(ctx, c.HotStore, key, result.ETag)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer", "key", key, "err", err)
			return result.Body, nil
		}
		teeReader := io.TeeReader(result.Body, cacheWriter)
		return newMultiCloser(teeReader, result.Body, cacheWriter), nil
	}
	return result.Body, nil
}

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
