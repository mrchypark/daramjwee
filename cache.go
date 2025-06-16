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
// CHANGED: 요청하신 새로운 Get 로직 전체를 여기에 구현했습니다.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Hot 캐시 확인
	hotStream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)
		// 있으면 응답 > worker에게 refresh 요청
		// 응답은 즉시 반환하고, 백그라운드에서 캐시 갱신을 시도합니다.
		// context.Background()를 사용하여 부모 요청이 취소되어도 갱신은 계속되도록 합니다.
		c.ScheduleRefresh(context.Background(), key, fetcher)
		return hotStream, nil
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
		// Hot 스토어 자체의 오류일 수 있으므로 바로 Fetcher로 넘어가지 않고 오류를 반환할 수 있습니다.
		// 여기서는 일단 로깅만 하고 다음 단계로 진행합니다.
	}

	// 2. Cold 캐시 확인 (Cold 스토어가 설정된 경우)
	if c.ColdStore != nil {
		coldStream, coldEtag, err := c.getStreamFromStore(ctx, c.ColdStore, key)
		if err == nil {
			level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)
			// cold가 있으면 hot에 set 하면서 응답
			// Cold -> Hot으로 데이터를 스트리밍하면서 동시에 클라이언트에게 응답합니다.
			return c.promoteAndTeeStream(ctx, key, coldEtag, coldStream)
		}
		if err != ErrNotFound {
			level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
		}
	}

	// 3. Origin에서 Fetch (Hot/Cold 모두 miss)
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

	// ETag가 있으면 활용하여 304 Not Modified 응답을 받을 수 있습니다.
	var oldETag string
	if etag, err := c.statFromStore(ctx, c.HotStore, key); err == nil {
		oldETag = etag
	}

	result, err := fetcher.Fetch(ctx, oldETag)
	if err != nil {
		if err == ErrNotModified {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			// 304 응답을 받으면 데이터가 변경되지 않았다는 의미이므로, 다시 Hot 캐시에서 가져옵니다.
			// (그 사이에 다른 요청에 의해 캐시에 저장되었을 수 있습니다)
			stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			return stream, nil
		}
		return nil, err
	}

	// fetch 하고 hot에 set하면서 응답
	hotTeeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		// Tee 스트림 생성에 실패하면 원본 스트림만 반환합니다.
		return result.Body, nil
	}

	// cold에 set 을 worker에 요청 (Cold 스토어가 설정된 경우)
	if c.ColdStore != nil {
		// 새로 fetch된 데이터가 Hot 캐시에 저장되면,
		// 백그라운드에서 Hot -> Cold로 복사하는 작업을 예약합니다.
		c.scheduleSetToStore(context.Background(), c.ColdStore, key)
	}

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
// 요청하신 'refresh' 로직 (check -> fetch)은 이 함수가 이미 수행하고 있습니다.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule refresh", "key", key)
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		// 1. Check: 현재 캐시의 ETag를 가져와 fetcher에게 전달
		var oldETag string
		if etag, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil {
			oldETag = etag
		}

		// 2. Fetch: fetcher는 ETag를 보고 원본이 변경되었는지 확인 (ErrNotModified 반환 가능)
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

		// 3. Hot 스토어에 새로운 내용 저장
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
		return ctx, func() {} // 이미 타임아웃이 설정된 경우 그대로 사용
	}
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, string, error) {
	if caStore, ok := store.(ContextAwareStore); ok {
		return caStore.GetStreamContext(ctx, key)
	}
	return store.GetStream(key)
}

// ADDED: SetWithWriter를 컨텍스트와 함께 호출하는 헬퍼
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, etag string) (io.WriteCloser, error) {
	if caStore, ok := store.(ContextAwareStore); ok {
		return caStore.SetWithWriterContext(ctx, key, etag)
	}
	return store.SetWithWriter(key, etag)
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

// ADDED: 백그라운드에서 특정 저장소로 데이터를 복사하는 작업을 스케줄링합니다.
func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set", "key", key, "dest", "cold")

		// Hot 스토어에서 데이터를 읽어옵니다.
		srcStream, etag, err := c.getStreamFromStore(jobCtx, c.HotStore, key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
			return
		}
		defer srcStream.Close()

		// 대상 스토어(Cold)에 데이터를 씁니다.
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

// ADDED: Cold -> Hot으로 승격시키면서 클라이언트에게 스트리밍하는 헬퍼
func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key, etag string, coldStream io.ReadCloser) (io.ReadCloser, error) {
	// Hot 스토어에 쓸 writer를 가져옵니다.
	hotWriter, err := c.setStreamToStore(ctx, c.HotStore, key, etag)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get hot store writer for promotion", "key", key, "err", err)
		// 승격에 실패하더라도 Cold 스트림은 그대로 반환하여 응답은 성공시킵니다.
		return coldStream, nil
	}

	// Cold 스트림을 읽어서 클라이언트와 Hot 캐시에 동시에 보냅니다.
	teeReader := io.TeeReader(coldStream, hotWriter)

	// coldStream과 hotWriter를 모두 닫아주는 multiCloser를 반환합니다.
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
