// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/samber/go-singleflightx" // 라이브러리 import
	"golang.org/x/sync/errgroup"
)

// DaramjweeCache는 Cache 인터페이스의 구체적인 구현체입니다.
type DaramjweeCache struct {
	HotStore       Store
	ColdStore      Store // Optional
	Logger         log.Logger
	Worker         *worker.Manager
	DefaultTimeout time.Duration

	sfg singleflightx.Group[string, *inFlightFetch]
}

// 컴파일 타임에 DaramjweeCache가 Cache 인터페이스를 만족하는지 확인합니다.
var _ Cache = (*DaramjweeCache)(nil)

// Get은 요청된 캐싱 전략에 따라 데이터를 가져옵니다.
// 캐시 확인 및 Origin fetch 로직을 각 헬퍼 메서드로 분리하여 가독성을 높였습니다.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Hot 캐시 확인
	hotStream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		return c.handleHotHit(ctx, key, fetcher, hotStream)
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Cold 캐시 확인
	coldStream, coldMeta, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		return c.handleColdHit(ctx, key, coldStream, coldMeta)
	}
	if err != ErrNotFound {
		level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Origin에서 Fetch
	return c.handleMiss(ctx, key, fetcher)
}

// handleHotHit는 Hot 캐시에서 객체를 찾았을 때의 로직을 처리합니다.
func (c *DaramjweeCache) handleHotHit(ctx context.Context, key string, fetcher Fetcher, hotStream io.ReadCloser) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)
	// 응답은 즉시 반환하고, 백그라운드에서 캐시 갱신을 시도합니다.
	if err := c.ScheduleRefresh(context.Background(), key, fetcher); err != nil {
		level.Warn(c.Logger).Log("msg", "failed to schedule refresh on hot hit", "key", key, "err", err)
	}
	return hotStream, nil
}

// handleColdHit는 Cold 캐시에서 객체를 찾았을 때의 로직을 처리합니다.
func (c *DaramjweeCache) handleColdHit(ctx context.Context, key string, coldStream io.ReadCloser, coldMeta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)
	// Cold 캐시의 데이터를 클라이언트로 스트리밍하면서 동시에 Hot 캐시로 승격시킵니다.
	return c.promoteAndTeeStream(ctx, key, coldMeta.ETag, coldStream)
}

type inFlightFetch struct {
	waiters     []*io.PipeWriter
	mu          sync.Mutex
	fetchResult *FetchResult
	fetchErr    error
	ready       chan struct{}
}

func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "full cache miss, looking up via singleflight", "key", key)

	myPipeReader, myPipeWriter := io.Pipe()

	// 1. 제네릭 Do 메소드 호출. 반환값이 바로 *inFlightFetch 타입입니다.
	//    더 이상 `interface{}`와 타입 단언을 사용할 필요가 없습니다.
	flight, err, _ := c.sfg.Do(key, func() (*inFlightFetch, error) {
		// --- 이 부분은 리더만 실행합니다 (이전 답변과 로직 동일) ---
		level.Debug(c.Logger).Log("msg", "singleflight leader: fetching from origin", "key", key)

		shared := &inFlightFetch{
			waiters: []*io.PipeWriter{},
			ready:   make(chan struct{}),
		}

		var oldETag string
		if meta, err := c.statFromStore(ctx, c.HotStore, key); err == nil && meta != nil {
			oldETag = meta.ETag
		}

		result, fetchErr := fetcher.Fetch(ctx, oldETag)
		shared.fetchResult = result
		shared.fetchErr = fetchErr

		if fetchErr == nil {
			go c.fanOutStream(key, shared) // fanOutStream 헬퍼 함수는 이전 답변과 동일
		}

		close(shared.ready)
		return shared, nil
	})

	// Do 함수 자체의 에러 처리 (예: 패닉 복구)
	if err != nil {
		_ = myPipeWriter.CloseWithError(err)
		return nil, fmt.Errorf("singleflight execution failed: %w", err)
	}

	// 2. 모든 고루틴 (리더, 대기자)이 여기서부터 실행됩니다.
	<-flight.ready

	// 3. 리더의 Fetch 결과를 확인하고 에러를 처리합니다.
	if flight.fetchErr != nil {
		_ = myPipeWriter.CloseWithError(flight.fetchErr)
		if flight.fetchErr == ErrNotModified {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			// ErrNotModified 일 경우, 스트림과 메타데이터를 다시 가져옵니다.
			stream, _, err := c.getStreamFromStore(ctx, c.HotStore, key)
			return stream, err // getStreamFromStore가 반환하는 에러를 그대로 반환
		}
		return nil, flight.fetchErr
	}

	// 4. 자신의 Pipe Writer를 공유 waiter 목록에 추가합니다.
	flight.mu.Lock()
	flight.waiters = append(flight.waiters, myPipeWriter)
	flight.mu.Unlock()

	// 5. 자신만의 Pipe Reader를 반환하여 스트리밍을 시작합니다.
	return myPipeReader, nil
}

// fanOutStream is run by the leader goroutine to distribute the origin stream
// to all waiters and cache it.
func (c *DaramjweeCache) fanOutStream(key string, flight *inFlightFetch) {
	defer flight.fetchResult.Body.Close()

	// 캐시 저장을 위한 Writer 준비
	// 이 writer는 io.MultiWriter에 포함되어 데이터 스트림을 동시에 받습니다.
	cacheWriter, err := c.setStreamToStore(context.Background(), c.HotStore, key, flight.fetchResult.Metadata.ETag)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get cache writer for singleflight", "key", key, "err", err)
		// 캐싱에 실패하더라도 클라이언트에게 스트리밍은 계속되어야 합니다.
	}

	// MultiWriter를 생성합니다.
	// 초반에는 캐시 writer만 있을 수 있습니다. 대기자들은 동적으로 추가됩니다.
	allWriters := []io.Writer{}
	if cacheWriter != nil {
		allWriters = append(allWriters, cacheWriter)
	}

	// TeeReader를 사용하여 원본 스트림을 읽으면서 동시에 MultiWriter에 씁니다.
	// MultiWriter는 연결된 모든 클라이언트와 캐시에 데이터를 분배합니다.
	reader := flight.fetchResult.Body
	multiWriter := io.MultiWriter(allWriters...)
	teeReader := io.TeeReader(reader, multiWriter)

	// 데이터를 모두 소진하여 모든 waiter들과 캐시에 데이터를 전송합니다.
	// io.Copy가 끝나면 원본 스트림은 EOF에 도달합니다.
	_, copyErr := io.Copy(io.Discard, teeReader)

	// 모든 작업이 끝난 후, 리소스를 정리합니다.
	if cacheWriter != nil {
		if err := cacheWriter.Close(); err != nil {
			level.Warn(c.Logger).Log("msg", "failed to close cache writer in singleflight", "key", key, "err", err)
		}
	}

	// 모든 waiter들의 파이프를 닫아주어 스트림의 끝(EOF)을 알립니다.
	flight.mu.Lock()
	defer flight.mu.Unlock()
	for _, writer := range flight.waiters {
		// writer는 이제 *io.PipeWriter 타입이므로 타입 단언이 필요 없습니다.
		if copyErr != nil {
			_ = writer.CloseWithError(copyErr)
		} else {
			_ = writer.Close()
		}
	}

	// 백그라운드에서 Cold 캐시 저장 작업을 예약할 수 있습니다.
	if copyErr == nil {
		c.scheduleSetToStore(context.Background(), c.ColdStore, key)
	}
}

// Set returns a WriteCloser for streaming data directly into the Hot Store.
// The cache entry is finalized only when the returned writer is closed.
// IMPORTANT: The caller MUST call Close() on the returned io.WriteCloser to finalize
// the operation and release associated resources, including the context timeout.
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
		if meta, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil && meta != nil {
			oldETag = meta.ETag
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
		defer func() {
			if err := result.Body.Close(); err != nil {
				level.Warn(c.Logger).Log("msg", "failed to close result body in ScheduleRefresh", "key", key, "err", err)
			}
		}()

		writer, err := c.setStreamToStore(jobCtx, c.HotStore, key, result.Metadata.ETag)
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

func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, *Metadata, error) {
	return store.GetStream(ctx, key)
}

func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, etag string) (io.WriteCloser, error) {
	return store.SetWithWriter(ctx, key, etag)
}

func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	return store.Stat(ctx, key)
}

func (c *DaramjweeCache) scheduleSetToStore(ctx context.Context, destStore Store, key string) {
	if c.Worker == nil {
		level.Warn(c.Logger).Log("msg", "worker is not configured, cannot schedule set", "key", key)
		return
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background set", "key", key, "dest", "cold")

		srcStream, meta, err := c.getStreamFromStore(jobCtx, c.HotStore, key)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get stream from hot store for background set", "key", key, "err", err)
			return
		}
		defer func() {
			if err := srcStream.Close(); err != nil {
				level.Warn(c.Logger).Log("msg", "failed to close srcStream in scheduleSetToStore", "key", key, "err", err)
			}
		}()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, meta.ETag)
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
	if c.HotStore != nil && result.Metadata != nil {
		cacheWriter, err := c.setStreamToStore(ctx, c.HotStore, key, result.Metadata.ETag)
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
