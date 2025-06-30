// Package daramjwee contains the core implementation of the Cache interface.
package daramjwee

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
)

var ErrCacheClosed = errors.New("daramjwee: cache is closed")

// DaramjweeCache는 Cache 인터페이스의 구체적인 구현체입니다.
type DaramjweeCache struct {
	HotStore         Store
	ColdStore        Store // Optional
	Logger           log.Logger
	Worker           *worker.Manager
	DefaultTimeout   time.Duration
	ShutdownTimeout  time.Duration
	PositiveFreshFor time.Duration
	NegativeFreshFor time.Duration
	isClosed         atomic.Bool
}

// 컴파일 타임에 DaramjweeCache가 Cache 인터페이스를 만족하는지 확인합니다.
var _ Cache = (*DaramjweeCache)(nil)

// Get은 요청된 캐싱 전략에 따라 데이터를 가져옵니다.
func (c *DaramjweeCache) Get(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// 1. Hot 캐시 확인
	hotStream, hotMeta, err := c.getStreamFromStore(ctx, c.HotStore, key)
	if err == nil {
		return c.handleHotHit(ctx, key, fetcher, hotStream, hotMeta)
	}
	if !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "hot store get failed", "key", key, "err", err)
	}

	// 2. Cold 캐시 확인
	coldStream, coldMeta, err := c.getStreamFromStore(ctx, c.ColdStore, key)
	if err == nil {
		return c.handleColdHit(ctx, key, coldStream, coldMeta)
	}
	if !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "cold store get failed", "key", key, "err", err)
	}

	// 3. Origin에서 Fetch
	return c.handleMiss(ctx, key, fetcher)
}

// Set은 데이터를 캐시에 직접 쓰는 스트림을 반환합니다.
func (c *DaramjweeCache) Set(ctx context.Context, key string, metadata *Metadata) (io.WriteCloser, error) {
	if c.isClosed.Load() {
		return nil, ErrCacheClosed
	}
	// ... (이하 동일)
	if c.HotStore == nil {
		return nil, &ConfigError{"hotStore is not configured"}
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)

	if metadata == nil {
		metadata = &Metadata{}
	}
	metadata.CachedAt = time.Now()

	wc, err := c.setStreamToStore(ctx, c.HotStore, key, metadata)
	if err != nil {
		cancel()
		return nil, err
	}
	return newCancelWriteCloser(wc, cancel), nil
}

// Delete는 모든 캐시 티어에서 객체를 순차적으로 삭제하여 교착 상태를 방지합니다.
func (c *DaramjweeCache) Delete(ctx context.Context, key string) error {
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	ctx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()

	// --- errgroup을 사용한 병렬 삭제를 제거 ---
	var firstErr error

	// 1. 항상 Hot Store를 먼저 삭제합니다.
	if c.HotStore != nil {
		if err := c.deleteFromStore(ctx, c.HotStore, key); err != nil && !errors.Is(err, ErrNotFound) {
			level.Error(c.Logger).Log("msg", "failed to delete from hot store", "key", key, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// 2. 그 다음 Cold Store를 삭제합니다.
	if err := c.deleteFromStore(ctx, c.ColdStore, key); err != nil && !errors.Is(err, ErrNotFound) {
		level.Error(c.Logger).Log("msg", "failed to delete from cold store", "key", key, "err", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// ScheduleRefresh는 백그라운드 캐시 갱신 작업을 워커에게 제출합니다.
func (c *DaramjweeCache) ScheduleRefresh(ctx context.Context, key string, fetcher Fetcher) error {
	// --- 여기에도 가드 코드 추가 ---
	if c.isClosed.Load() {
		return ErrCacheClosed
	}
	// -------------------------

	if c.Worker == nil {
		return errors.New("worker is not configured, cannot schedule refresh")
	}

	job := func(jobCtx context.Context) {
		level.Info(c.Logger).Log("msg", "starting background refresh", "key", key)

		var oldMetadata *Metadata
		if meta, err := c.statFromStore(jobCtx, c.HotStore, key); err == nil && meta != nil {
			oldMetadata = meta
		}

		result, err := fetcher.Fetch(jobCtx, oldMetadata)
		if err != nil {
			if errors.Is(err, ErrCacheableNotFound) {
				level.Debug(c.Logger).Log("msg", "re-caching as negative entry during background refresh", "key", key)
				c.handleNegativeCache(jobCtx, key)
			} else if errors.Is(err, ErrNotModified) {
				level.Debug(c.Logger).Log("msg", "background refresh: object not modified", "key", key)
			} else {
				level.Error(c.Logger).Log("msg", "background fetch failed", "key", key, "err", err)
			}
			return
		}
		defer result.Body.Close()

		if result.Metadata == nil {
			result.Metadata = &Metadata{}
		}
		result.Metadata.CachedAt = time.Now()

		writer, err := c.setStreamToStore(jobCtx, c.HotStore, key, result.Metadata)
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
	// --- 수정된 Close 메서드 ---
	if c.isClosed.Swap(true) {
		// 이미 닫혔으면 아무것도 하지 않음 (중복 호출 방지)
		return
	}
	// -------------------------

	if c.Worker != nil {
		level.Info(c.Logger).Log("msg", "shutting down daramjwee cache")
		if err := c.Worker.Shutdown(c.ShutdownTimeout); err != nil {
			level.Error(c.Logger).Log("msg", "graceful shutdown failed", "err", err)
		} else {
			level.Info(c.Logger).Log("msg", "daramjwee cache shutdown complete")
		}
	}
}

// --- 이하 내부 헬퍼 메서드들은 변경 없음 ---
// handleHotHit, handleColdHit, handleMiss, ...

// handleHotHit는 Hot 캐시에서 객체를 찾았을 때의 로직을 처리합니다.
func (c *DaramjweeCache) handleHotHit(ctx context.Context, key string, fetcher Fetcher, hotStream io.ReadCloser, meta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "hot cache hit", "key", key)

	var isStale bool
	// 메타데이터와 신선도 유지 기간(FreshFor)을 사용한 만료 시간 계산
	if meta.IsNegative {
		freshnessLifetime := c.NegativeFreshFor
		if freshnessLifetime == 0 || (freshnessLifetime > 0 && time.Now().After(meta.CachedAt.Add(freshnessLifetime))) {
			isStale = true
		}
	} else {
		freshnessLifetime := c.PositiveFreshFor
		if freshnessLifetime == 0 || (freshnessLifetime > 0 && time.Now().After(meta.CachedAt.Add(freshnessLifetime))) {
			isStale = true
		}
	}

	hotStreamCloser := newCloserWithCallback(hotStream, func() {})

	if isStale {
		level.Debug(c.Logger).Log("msg", "hot cache is stale, scheduling refresh", "key", key)
		hotStreamCloser = newCloserWithCallback(hotStream, func() { c.ScheduleRefresh(context.Background(), key, fetcher) })
	}

	if meta.IsNegative {
		hotStreamCloser.Close()
		return nil, ErrNotFound
	}

	return hotStreamCloser, nil
}

// handleColdHit는 Cold 캐시에서 객체를 찾았을 때의 로직을 처리합니다.
func (c *DaramjweeCache) handleColdHit(ctx context.Context, key string, coldStream io.ReadCloser, coldMeta *Metadata) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "cold cache hit, promoting to hot", "key", key)

	// ✅ [핵심 수정] Hot 캐시로 승격할 메타데이터의 복사본을 만듭니다.
	// 이렇게 하면 여러 고루틴이 동시에 Cold Hit을 처리하더라도
	// 원본 coldMeta 객체를 직접 수정하지 않아 데이터 경쟁을 방지할 수 있습니다.
	metaToPromote := &Metadata{}
	if coldMeta != nil {
		// 기존 메타데이터의 값을 복사합니다.
		*metaToPromote = *coldMeta
	}
	// 복사본의 CachedAt 필드만 현재 시간으로 갱신합니다.
	metaToPromote.CachedAt = time.Now()

	// 수정된 복사본을 승격 로직으로 전달합니다.
	return c.promoteAndTeeStream(ctx, key, metaToPromote, coldStream)
}

// handleMiss는 Hot/Cold 캐시에서 모두 객체를 찾지 못했을 때의 로직을 처리합니다.
func (c *DaramjweeCache) handleMiss(ctx context.Context, key string, fetcher Fetcher) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "full cache miss, fetching from origin", "key", key)

	var oldMetadata *Metadata
	if meta, err := c.statFromStore(ctx, c.HotStore, key); err == nil {
		oldMetadata = meta
	}

	result, err := fetcher.Fetch(ctx, oldMetadata)
	if err != nil {
		if errors.Is(err, ErrCacheableNotFound) {
			return c.handleNegativeCache(ctx, key)
		}
		if errors.Is(err, ErrNotModified) {
			level.Debug(c.Logger).Log("msg", "object not modified, serving from hot cache again", "key", key)
			stream, meta, err := c.getStreamFromStore(ctx, c.HotStore, key)
			if err != nil {
				level.Warn(c.Logger).Log("msg", "failed to refetch from hot cache after 304", "key", key, "err", err)
				return nil, ErrNotFound
			}
			if meta.IsNegative {
				stream.Close()
				return nil, ErrNotFound
			}
			return stream, nil
		}
		return nil, err
	}

	if result.Metadata == nil {
		result.Metadata = &Metadata{}
	}
	result.Metadata.CachedAt = time.Now()

	hotTeeStream, err := c.cacheAndTeeStream(ctx, key, result)
	if err != nil {
		return result.Body, nil
	}

	c.scheduleSetToStore(context.Background(), c.ColdStore, key)
	return hotTeeStream, nil
}

// handleNegativeCache는 네거티브 캐시 항목을 저장하는 로직을 처리합니다.
func (c *DaramjweeCache) handleNegativeCache(ctx context.Context, key string) (io.ReadCloser, error) {
	level.Debug(c.Logger).Log("msg", "caching as negative entry", "key", key, "NegativeFreshFor", c.NegativeFreshFor)

	meta := &Metadata{
		IsNegative: true,
		CachedAt:   time.Now(),
	}

	writer, err := c.setStreamToStore(ctx, c.HotStore, key, meta)
	if err != nil {
		level.Warn(c.Logger).Log("msg", "failed to get writer for negative cache entry", "key", key, "err", err)
	} else {
		if closeErr := writer.Close(); closeErr != nil {
			level.Warn(c.Logger).Log("msg", "failed to close writer for negative cache entry", "key", key, "err", closeErr)
		}
	}
	return nil, ErrNotFound
}

// newCtxWithTimeout는 컨텍스트에 데드라인이 없으면 기본 타임아웃을 적용합니다.
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.DefaultTimeout)
}

// getStreamFromStore는 Store 인터페이스를 호출하는 래퍼(wrapper)입니다.
func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, *Metadata, error) {
	return store.GetStream(ctx, key)
}

// setStreamToStore는 Store 인터페이스를 호출하는 래퍼입니다.
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, metadata *Metadata) (io.WriteCloser, error) {
	return store.SetWithWriter(ctx, key, metadata)
}

// deleteFromStore는 Store 인터페이스를 호출하는 래퍼입니다.
func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

// statFromStore는 Store 인터페이스를 호출하는 래퍼입니다.
func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	return store.Stat(ctx, key)
}

// scheduleSetToStore는 Hot 캐시의 내용을 Cold 캐시로 비동기 복사하는 작업을 예약합니다.
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
		defer srcStream.Close()

		destWriter, err := c.setStreamToStore(jobCtx, destStore, key, meta)
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

// promoteAndTeeStream은 Cold 스트림을 Hot으로 승격시키면서 동시에 사용자에게 반환합니다.
func (c *DaramjweeCache) promoteAndTeeStream(ctx context.Context, key string, metadata *Metadata, coldStream io.ReadCloser) (io.ReadCloser, error) {
	hotWriter, err := c.setStreamToStore(ctx, c.HotStore, key, metadata)
	if err != nil {
		level.Error(c.Logger).Log("msg", "failed to get hot store writer for promotion", "key", key, "err", err)
		return coldStream, nil
	}

	teeReader := io.TeeReader(coldStream, hotWriter)
	return newMultiCloser(teeReader, coldStream, hotWriter), nil
}

// cacheAndTeeStream은 원본 스트림을 Hot 캐시에 저장하면서 동시에 사용자에게 반환합니다.
func (c *DaramjweeCache) cacheAndTeeStream(ctx context.Context, key string, result *FetchResult) (io.ReadCloser, error) {
	if c.HotStore != nil && result.Metadata != nil {
		cacheWriter, err := c.setStreamToStore(ctx, c.HotStore, key, result.Metadata)
		if err != nil {
			level.Error(c.Logger).Log("msg", "failed to get cache writer", "key", key, "err", err)
			return result.Body, err
		}
		teeReader := io.TeeReader(result.Body, cacheWriter)
		return newMultiCloser(teeReader, result.Body, cacheWriter), nil
	}
	return result.Body, nil
}

// multiCloser는 여러 io.Closer를 하나로 묶어줍니다.
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

// cancelWriteCloser는 WriteCloser가 닫힐 때 컨텍스트를 함께 취소합니다.
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

type closerWithCallback struct {
	io.ReadCloser        // 원본 스트림
	callback      func() // Close 후에 호출될 함수
}

// newCloserWithCallback은 원본 ReadCloser와 콜백 함수를 받아
// 새로운 ReadCloser를 생성합니다.
func newCloserWithCallback(rc io.ReadCloser, cb func()) io.ReadCloser {
	return &closerWithCallback{
		ReadCloser: rc,
		callback:   cb,
	}
}

// Close는 먼저 내장된 ReadCloser의 Close를 호출하고, 그 다음에 콜백 함수를 실행합니다.
// 내장된 Closer의 에러를 반환합니다.
func (c *closerWithCallback) Close() error {
	// 콜백 함수는 에러를 반환하지 않으므로, defer를 사용하여
	// 원본 스트림의 Close가 실패하더라도 콜백이 반드시 실행되도록 보장합니다.
	defer c.callback()
	return c.ReadCloser.Close()
}
