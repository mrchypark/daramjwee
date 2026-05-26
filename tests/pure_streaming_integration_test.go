package daramjwee_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_MissReturnsStreamBeforeSourceEOF(t *testing.T) {
	hot := newMockStore()
	source := newBlockingReadCloser([]byte("hello"), []byte(" world"))
	fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{CacheTag: "v1"}}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resultCh := make(chan getResult, 1)
	go func() {
		stream, err := cache.Get(context.Background(), "miss-key", daramjwee.GetRequest{}, fetcher)
		resultCh <- getResult{stream: stream, err: err}
	}()

	var result getResult
	select {
	case result = <-resultCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cache.Get blocked until source EOF on miss")
	}

	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	defer result.stream.Close()

	first := make([]byte, 5)
	n, err := result.stream.Read(first)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(first[:n]))

	_, _, err = hot.GetStream(context.Background(), "miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	source.Release()
	rest, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, " world", string(rest))
	require.NoError(t, result.stream.Close())

	reader, _, err := hot.GetStream(context.Background(), "miss-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(body))
}

func TestCache_ColdHitReturnsStreamBeforeSourceEOF(t *testing.T) {
	hot := newMockStore()
	source := newBlockingReadCloser([]byte("cold"), []byte(" value"))
	cold := &blockingColdStore{
		streamFactory: func() io.ReadCloser { return source },
		metadata:      &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()},
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resultCh := make(chan getResult, 1)
	go func() {
		stream, err := cache.Get(context.Background(), "cold-key", daramjwee.GetRequest{}, &mockFetcher{})
		resultCh <- getResult{stream: stream, err: err}
	}()

	var result getResult
	select {
	case result = <-resultCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cache.Get blocked until cold source EOF on cold hit")
	}

	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	defer result.stream.Close()

	first := make([]byte, 4)
	n, err := result.stream.Read(first)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "cold", string(first[:n]))

	_, _, err = hot.GetStream(context.Background(), "cold-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	source.Release()
	rest, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, " value", string(rest))
	require.NoError(t, result.stream.Close())

	reader, _, err := hot.GetStream(context.Background(), "cold-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "cold value", string(body))
}

func TestCache_MissBeginSetFailureFallsBackToPassthrough(t *testing.T) {
	hot := newMockStore()
	hot.forceSetError = true
	fetcher := &mockFetcher{content: "passthrough", etag: "v1"}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "passthrough", string(body))

	_, _, err = hot.GetStream(context.Background(), "miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_MissFullReadWithoutExplicitClosePublishesAndReleasesKey(t *testing.T) {
	hot := newMockStore()
	fetcher := &mockFetcher{content: "fetched-value", etag: "origin-v1"}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "auto-close-miss-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "fetched-value", string(body))

	current, meta, err := readStoreValue(context.Background(), hot, "auto-close-miss-key")
	require.NoError(t, err)
	assert.Equal(t, "fetched-value", current)
	assert.Equal(t, "origin-v1", meta.CacheTag)

	done := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "auto-close-miss-key", &daramjwee.Metadata{CacheTag: "user-v2"})
		if err != nil {
			done <- err
			return
		}
		if _, err := writer.Write([]byte("user-value")); err != nil {
			_ = writer.Abort()
			done <- err
			return
		}
		done <- writer.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-key Set blocked after miss response was fully read to EOF")
	}

	current, meta, err = readStoreValue(context.Background(), hot, "auto-close-miss-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", string(current))
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_LowerTierFullReadWithoutExplicitClosePublishesAndReleasesKey(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("auto-close-lower-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "auto-close-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(body))

	current, meta, err := readStoreValue(context.Background(), hot, "auto-close-lower-key")
	require.NoError(t, err)
	assert.Equal(t, "lower-value", current)
	assert.Equal(t, "lower-v1", meta.CacheTag)

	done := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "auto-close-lower-key", &daramjwee.Metadata{CacheTag: "user-v2"})
		if err != nil {
			done <- err
			return
		}
		if _, err := writer.Write([]byte("user-value")); err != nil {
			_ = writer.Abort()
			done <- err
			return
		}
		done <- writer.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("same-key Set blocked after lower-tier response was fully read to EOF")
	}

	current, meta, err = readStoreValue(context.Background(), hot, "auto-close-lower-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", string(current))
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_SameKeySetPreemptsUnconsumedMissFill(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "preempt-miss-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- writeCacheValue(cache, "preempt-miss-key", "user-value", "user-v2")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-key Set blocked behind an unconsumed miss fill")
	}

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))
	require.NoError(t, resp.Close())

	current, meta, err := readStoreValue(context.Background(), hot, "preempt-miss-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_SecondSameKeyMissDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "read-read-miss-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "first-origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)
	defer first.Close()

	secondDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "read-read-miss-key", daramjwee.GetRequest{}, &mockFetcher{
			content: "second-origin-value",
			etag:    "origin-v2",
		})
		secondDone <- getResult{stream: resp, err: err}
	}()

	var second getResult
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.stream)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second same-key miss blocked behind unconsumed active fill")
	}
	secondBody, err := io.ReadAll(second.stream)
	require.NoError(t, err)
	assert.Equal(t, "second-origin-value", string(secondBody))
	require.NoError(t, second.stream.Close())

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "first-origin-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_SameKeySetPreemptsMissFillBeforeStoreBeginSetReturns(t *testing.T) {
	hot := newFirstBeginSetBlockingStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	getDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "preempt-before-register-key", daramjwee.GetRequest{}, &mockFetcher{
			content: "origin-value",
			etag:    "origin-v1",
		})
		getDone <- getResult{stream: resp, err: err}
	}()

	select {
	case <-hot.firstBeginSetStarted:
	case <-time.After(time.Second):
		t.Fatal("miss fill did not enter the first BeginSet")
	}

	setDone := make(chan error, 1)
	go func() {
		setDone <- writeCacheValue(cache, "preempt-before-register-key", "user-value", "user-v2")
	}()

	select {
	case <-hot.secondBeginSetStarted:
		t.Fatal("same-key Set entered a second BeginSet before the abandoned fill's BeginSet returned")
	case err := <-setDone:
		t.Fatalf("same-key Set completed before the abandoned fill's BeginSet returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, int32(1), hot.beginSetCalls.Load(), "same-key BeginSet calls must remain serialized")

	hot.releaseFirstBeginSet()
	select {
	case err := <-setDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("same-key Set stayed blocked after the abandoned fill's BeginSet returned")
	}
	result := <-getDone
	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	body, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))
	require.NoError(t, result.stream.Close())

	current, meta, err := readStoreValue(context.Background(), hot.mockStore, "preempt-before-register-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_FillLeaseStartsAfterBeginSetReturns(t *testing.T) {
	hot := newFirstBeginSetBlockingStore()
	const lease = 500 * time.Millisecond
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(lease),
	)
	require.NoError(t, err)
	defer cache.Close()

	getDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "lease-after-begin-key", daramjwee.GetRequest{}, &mockFetcher{
			content: "origin-value",
			etag:    "origin-v1",
		})
		getDone <- getResult{stream: resp, err: err}
	}()

	select {
	case <-hot.firstBeginSetStarted:
	case <-time.After(time.Second):
		t.Fatal("miss fill did not enter the first BeginSet")
	}
	time.Sleep(50 * time.Millisecond)
	hot.releaseFirstBeginSet()

	result := <-getDone
	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	body, err := io.ReadAll(result.stream)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))
	require.NoError(t, result.stream.Close())

	current, meta, err := readStoreValue(context.Background(), hot.mockStore, "lease-after-begin-key")
	require.NoError(t, err)
	assert.Equal(t, "origin-value", current)
	assert.Equal(t, "origin-v1", meta.CacheTag)
}

func TestCache_ConditionalLowerTierPromotionDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("conditional-lower-busy-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "conditional-lower-busy-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	defer first.Close()

	secondDone := make(chan struct {
		resp *daramjwee.GetResponse
		err  error
	}, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "conditional-lower-busy-key", daramjwee.GetRequest{IfNoneMatch: "lower-v1"}, &mockFetcher{})
		secondDone <- struct {
			resp *daramjwee.GetResponse
			err  error
		}{resp: resp, err: err}
	}()

	var second struct {
		resp *daramjwee.GetResponse
		err  error
	}
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.resp)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("conditional lower-tier promotion blocked behind unconsumed active fill")
	}
	assert.Equal(t, daramjwee.GetStatusNotModified, second.resp.Status)

	if second.resp.Body != nil {
		body, err := io.ReadAll(second.resp)
		require.NoError(t, err)
		assert.Equal(t, "lower-value", string(body))
	}
	require.NoError(t, second.resp.Close())

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_SecondSameKeyLowerTierPromotionDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("read-read-lower-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "read-read-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	defer first.Close()

	secondDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "read-read-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
		secondDone <- getResult{stream: resp, err: err}
	}()

	var second getResult
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.stream)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second same-key lower-tier promotion blocked behind unconsumed active fill")
	}
	secondBody, err := io.ReadAll(second.stream)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(secondBody))
	require.NoError(t, second.stream.Close())

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_ConditionalLowerTierHitDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("conditional-read-read-lower-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "conditional-read-read-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	defer first.Close()

	secondDone := make(chan struct {
		resp *daramjwee.GetResponse
		err  error
	}, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "conditional-read-read-lower-key", daramjwee.GetRequest{IfNoneMatch: "lower-v1"}, &mockFetcher{})
		secondDone <- struct {
			resp *daramjwee.GetResponse
			err  error
		}{resp: resp, err: err}
	}()

	var second struct {
		resp *daramjwee.GetResponse
		err  error
	}
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.resp)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("conditional same-key lower-tier hit blocked behind unconsumed active fill")
	}
	require.Equal(t, daramjwee.GetStatusNotModified, second.resp.Status)
	require.Nil(t, second.resp.Body)

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_NegativeMissDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "negative-busy-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)
	defer first.Close()

	secondDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "negative-busy-key", daramjwee.GetRequest{}, &errFetcher{err: daramjwee.ErrCacheableNotFound})
		secondDone <- getResult{stream: resp, err: err}
	}()

	var second getResult
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.stream)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("negative miss blocked behind unconsumed active fill")
	}
	if resp, ok := second.stream.(*daramjwee.GetResponse); assert.True(t, ok) {
		assert.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	}
	require.NoError(t, second.stream.Close())

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_NegativeLowerTierPromotionDoesNotBlockBehindUnconsumedFill(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "negative-lower-busy-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)
	defer first.Close()
	cold.setData("negative-lower-busy-key", "unused", &daramjwee.Metadata{IsNegative: true, CachedAt: time.Now()})

	secondDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "negative-lower-busy-key", daramjwee.GetRequest{}, &mockFetcher{})
		secondDone <- getResult{stream: resp, err: err}
	}()

	var second getResult
	select {
	case second = <-secondDone:
		require.NoError(t, second.err)
		require.NotNil(t, second.stream)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("negative lower-tier promotion blocked behind unconsumed active fill")
	}
	if resp, ok := second.stream.(*daramjwee.GetResponse); assert.True(t, ok) {
		assert.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	}
	require.NoError(t, second.stream.Close())

	firstBody, err := io.ReadAll(first)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(firstBody))
	require.NoError(t, first.Close())
}

func TestCache_PreemptedFillBeginSetFailureReleasesSameKeyWriters(t *testing.T) {
	hot := newFirstBeginSetBlockingErrorStore(errors.New("begin set failed after preempt"))
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	getDone := make(chan getResult, 1)
	go func() {
		resp, err := cache.Get(context.Background(), "failed-preempt-key", daramjwee.GetRequest{}, &mockFetcher{
			content: "origin-value",
			etag:    "origin-v1",
		})
		getDone <- getResult{stream: resp, err: err}
	}()

	select {
	case <-hot.firstBeginSetStarted:
	case <-time.After(time.Second):
		t.Fatal("miss fill did not enter the first BeginSet")
	}

	setDone := make(chan error, 1)
	go func() {
		writer, err := cache.Set(context.Background(), "failed-preempt-key", &daramjwee.Metadata{CacheTag: "user-v2"})
		if err != nil {
			setDone <- err
			return
		}
		if _, err := writer.Write([]byte("user-value")); err != nil {
			_ = writer.Abort()
			setDone <- err
			return
		}
		setDone <- writer.Close()
	}()

	result := <-getDone
	require.NoError(t, result.err)
	require.NotNil(t, result.stream)
	require.NoError(t, result.stream.Close())
	hot.releaseFirstBeginSet()

	select {
	case err := <-setDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("same-key Set stayed blocked after preempted fill BeginSet failed")
	}

	current, meta, err := readStoreValue(context.Background(), hot.mockStore, "failed-preempt-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_ConditionalLowerTierHitAfterTopReadErrorReturnsBody(t *testing.T) {
	top := &getErrorStore{mockStore: newMockStore(), err: errors.New("top read failed")}
	top.setData("top-read-error-key", "top-value-v2", &daramjwee.Metadata{CacheTag: "v2", CachedAt: time.Now()})
	lower := newMockStore()
	lower.setData("top-read-error-key", "lower-value-v1", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "top-read-error-key", daramjwee.GetRequest{IfNoneMatch: "v1"}, &mockFetcher{})
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusOK, resp.Status)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "lower-value-v1", string(body))
	require.NoError(t, resp.Close())
}

func TestCache_LowerTierHitAfterTopReadErrorDoesNotPromoteToTop(t *testing.T) {
	top := &getErrorStore{mockStore: newMockStore(), err: errors.New("top read failed")}
	top.setData("top-read-error-promote-key", "top-value-v2", &daramjwee.Metadata{CacheTag: "v2", CachedAt: time.Now()})
	lower := newMockStore()
	lower.setData("top-read-error-promote-key", "lower-value-v1", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "top-read-error-promote-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusOK, resp.Status)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "lower-value-v1", string(body))
	require.NoError(t, resp.Close())

	current, meta, err := readStoreValue(context.Background(), top.mockStore, "top-read-error-promote-key")
	require.NoError(t, err)
	assert.Equal(t, "top-value-v2", current)
	assert.Equal(t, "v2", meta.CacheTag)
}

func TestCache_NegativeLowerTierHitAfterTopReadErrorReturnsNotFound(t *testing.T) {
	top := &getErrorStore{mockStore: newMockStore(), err: errors.New("top read failed")}
	lower := newMockStore()
	lower.setData("negative-top-read-error-key", "unused", &daramjwee.Metadata{IsNegative: true, CachedAt: time.Now().Add(-time.Hour)})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "negative-top-read-error-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	require.Nil(t, resp.Body)
	require.NoError(t, resp.Close())
}

func TestCache_NotModifiedAfterTopReadErrorReturnsError(t *testing.T) {
	top := &getErrorStore{mockStore: newMockStore(), err: errors.New("top read failed")}
	top.setData("not-modified-top-error-key", "top-value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "not-modified-top-error-key", daramjwee.GetRequest{}, &errFetcher{err: daramjwee.ErrNotModified})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCache_NotModifiedReplayMissReturnsError(t *testing.T) {
	top := &statOnlyMissingStore{meta: &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()}}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "not-modified-replay-miss-key", daramjwee.GetRequest{}, &errFetcher{err: daramjwee.ErrNotModified})
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestCache_NegativeMissFansOutToLowerTier(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	lower.setData("negative-fanout-key", "old-positive", &daramjwee.Metadata{CacheTag: "old", CachedAt: time.Now().Add(-time.Hour)})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "negative-fanout-key", daramjwee.GetRequest{}, &errFetcher{err: daramjwee.ErrCacheableNotFound})
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusOK, resp.Status)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "old-positive", string(body))
	require.NoError(t, resp.Close())

	require.Eventually(t, func() bool {
		meta, err := lower.Stat(context.Background(), "negative-fanout-key")
		return err == nil && meta.IsNegative
	}, 2*time.Second, 10*time.Millisecond)
}

func TestScheduleRefreshSameKeyActiveFillDoesNotBlockWorkerAndClosesBody(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	active, err := cache.Get(context.Background(), "refresh-active-fill-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "active-value",
		etag:    "active-v1",
	})
	require.NoError(t, err)
	defer active.Close()

	closed := make(chan struct{})
	blockedFetcher := &readCloserRefreshFetcher{
		body: &closeTrackingReadCloser{
			Reader:  bytes.NewReader([]byte("refresh-value")),
			onClose: func() { close(closed) },
		},
		meta: &daramjwee.Metadata{CacheTag: "refresh-v1"},
	}
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "refresh-active-fill-key", blockedFetcher))

	secondFetcher := &mockFetcher{content: "other-value", etag: "other-v1"}
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "refresh-other-key", secondFetcher))

	require.Eventually(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return secondFetcher.getFetchCount() > 0 }, 2*time.Second, 10*time.Millisecond)
}

func TestScheduleRefreshClosesBodyWhenWriterBeginFails(t *testing.T) {
	hot := newMockStore()
	hot.forceSetError = true
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithWorkers(1),
		daramjwee.WithWorkerQueue(4),
		daramjwee.WithWorkerTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	closed := make(chan struct{})
	fetcher := &readCloserRefreshFetcher{
		body: &closeTrackingReadCloser{
			Reader:  bytes.NewReader([]byte("refresh-value")),
			onClose: func() { close(closed) },
		},
		meta: &daramjwee.Metadata{CacheTag: "refresh-v1"},
	}
	require.NoError(t, cache.ScheduleRefresh(context.Background(), "refresh-begin-fails-key", fetcher))
	require.Eventually(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_LowerTierNotModifiedFallbackSkipsChangedPositiveEntry(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("fallback-changed-positive-key", "old-body", &daramjwee.Metadata{CacheTag: "old", CachedAt: oldCachedAt})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified, fetchDelay: 100 * time.Millisecond}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "fallback-changed-positive-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "old-body", string(body))
	require.NoError(t, resp.Close())

	lower.setData("fallback-changed-positive-key", "new-body", &daramjwee.Metadata{CacheTag: "new", CachedAt: oldCachedAt})
	require.Eventually(t, func() bool { return fetcher.getFetchCount() > 0 }, 2*time.Second, 10*time.Millisecond)
	_, _, err = top.GetStream(context.Background(), "fallback-changed-positive-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_LowerTierNotModifiedFallbackSkipsChangedNegativeEntry(t *testing.T) {
	top := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("fallback-changed-negative-key", "", &daramjwee.Metadata{IsNegative: true, CacheTag: "old-neg", CachedAt: oldCachedAt})
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified, fetchDelay: 100 * time.Millisecond}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "fallback-changed-negative-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	require.Equal(t, daramjwee.GetStatusNotFound, resp.Status)
	require.NoError(t, resp.Close())

	lower.setData("fallback-changed-negative-key", "new-body", &daramjwee.Metadata{CacheTag: "new", CachedAt: oldCachedAt})
	require.Eventually(t, func() bool { return fetcher.getFetchCount() > 0 }, 2*time.Second, 10*time.Millisecond)
	_, _, err = top.GetStream(context.Background(), "fallback-changed-negative-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_LowerTierNotModifiedFallbackCloseErrorDoesNotPublish(t *testing.T) {
	top := newMockStore()
	closeErr := errors.New("fallback source close failed")
	lower := newSecondReadCloseErrorStore("fallback-close-error-key", []byte("old-body"), &daramjwee.Metadata{CacheTag: "old", CachedAt: time.Now().Add(-time.Hour)}, closeErr)
	fetcher := &mockFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(top, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "fallback-close-error-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "old-body", string(body))
	require.NoError(t, resp.Close())

	require.Eventually(t, lower.secondClosed, 2*time.Second, 10*time.Millisecond)
	_, _, err = top.GetStream(context.Background(), "fallback-close-error-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_SameKeySetPreemptsQueuedMissFills(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	first, err := cache.Get(context.Background(), "queued-preempt-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value-0",
		etag:    "origin-v0",
	})
	require.NoError(t, err)
	defer first.Close()

	const queued = 24
	started := make(chan struct{}, queued)
	results := make(chan getResult, queued)
	for i := 0; i < queued; i++ {
		go func() {
			resp, err := cache.Get(context.Background(), "queued-preempt-key", daramjwee.GetRequest{}, &signalingFetcher{
				started: started,
				content: []byte("origin-value-queued"),
				etag:    "origin-v-queued",
			})
			results <- getResult{stream: resp, err: err}
		}()
	}
	for i := 0; i < queued; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("queued fill %d did not reach the fetcher", i)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- writeCacheValue(cache, "queued-preempt-key", "user-value", "user-v2")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("same-key Set blocked behind queued miss fills")
	}

	for i := 0; i < queued; i++ {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			require.NotNil(t, result.stream)
			body, err := io.ReadAll(result.stream)
			require.NoError(t, err)
			assert.Equal(t, "origin-value-queued", string(body))
			require.NoError(t, result.stream.Close())
		case <-time.After(time.Second):
			t.Fatalf("queued fill %d did not return after Set preempted active fill", i)
		}
	}

	current, meta, err := readStoreValue(context.Background(), hot, "queued-preempt-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_SameKeySetPreemptsPartiallyConsumedMissFill(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "preempt-partial-miss-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)

	prefix := make([]byte, len("origin"))
	n, err := resp.Read(prefix)
	require.NoError(t, err)
	assert.Equal(t, "origin", string(prefix[:n]))

	done := make(chan error, 1)
	go func() {
		done <- writeCacheValue(cache, "preempt-partial-miss-key", "user-value", "user-v2")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-key Set blocked behind a partially consumed miss fill")
	}

	rest, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "-value", string(rest))
	require.NoError(t, resp.Close())

	current, meta, err := readStoreValue(context.Background(), hot, "preempt-partial-miss-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_DeletePreemptsUnconsumedMissFillAndKeepsKeyDeleted(t *testing.T) {
	hot := newMockStore()
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "preempt-delete-miss-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- cache.Delete(context.Background(), "preempt-delete-miss-key")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Delete blocked behind an unconsumed miss fill")
	}

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))
	require.NoError(t, resp.Close())

	_, _, err = hot.GetStream(context.Background(), "preempt-delete-miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_FillLeaseExpiryTurnsMissIntoPassthrough(t *testing.T) {
	hot := newMockStore()
	const lease = 20 * time.Millisecond
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(lease),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "lease-miss-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	require.NoError(t, err)

	requireStoreAbort(t, hot, "lease-miss-key", 2*time.Second)

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "origin-value", string(body))
	require.NoError(t, resp.Close())

	_, _, err = hot.GetStream(context.Background(), "lease-miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_FillLeaseExpiryTurnsLowerTierPromotionIntoPassthrough(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("lease-lower-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	const lease = 20 * time.Millisecond
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(lease),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "lease-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)

	requireStoreAbort(t, hot, "lease-lower-key", 2*time.Second)

	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "lower-value", string(body))
	require.NoError(t, resp.Close())

	_, _, err = hot.GetStream(context.Background(), "lease-lower-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_SameKeySetPreemptsPartiallyConsumedLowerTierPromotion(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("preempt-partial-lower-key", "lower-value", &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "preempt-partial-lower-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)

	prefix := make([]byte, len("lower"))
	n, err := resp.Read(prefix)
	require.NoError(t, err)
	assert.Equal(t, "lower", string(prefix[:n]))

	done := make(chan error, 1)
	go func() {
		done <- writeCacheValue(cache, "preempt-partial-lower-key", "user-value", "user-v2")
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("same-key Set blocked behind a partially consumed lower-tier promotion")
	}

	rest, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "-value", string(rest))
	require.NoError(t, resp.Close())

	current, meta, err := readStoreValue(context.Background(), hot, "preempt-partial-lower-key")
	require.NoError(t, err)
	assert.Equal(t, "user-value", current)
	assert.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_StaleTopTierFullReadWithoutExplicitCloseSchedulesRefresh(t *testing.T) {
	hot := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	hot.setData("auto-refresh-top-key", "stale-value", &daramjwee.Metadata{CacheTag: "old", CachedAt: oldCachedAt})
	fetcher := &mockFetcher{content: "fresh-value", etag: "fresh"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "auto-refresh-top-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "stale-value", string(body))

	require.Eventually(t, func() bool {
		current, meta, err := readStoreValue(context.Background(), hot, "auto-refresh-top-key")
		return err == nil && current == "fresh-value" && meta.CacheTag == "fresh" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_StaleLowerTierFullReadWithoutExplicitCloseSchedulesRefresh(t *testing.T) {
	hot := newMockStore()
	lower := newMockStore()
	oldCachedAt := time.Now().Add(-time.Hour)
	lower.setData("auto-refresh-lower-key", "stale-lower", &daramjwee.Metadata{CacheTag: "old", CachedAt: oldCachedAt})
	fetcher := &mockFetcher{content: "fresh-lower", etag: "fresh"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, lower),
		daramjwee.WithFreshness(time.Second, time.Second),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "auto-refresh-lower-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	body, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "stale-lower", string(body))

	require.Eventually(t, func() bool {
		current, meta, err := readStoreValue(context.Background(), hot, "auto-refresh-lower-key")
		return err == nil && current == "fresh-lower" && meta.CacheTag == "fresh" && fetcher.getFetchCount() > 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCache_TopNegativeHitPropagatesCloseError(t *testing.T) {
	closeErr := errors.New("top negative close failed")
	hot := &singleEntryCloseErrorStore{
		meta:     &daramjwee.Metadata{IsNegative: true, CachedAt: time.Now()},
		closeErr: closeErr,
	}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "top-negative-close-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.Nil(t, resp)
	require.ErrorIs(t, err, closeErr)
}

func TestCache_LowerNegativeHitPropagatesCloseError(t *testing.T) {
	closeErr := errors.New("lower negative close failed")
	hot := newMockStore()
	lower := &singleEntryCloseErrorStore{
		meta:     &daramjwee.Metadata{IsNegative: true, CachedAt: time.Now()},
		closeErr: closeErr,
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "lower-negative-close-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.Nil(t, resp)
	require.ErrorIs(t, err, closeErr)

	_, _, statErr := hot.GetStream(context.Background(), "lower-negative-close-key")
	require.ErrorIs(t, statErr, daramjwee.ErrNotFound)
}

func TestCache_ReplayedNegativeHitPropagatesCloseError(t *testing.T) {
	closeErr := errors.New("replayed negative close failed")
	hot := &statOnlyThenReadableStore{
		meta:     &daramjwee.Metadata{IsNegative: true, CachedAt: time.Now()},
		closeErr: closeErr,
	}
	fetcher := &errFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "replayed-negative-close-key", daramjwee.GetRequest{}, fetcher)
	require.Nil(t, resp)
	require.ErrorIs(t, err, closeErr)
}

func TestCache_TopPositiveHitPropagatesAutoCloseError(t *testing.T) {
	closeErr := errors.New("top positive close failed")
	hot := &singleEntryCloseErrorStore{
		data:     []byte("top-value"),
		meta:     &daramjwee.Metadata{CacheTag: "top-v1", CachedAt: time.Now()},
		closeErr: closeErr,
	}
	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "top-close-error-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.NotNil(t, resp.Body)

	body, err := io.ReadAll(resp.Body)
	assert.Equal(t, "top-value", string(body))
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, resp.Body.Close(), closeErr)
}

func TestCache_StaleLowerTierHitPropagatesAutoCloseError(t *testing.T) {
	closeErr := errors.New("stale lower close failed")
	hot := newMockStore()
	cold := &singleEntryCloseErrorStore{
		data:     []byte("lower-value"),
		meta:     &daramjwee.Metadata{CacheTag: "lower-v1", CachedAt: time.Now().Add(-time.Hour)},
		closeErr: closeErr,
	}
	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Nanosecond, time.Nanosecond),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "stale-lower-close-error-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	require.NotNil(t, resp.Body)

	body, err := io.ReadAll(resp.Body)
	assert.Equal(t, "lower-value", string(body))
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, resp.Body.Close(), closeErr)
}

func TestCache_ConditionalLowerTierPromotionCloseErrorAfterEOFDoesNotPublish(t *testing.T) {
	closeErr := errors.New("lower source close failed after eof")
	hot := newMockStore()
	lower := &singleEntryCloseErrorStore{
		data:     []byte("conditional-value"),
		meta:     &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()},
		closeErr: closeErr,
	}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "conditional-close-key", daramjwee.GetRequest{IfNoneMatch: "v1"}, &mockFetcher{})
	require.ErrorIs(t, err, closeErr)
	require.Nil(t, resp)

	_, _, err = hot.GetStream(context.Background(), "conditional-close-key")
	require.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PartialMissReadCloseDoesNotPublishToHot(t *testing.T) {
	hot := newMockStore()
	fetcher := &mockFetcher{content: "partial-value", etag: "v1"}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "partial-miss-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	buf := make([]byte, 7)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	require.NoError(t, stream.Close())

	_, _, err = hot.GetStream(context.Background(), "partial-miss-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PartialMissStreamDoesNotBlockSameKeySet(t *testing.T) {
	hot := memstore.New(0, nil)
	source := newBlockingReadCloser([]byte("origin"), []byte("-value"))
	fetcher := &blockingSourceFetcher{source: source, metadata: &daramjwee.Metadata{CacheTag: "origin-v1"}}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "same-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	require.NotNil(t, resp)

	buf := make([]byte, len("origin"))
	n, err := resp.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len("origin"), n)
	require.Equal(t, "origin", string(buf[:n]))

	setCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sink, err := cache.Set(setCtx, "same-key", &daramjwee.Metadata{CacheTag: "user-v2"})
	require.NoError(t, err)
	_, err = sink.Write([]byte("user-value"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	source.Release()
	rest, err := io.ReadAll(resp)
	require.NoError(t, err)
	require.Equal(t, "-value", string(rest))
	require.ErrorIs(t, resp.Close(), daramjwee.ErrTopWriteInvalidated)

	reader, meta, err := hot.GetStream(context.Background(), "same-key")
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "user-value", string(body))
	require.Equal(t, "user-v2", meta.CacheTag)
}

func TestCache_PartialColdHitReadCloseDoesNotPublishToHot(t *testing.T) {
	hot := newMockStore()
	cold := newMockStore()
	cold.setData("partial-cold-key", "partial-value", &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "partial-cold-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)

	buf := make([]byte, 7)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	require.NoError(t, stream.Close())

	_, _, err = hot.GetStream(context.Background(), "partial-cold-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
}

func TestCache_PublishFailureDoesNotScheduleColdPersist(t *testing.T) {
	hot := &failingHotStore{}
	cold := newMockStore()
	fetcher := &mockFetcher{content: "value", etag: "v1"}

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, cold),
		daramjwee.WithOpTimeout(2*time.Second),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "publish-fail-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.ErrorIs(t, err, errPublishFailed)
	assert.Equal(t, "value", string(body))
	require.ErrorIs(t, stream.Close(), errPublishFailed)

	require.Never(t, func() bool {
		reader, _, err := cold.GetStream(context.Background(), "publish-fail-key")
		if err != nil {
			return false
		}
		_ = reader.Close()
		return true
	}, 200*time.Millisecond, 20*time.Millisecond)
}

func TestCache_NotModifiedKeepsHotStreamOpenUntilExplicitClose(t *testing.T) {
	hot := &statOnlyThenReadableStore{
		data: []byte("cached-value"),
		meta: &daramjwee.Metadata{CacheTag: "v1"},
	}
	fetcher := &errFetcher{err: daramjwee.ErrNotModified}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(2*time.Second))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "not-modified-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "cached-value", string(body))
	assert.Equal(t, 0, hot.closeCount(), "stream should remain open until caller closes it")

	require.NoError(t, stream.Close())
	assert.Equal(t, 1, hot.closeCount(), "explicit close should close the underlying hot stream exactly once")
}

func TestCache_MissStreamIgnoresOpTimeoutAfterFetchReturns(t *testing.T) {
	hot := newContextBoundStore()
	fetcher := &contextBoundFetcher{body: []byte("timeout-safe-miss"), metadata: &daramjwee.Metadata{CacheTag: "v1"}}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-timeout-key", daramjwee.GetRequest{}, fetcher)
	require.NoError(t, err)
	defer stream.Close()

	time.Sleep(50 * time.Millisecond)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-miss", string(body))
	require.NoError(t, stream.Close())

	reader, meta, err := hot.GetStream(context.Background(), "miss-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-miss", string(persisted))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestCache_MissUsesOpTimeoutForFetchSetup(t *testing.T) {
	hot := newMockStore()
	fetcher := &mockFetcher{content: "timeout-safe-miss", etag: "v1", fetchDelay: 100 * time.Millisecond}

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "miss-timeout-setup-key", daramjwee.GetRequest{}, fetcher)
	require.Error(t, err)
	require.Nil(t, stream)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCache_LowerTierStreamIgnoresOpTimeoutAfterGetReturns(t *testing.T) {
	hot := newContextBoundStore()
	lower := newContextBoundStore()
	lower.set("lower-timeout-key", []byte("lower-timeout-value"), &daramjwee.Metadata{CacheTag: "v1", CachedAt: time.Now()})

	cache, err := daramjwee.New(
		nil,
		daramjwee.WithTiers(hot, lower),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithOpTimeout(20*time.Millisecond),
	)
	require.NoError(t, err)
	defer cache.Close()

	stream, err := cache.Get(context.Background(), "lower-timeout-key", daramjwee.GetRequest{}, &mockFetcher{})
	require.NoError(t, err)
	defer stream.Close()

	time.Sleep(50 * time.Millisecond)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "lower-timeout-value", string(body))
	require.NoError(t, stream.Close())

	reader, meta, err := hot.GetStream(context.Background(), "lower-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "lower-timeout-value", string(persisted))
	assert.Equal(t, "v1", meta.CacheTag)
}

func TestCache_SetSinkIgnoresOpTimeoutAfterBeginSetReturns(t *testing.T) {
	hot := newContextBoundStore()

	cache, err := daramjwee.New(nil, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(20*time.Millisecond))
	require.NoError(t, err)
	defer cache.Close()

	writer, err := cache.Set(context.Background(), "set-timeout-key", &daramjwee.Metadata{CacheTag: "v1"})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = writer.Write([]byte("timeout-safe-set"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader, meta, err := hot.GetStream(context.Background(), "set-timeout-key")
	require.NoError(t, err)
	defer reader.Close()
	persisted, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "timeout-safe-set", string(persisted))
	assert.Equal(t, "v1", meta.CacheTag)
}

type getResult struct {
	stream io.ReadCloser
	err    error
}

type signalingFetcher struct {
	started chan<- struct{}
	content []byte
	etag    string
}

func (f *signalingFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	f.started <- struct{}{}
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(bytes.Clone(f.content))),
		Metadata: &daramjwee.Metadata{CacheTag: f.etag},
	}, nil
}

func requireStoreAbort(t *testing.T, store *mockStore, key string, timeout time.Duration) {
	t.Helper()
	select {
	case abortedKey := <-store.writeAborted:
		require.Equal(t, key, abortedKey)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for store abort for key %q", key)
	}
}

type contextBoundFetcher struct {
	body     []byte
	metadata *daramjwee.Metadata
}

func (f *contextBoundFetcher) FetchUsesContext() bool { return true }

func (f *contextBoundFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     &contextBoundReadCloser{ctx: ctx, Reader: bytes.NewReader(bytes.Clone(f.body))},
		Metadata: f.metadata,
	}, nil
}

type blockingSourceFetcher struct {
	source   *blockingReadCloser
	metadata *daramjwee.Metadata
}

func (f *blockingSourceFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     f.source,
		Metadata: f.metadata,
	}, nil
}

type blockingColdStore struct {
	streamFactory func() io.ReadCloser
	metadata      *daramjwee.Metadata
}

func (s *blockingColdStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return s.streamFactory(), s.metadata, nil
}

func (s *blockingColdStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *blockingColdStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *blockingColdStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.metadata, nil
}

type firstBeginSetBlockingStore struct {
	mockStore             *mockStore
	firstBeginSetStarted  chan struct{}
	secondBeginSetStarted chan struct{}
	releaseFirst          chan struct{}
	beginSetCalls         atomic.Int32
}

func newFirstBeginSetBlockingStore() *firstBeginSetBlockingStore {
	return &firstBeginSetBlockingStore{
		mockStore:             newMockStore(),
		firstBeginSetStarted:  make(chan struct{}),
		secondBeginSetStarted: make(chan struct{}),
		releaseFirst:          make(chan struct{}),
	}
}

func (s *firstBeginSetBlockingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return s.mockStore.GetStream(ctx, key)
}

func (s *firstBeginSetBlockingStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	switch s.beginSetCalls.Add(1) {
	case 1:
		close(s.firstBeginSetStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.releaseFirst:
		}
	case 2:
		close(s.secondBeginSetStarted)
	}
	return s.mockStore.BeginSet(ctx, key, metadata)
}

func (s *firstBeginSetBlockingStore) Delete(ctx context.Context, key string) error {
	return s.mockStore.Delete(ctx, key)
}

func (s *firstBeginSetBlockingStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.mockStore.Stat(ctx, key)
}

func (s *firstBeginSetBlockingStore) releaseFirstBeginSet() {
	select {
	case <-s.releaseFirst:
	default:
		close(s.releaseFirst)
	}
}

type firstBeginSetBlockingErrorStore struct {
	*firstBeginSetBlockingStore
	err error
}

func newFirstBeginSetBlockingErrorStore(err error) *firstBeginSetBlockingErrorStore {
	return &firstBeginSetBlockingErrorStore{
		firstBeginSetBlockingStore: newFirstBeginSetBlockingStore(),
		err:                        err,
	}
}

func (s *firstBeginSetBlockingErrorStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	switch s.beginSetCalls.Add(1) {
	case 1:
		close(s.firstBeginSetStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.releaseFirst:
		}
		return nil, s.err
	case 2:
		close(s.secondBeginSetStarted)
	}
	return s.mockStore.BeginSet(ctx, key, metadata)
}

type getErrorStore struct {
	*mockStore
	err error
}

func (s *getErrorStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, s.err
}

type secondReadCloseErrorStore struct {
	key      string
	data     []byte
	meta     *daramjwee.Metadata
	closeErr error
	reads    atomic.Int32
	closed   atomic.Bool
}

func newSecondReadCloseErrorStore(key string, data []byte, meta *daramjwee.Metadata, closeErr error) *secondReadCloseErrorStore {
	return &secondReadCloseErrorStore{key: key, data: bytes.Clone(data), meta: meta, closeErr: closeErr}
}

func (s *secondReadCloseErrorStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if key != s.key || s.meta == nil {
		return nil, nil, daramjwee.ErrNotFound
	}
	meta := *s.meta
	rc := &closeTrackingReadCloser{Reader: bytes.NewReader(bytes.Clone(s.data))}
	if s.reads.Add(1) >= 2 {
		rc.closeErr = s.closeErr
		rc.onClose = func() { s.closed.Store(true) }
	}
	return rc, &meta, nil
}

func (s *secondReadCloseErrorStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *secondReadCloseErrorStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *secondReadCloseErrorStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if key != s.key || s.meta == nil {
		return nil, daramjwee.ErrNotFound
	}
	meta := *s.meta
	return &meta, nil
}

func (s *secondReadCloseErrorStore) secondClosed() bool {
	return s.closed.Load()
}

type discardSink struct{}

func (s *discardSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *discardSink) Close() error                { return nil }
func (s *discardSink) Abort() error                { return nil }

type contextBoundReadCloser struct {
	ctx context.Context
	*bytes.Reader
}

func (r *contextBoundReadCloser) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}
	return r.Reader.Read(p)
}

func (r *contextBoundReadCloser) Close() error {
	return nil
}

type contextBoundStore struct {
	mu   sync.Mutex
	data map[string][]byte
	meta map[string]*daramjwee.Metadata
}

func newContextBoundStore() *contextBoundStore {
	return &contextBoundStore{
		data: make(map[string][]byte),
		meta: make(map[string]*daramjwee.Metadata),
	}
}

func (s *contextBoundStore) GetStreamUsesContext() bool { return true }

func (s *contextBoundStore) BeginSetUsesContext() bool { return true }

func (s *contextBoundStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	body, ok := s.data[key]
	if !ok {
		return nil, nil, daramjwee.ErrNotFound
	}
	meta := *s.meta[key]
	return &contextBoundReadCloser{ctx: ctx, Reader: bytes.NewReader(bytes.Clone(body))}, &meta, nil
}

func (s *contextBoundStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	metaCopy := &daramjwee.Metadata{}
	if metadata != nil {
		*metaCopy = *metadata
	}
	return &contextBoundSink{ctx: ctx, store: s, key: key, meta: metaCopy}, nil
}

func (s *contextBoundStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

func (s *contextBoundStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.meta[key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}
	metaCopy := *meta
	return &metaCopy, nil
}

func (s *contextBoundStore) set(key string, body []byte, meta *daramjwee.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = bytes.Clone(body)
	metaCopy := &daramjwee.Metadata{}
	if meta != nil {
		*metaCopy = *meta
	}
	s.meta[key] = metaCopy
}

type contextBoundSink struct {
	ctx   context.Context
	store *contextBoundStore
	key   string
	meta  *daramjwee.Metadata
	buf   bytes.Buffer
	done  atomic.Bool
}

func (s *contextBoundSink) Write(p []byte) (int, error) {
	if s.done.Load() {
		return 0, io.ErrClosedPipe
	}
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
	}
	return s.buf.Write(p)
}

func (s *contextBoundSink) Close() error {
	if !s.done.CompareAndSwap(false, true) {
		return nil
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.store.set(s.key, s.buf.Bytes(), s.meta)
	return nil
}

func (s *contextBoundSink) Abort() error {
	s.done.Store(true)
	return nil
}

type errFetcher struct {
	err error
}

func (f *errFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return nil, f.err
}

type closeTrackingReadCloser struct {
	*bytes.Reader
	once     sync.Once
	onClose  func()
	closeErr error
}

func (r *closeTrackingReadCloser) Close() error {
	r.once.Do(func() {
		if r.onClose != nil {
			r.onClose()
		}
	})
	return r.closeErr
}

type statOnlyThenReadableStore struct {
	data     []byte
	meta     *daramjwee.Metadata
	getCalls int32
	closed   int32
	closeErr error
}

func (s *statOnlyThenReadableStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if atomic.AddInt32(&s.getCalls, 1) == 1 {
		return nil, nil, daramjwee.ErrNotFound
	}
	return &closeTrackingReadCloser{
		Reader: bytes.NewReader(s.data),
		onClose: func() {
			atomic.AddInt32(&s.closed, 1)
		},
		closeErr: s.closeErr,
	}, s.meta, nil
}

func (s *statOnlyThenReadableStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *statOnlyThenReadableStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *statOnlyThenReadableStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return s.meta, nil
}

func (s *statOnlyThenReadableStore) closeCount() int {
	return int(atomic.LoadInt32(&s.closed))
}

type statOnlyMissingStore struct {
	meta *daramjwee.Metadata
}

func (s *statOnlyMissingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (s *statOnlyMissingStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *statOnlyMissingStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *statOnlyMissingStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if s.meta == nil {
		return nil, daramjwee.ErrNotFound
	}
	meta := *s.meta
	return &meta, nil
}

type singleEntryCloseErrorStore struct {
	data     []byte
	meta     *daramjwee.Metadata
	closeErr error
}

func (s *singleEntryCloseErrorStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if s.meta == nil {
		return nil, nil, daramjwee.ErrNotFound
	}
	meta := *s.meta
	return &closeTrackingReadCloser{
		Reader:   bytes.NewReader(bytes.Clone(s.data)),
		closeErr: s.closeErr,
	}, &meta, nil
}

func (s *singleEntryCloseErrorStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &discardSink{}, nil
}

func (s *singleEntryCloseErrorStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *singleEntryCloseErrorStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if s.meta == nil {
		return nil, daramjwee.ErrNotFound
	}
	meta := *s.meta
	return &meta, nil
}

type blockingReadCloser struct {
	first     []byte
	second    []byte
	releaseCh chan struct{}
	stage     int
}

func newBlockingReadCloser(first, second []byte) *blockingReadCloser {
	return &blockingReadCloser{
		first:     bytes.Clone(first),
		second:    bytes.Clone(second),
		releaseCh: make(chan struct{}),
	}
}

func (r *blockingReadCloser) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage = 1
		return copy(p, r.first), nil
	case 1:
		<-r.releaseCh
		r.stage = 2
		return copy(p, r.second), nil
	default:
		return 0, io.EOF
	}
}

func (r *blockingReadCloser) Close() error {
	r.Release()
	return nil
}

func (r *blockingReadCloser) Release() {
	select {
	case <-r.releaseCh:
	default:
		close(r.releaseCh)
	}
}

var errPublishFailed = assert.AnError

type failingHotStore struct{}

func (s *failingHotStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	return nil, nil, daramjwee.ErrNotFound
}

func (s *failingHotStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return &failingWriteSink{}, nil
}

func (s *failingHotStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *failingHotStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	return nil, daramjwee.ErrNotFound
}

type failingWriteSink struct {
	bytes.Buffer
}

func (s *failingWriteSink) Close() error { return errPublishFailed }
func (s *failingWriteSink) Abort() error { return nil }
