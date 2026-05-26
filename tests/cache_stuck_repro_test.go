package daramjwee_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

const cacheStuckReproEnv = "DJ_REPRO_CACHE_STUCK"

func requireCacheStuckReproEnabled(tb testing.TB) {
	tb.Helper()
	if os.Getenv(cacheStuckReproEnv) != "1" {
		tb.Skipf("set %s=1 to run cache stuck reproduction", cacheStuckReproEnv)
	}
}

func TestFileStore_UnconsumedMissBodyDoesNotBlockSameKeyUpdate(t *testing.T) {
	store, err := filestore.New(t.TempDir(), log.NewNopLogger())
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	cache, err := daramjwee.New(
		log.NewNopLogger(),
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	resp, err := cache.Get(context.Background(), "filestore-unconsumed-key", daramjwee.GetRequest{}, &mockFetcher{
		content: "origin-value",
		etag:    "origin-v1",
	})
	if err != nil {
		t.Fatalf("get miss: %v", err)
	}
	defer resp.Close()

	done := make(chan error, 1)
	go func() {
		done <- writeCacheValue(cache, "filestore-unconsumed-key", "user-value", "user-v2")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("same-key update failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("same-key filestore update blocked behind an unconsumed miss body")
	}

	body, err := io.ReadAll(resp)
	if err != nil {
		t.Fatalf("read original response body: %v", err)
	}
	if string(body) != "origin-value" {
		t.Fatalf("original response body = %q, want origin-value", body)
	}

	current, meta, err := readStoreValue(context.Background(), store, "filestore-unconsumed-key")
	if err != nil {
		t.Fatalf("read filestore value: %v", err)
	}
	if current != "user-value" {
		t.Fatalf("filestore value = %q, want user-value", current)
	}
	if meta.CacheTag != "user-v2" {
		t.Fatalf("filestore cache tag = %q, want user-v2", meta.CacheTag)
	}
}

func TestFileStore_UnconsumedMissBodyDoesNotBlockHeldAndControlUpdates(t *testing.T) {
	store, err := filestore.New(t.TempDir(), log.NewNopLogger())
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	cache, err := daramjwee.New(
		log.NewNopLogger(),
		daramjwee.WithTiers(store),
		daramjwee.WithOpTimeout(2*time.Second),
		daramjwee.WithFillLeaseTimeout(0),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	const heldKey = "filestore-held-two-key"
	const controlKey = "filestore-control-two-key"
	heldResp, err := cache.Get(context.Background(), heldKey, daramjwee.GetRequest{}, &mockFetcher{
		content: "held-origin",
		etag:    "held-origin-v1",
	})
	if err != nil {
		t.Fatalf("get held miss: %v", err)
	}
	defer heldResp.Close()

	done := make(chan reproWriteResult, 2)
	go func() {
		done <- reproWriteResult{index: 0, err: writeCacheValue(cache, heldKey, "held-user", "held-user-v2")}
	}()
	go func() {
		done <- reproWriteResult{index: 1, err: writeCacheValue(cache, controlKey, "control-user", "control-user-v2")}
	}()

	for i := 0; i < 2; i++ {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatalf("update %d failed: %v", result.index, result.err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("filestore updates blocked behind one unconsumed miss body")
		}
	}

	body, err := io.ReadAll(heldResp)
	if err != nil {
		t.Fatalf("read held response body: %v", err)
	}
	if string(body) != "held-origin" {
		t.Fatalf("held response body = %q, want held-origin", body)
	}
	if err := heldResp.Close(); err != nil {
		t.Fatalf("close held response: %v", err)
	}

	got, meta, err := readStoreValue(context.Background(), store, heldKey)
	if err != nil {
		t.Fatalf("read held filestore value: %v", err)
	}
	if got != "held-user" || meta.CacheTag != "held-user-v2" {
		t.Fatalf("held filestore value=%q tag=%q, want value=held-user tag=held-user-v2", got, meta.CacheTag)
	}

	got, meta, err = readStoreValue(context.Background(), store, controlKey)
	if err != nil {
		t.Fatalf("read control filestore value: %v", err)
	}
	if got != "control-user" || meta.CacheTag != "control-user-v2" {
		t.Fatalf("control filestore value=%q tag=%q, want value=control-user tag=control-user-v2", got, meta.CacheTag)
	}
}

func TestRepro_UnconsumedMissBodyDoesNotBlockSameKeyUpdate(t *testing.T) {
	requireCacheStuckReproEnabled(t)

	hot := newMockStore()
	diag := newReproDiagnosticLogger()
	cache, err := daramjwee.New(diag, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	stuckKey := "repro-stuck-key"
	freeKey := "repro-free-key"

	stuckResp, err := cache.Get(context.Background(), stuckKey, daramjwee.GetRequest{}, &mockFetcher{
		content: "seed",
		etag:    "seed",
	})
	if err != nil {
		t.Fatalf("get stuck key: %v", err)
	}

	sameKeyDone := make(chan error, 1)
	go func() {
		sameKeyDone <- writeCacheValue(cache, stuckKey, "updated", "updated")
	}()

	otherKeyDone := make(chan error, 1)
	go func() {
		otherKeyDone <- writeCacheValue(cache, freeKey, "other", "other")
	}()

	select {
	case err := <-otherKeyDone:
		if err != nil {
			_ = stuckResp.Close()
			t.Fatalf("other key update failed: %v\n%s", err, diag.Dump(200))
		}
	case <-time.After(250 * time.Millisecond):
		_ = stuckResp.Close()
		t.Fatalf("control update for unrelated key also blocked\n%s", diag.Dump(200))
	}

	select {
	case err := <-sameKeyDone:
		if err != nil {
			_ = stuckResp.Close()
			t.Fatalf("same-key update failed: %v\n%s", err, diag.Dump(200))
		}
	case <-time.After(250 * time.Millisecond):
		_ = stuckResp.Close()
		t.Fatalf("same-key update blocked behind the unclosed response\n%s", diag.Dump(200))
	}

	if err := stuckResp.Close(); err != nil {
		t.Fatalf("cleanup close stuck response: %v\n%s", err, diag.Dump(200))
	}
	t.Logf("fixed: same-key update completed while the original cache miss response body remained unclosed\n%s", diag.Dump(200))
}

func BenchmarkRepro_UnconsumedBodiesNoLongerCauseKeySpecificUpdateStalls(b *testing.B) {
	requireCacheStuckReproEnabled(b)

	totalStalled := 0
	for i := 0; i < b.N; i++ {
		stalled, diag := runLargeUnclosedBodyUpdateRepro(b, 512, 128, 300*time.Millisecond)
		totalStalled += stalled
		if stalled > 0 {
			b.Fatalf("%d same-key updates stalled behind unclosed response bodies\n%s", stalled, diag.Dump(120))
		}
	}
	b.ReportMetric(float64(totalStalled)/float64(b.N), "stalled_keys/op")
}

func FuzzRepro_UnconsumedBodyDoesNotBlockSelectedKeys(f *testing.F) {
	f.Add([]byte{0xff, 0x0f, 0xf0, 0x55, 0xaa})
	f.Add([]byte{0x01})

	f.Fuzz(func(t *testing.T, pattern []byte) {
		requireCacheStuckReproEnabled(t)
		if len(pattern) == 0 {
			return
		}

		keyCount := 8 + int(pattern[0]%24)
		hold := make([]bool, keyCount)
		heldCount := 0
		for i := 0; i < keyCount; i++ {
			if pattern[i%len(pattern)]&(1<<uint(i%8)) != 0 {
				hold[i] = true
				heldCount++
			}
		}
		if heldCount == 0 {
			return
		}

		stalled, diag := runPatternUnclosedBodyUpdateRepro(t, hold, 200*time.Millisecond)
		if stalled > 0 {
			t.Fatalf("%d selected-key updates stalled behind unclosed response bodies; held=%d\n%s", stalled, heldCount, diag.Dump(120))
		}
	})
}

func TestRepro_AllClosedMissesPublishEveryFileStoreKey(t *testing.T) {
	const keyCount = 256
	store, err := filestore.New(t.TempDir(), log.NewNopLogger())
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	diag := newReproDiagnosticLogger()
	cache, err := daramjwee.New(
		diag,
		daramjwee.WithTiers(store),
		daramjwee.WithWorkers(16),
		daramjwee.WithWorkerQueue(1024),
		daramjwee.WithWorkerTimeout(5*time.Second),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	var wg sync.WaitGroup
	errs := make(chan error, keyCount)
	for i := 0; i < keyCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("closed-miss-key-%04d", i)
			value := fmt.Sprintf("miss-value-%04d", i)
			resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, staticFetcher{
				value:    value,
				cacheTag: value,
			})
			if err != nil {
				errs <- fmt.Errorf("%s get: %w", key, err)
				return
			}
			body, readErr := io.ReadAll(resp)
			closeErr := resp.Close()
			if readErr != nil || closeErr != nil {
				errs <- fmt.Errorf("%s read=%v close=%v", key, readErr, closeErr)
				return
			}
			if string(body) != value {
				errs <- fmt.Errorf("%s response body=%q want=%q", key, body, value)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("%v\n%s", err, diag.Dump(200))
		}
	}

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("closed-miss-key-%04d", i)
		want := fmt.Sprintf("miss-value-%04d", i)
		got, meta, err := readStoreValue(context.Background(), store, key)
		if err != nil {
			t.Fatalf("%s read store: %v\n%s", key, err, diag.Dump(200))
		}
		if got != want || meta.CacheTag != want {
			t.Fatalf("%s store=%q tag=%q want=%q\n%s", key, got, meta.CacheTag, want, diag.Dump(200))
		}
	}
}

func TestRepro_AllClosedStaleRefreshesUpdateEveryFileStoreKey(t *testing.T) {
	const keyCount = 256
	ctx := context.Background()
	store, err := filestore.New(t.TempDir(), log.NewNopLogger())
	if err != nil {
		t.Fatalf("new filestore: %v", err)
	}
	oldCachedAt := time.Now().Add(-2 * time.Hour)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("closed-stale-key-%04d", i)
		writer, err := store.BeginSet(ctx, key, &daramjwee.Metadata{
			CacheTag: fmt.Sprintf("old-%04d", i),
			CachedAt: oldCachedAt,
		})
		if err != nil {
			t.Fatalf("%s seed begin: %v", key, err)
		}
		if _, err := io.WriteString(writer, fmt.Sprintf("old-value-%04d", i)); err != nil {
			_ = writer.Abort()
			t.Fatalf("%s seed write: %v", key, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("%s seed close: %v", key, err)
		}
	}

	diag := newReproDiagnosticLogger()
	cache, err := daramjwee.New(
		diag,
		daramjwee.WithTiers(store),
		daramjwee.WithFreshness(time.Hour, time.Hour),
		daramjwee.WithWorkers(16),
		daramjwee.WithWorkerQueue(1024),
		daramjwee.WithWorkerTimeout(5*time.Second),
		daramjwee.WithOpTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	var wg sync.WaitGroup
	errs := make(chan error, keyCount)
	for i := 0; i < keyCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("closed-stale-key-%04d", i)
			newValue := fmt.Sprintf("new-value-%04d", i)
			resp, err := cache.Get(ctx, key, daramjwee.GetRequest{}, staticFetcher{
				value:    newValue,
				cacheTag: fmt.Sprintf("new-%04d", i),
			})
			if err != nil {
				errs <- fmt.Errorf("%s get: %w", key, err)
				return
			}
			body, readErr := io.ReadAll(resp)
			closeErr := resp.Close()
			if readErr != nil || closeErr != nil {
				errs <- fmt.Errorf("%s read=%v close=%v", key, readErr, closeErr)
				return
			}
			if string(body) != fmt.Sprintf("old-value-%04d", i) {
				errs <- fmt.Errorf("%s stale body=%q", key, body)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("%v\n%s", err, diag.Dump(200))
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		missing := firstNotRefreshedKey(t, store, keyCount)
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale refresh did not update %s after every response was closed\n%s", missing, diag.Dump(240))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runLargeUnclosedBodyUpdateRepro(tb testing.TB, keyCount, heldCount int, wait time.Duration) (int, *reproDiagnosticLogger) {
	tb.Helper()
	hold := make([]bool, keyCount)
	for i := 0; i < heldCount && i < keyCount; i++ {
		hold[i] = true
	}
	return runPatternUnclosedBodyUpdateRepro(tb, hold, wait)
}

func runPatternUnclosedBodyUpdateRepro(tb testing.TB, hold []bool, wait time.Duration) (int, *reproDiagnosticLogger) {
	tb.Helper()

	hot := newMockStore()
	diag := newReproDiagnosticLogger()
	cache, err := daramjwee.New(diag, daramjwee.WithTiers(hot), daramjwee.WithOpTimeout(5*time.Second))
	if err != nil {
		tb.Fatalf("new cache: %v", err)
	}
	defer cache.Close()

	heldResponses := make([]*daramjwee.GetResponse, 0, len(hold))
	for i, shouldHold := range hold {
		if !shouldHold {
			continue
		}
		key := fmt.Sprintf("repro-key-%04d", i)
		resp, err := cache.Get(context.Background(), key, daramjwee.GetRequest{}, &mockFetcher{
			content: "seed",
			etag:    "seed",
		})
		if err != nil {
			closeReproResponses(heldResponses)
			tb.Fatalf("prime held response %s: %v\n%s", key, err, diag.Dump(120))
		}
		heldResponses = append(heldResponses, resp)
	}

	var wg sync.WaitGroup
	started := make(chan struct{}, len(hold))
	release := make(chan struct{})
	results := make(chan reproWriteResult, len(hold))
	for i := range hold {
		i := i
		key := fmt.Sprintf("repro-key-%04d", i)
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			started <- struct{}{}
			<-release
			results <- reproWriteResult{index: i, err: writeCacheValue(cache, key, "updated", "updated")}
		}(key)
	}
	for range hold {
		<-started
	}
	close(release)

	time.Sleep(wait)
	completed := make([]bool, len(hold))
drainResults:
	for {
		select {
		case result := <-results:
			if result.err != nil {
				closeReproResponses(heldResponses)
				tb.Fatalf("write for key index %d returned error instead of blocking/completing: %v\n%s", result.index, result.err, diag.Dump(120))
			}
			completed[result.index] = true
		default:
			break drainResults
		}
	}

	stalled := 0
	stalledControl := 0
	for i, shouldHold := range hold {
		if completed[i] {
			continue
		}
		if shouldHold {
			stalled++
		} else {
			stalledControl++
		}
	}
	if stalledControl > 0 {
		closeReproResponses(heldResponses)
		tb.Fatalf("%d control keys stalled without held response bodies\n%s", stalledControl, diag.Dump(120))
	}

	closeReproResponses(heldResponses)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		tb.Fatalf("cleanup timed out waiting for blocked updates to finish\n%s", diag.Dump(120))
	}

	return stalled, diag
}

type reproWriteResult struct {
	index int
	err   error
}

func closeReproResponses(responses []*daramjwee.GetResponse) {
	for _, resp := range responses {
		_ = resp.Close()
	}
}

func writeCacheValue(cache daramjwee.Cache, key, value, tag string) error {
	writer, err := cache.Set(context.Background(), key, &daramjwee.Metadata{CacheTag: tag})
	if err != nil {
		return err
	}
	if _, err := io.WriteString(writer, value); err != nil {
		abortErr := writer.Abort()
		if abortErr != nil {
			return fmt.Errorf("write: %w; abort: %v", err, abortErr)
		}
		return err
	}
	return writer.Close()
}

type staticFetcher struct {
	value    string
	cacheTag string
}

func (f staticFetcher) Fetch(context.Context, *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.value)),
		Metadata: &daramjwee.Metadata{CacheTag: f.cacheTag},
	}, nil
}

func firstNotRefreshedKey(tb testing.TB, store daramjwee.Store, keyCount int) string {
	tb.Helper()
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("closed-stale-key-%04d", i)
		wantValue := fmt.Sprintf("new-value-%04d", i)
		wantTag := fmt.Sprintf("new-%04d", i)
		got, meta, err := readStoreValue(context.Background(), store, key)
		if err != nil {
			return fmt.Sprintf("%s: %v", key, err)
		}
		if got != wantValue || meta.CacheTag != wantTag {
			return fmt.Sprintf("%s: got value=%q tag=%q want value=%q tag=%q", key, got, meta.CacheTag, wantValue, wantTag)
		}
	}
	return ""
}

func readStoreValue(ctx context.Context, store daramjwee.Store, key string) (string, *daramjwee.Metadata, error) {
	reader, meta, err := store.GetStream(ctx, key)
	if err != nil {
		return "", nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, err
	}
	return string(body), meta, nil
}

type reproDiagnosticLogger struct {
	mu     sync.Mutex
	start  time.Time
	events []string
}

func newReproDiagnosticLogger() *reproDiagnosticLogger {
	return &reproDiagnosticLogger{start: time.Now()}
}

func (l *reproDiagnosticLogger) Log(keyvals ...any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "+%s", time.Since(l.start).Truncate(time.Microsecond))
	for i := 0; i < len(keyvals); i += 2 {
		key := keyvals[i]
		var value any = "<missing>"
		if i+1 < len(keyvals) {
			value = keyvals[i+1]
		}
		fmt.Fprintf(&b, " %v=%v", key, value)
	}
	l.events = append(l.events, b.String())
	return nil
}

func (l *reproDiagnosticLogger) Dump(limit int) string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.events) == 0 {
		return "diagnostic events: <none>"
	}

	start := 0
	if limit > 0 && len(l.events) > limit {
		start = len(l.events) - limit
	}

	var b strings.Builder
	if start > 0 {
		fmt.Fprintf(&b, "diagnostic events: showing last %d of %d\n", len(l.events)-start, len(l.events))
	} else {
		fmt.Fprintf(&b, "diagnostic events: %d\n", len(l.events))
	}
	for _, event := range l.events[start:] {
		b.WriteString(event)
		b.WriteByte('\n')
	}
	return b.String()
}
