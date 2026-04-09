package daramjwee

import (
	"context"
	"io"
	"time"
)

type valueOverlayContext struct {
	primary context.Context
	values  context.Context
}

func (c valueOverlayContext) Deadline() (time.Time, bool) {
	return c.primary.Deadline()
}

func (c valueOverlayContext) Done() <-chan struct{} {
	return c.primary.Done()
}

func (c valueOverlayContext) Err() error {
	return c.primary.Err()
}

func (c valueOverlayContext) Value(key any) any {
	if value := c.primary.Value(key); value != nil {
		return value
	}
	return c.values.Value(key)
}

func detachedValueContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func overlayContextValues(runCtx, valueCtx context.Context) context.Context {
	if valueCtx == nil {
		return runCtx
	}
	return valueOverlayContext{primary: runCtx, values: valueCtx}
}

func (c *DaramjweeCache) isCachedStale(oldMeta *Metadata, positive, negative time.Duration) bool {
	if oldMeta == nil {
		return true
	}

	freshnessLifetime := positive
	if oldMeta.IsNegative {
		freshnessLifetime = negative
	}
	if oldMeta.CachedAt.IsZero() {
		return true
	}

	return time.Now().After(oldMeta.CachedAt.Add(freshnessLifetime))
}

func (c *DaramjweeCache) tierFreshness(index int) (time.Duration, time.Duration) {
	override, ok := c.tierFreshnessOverrides[index]
	if !ok {
		return c.positiveFreshness, c.negativeFreshness
	}
	return override.Positive, override.Negative
}

func (c *DaramjweeCache) isTierCachedStale(oldMeta *Metadata, index int) bool {
	positive, negative := c.tierFreshness(index)
	return c.isCachedStale(oldMeta, positive, negative)
}

// newCtxWithTimeout applies the operation timeout to the context if no deadline is set.
func (c *DaramjweeCache) newCtxWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.opTimeout)
}

func usesContextAfterGetStream(store Store) bool {
	sensitive, ok := store.(GetStreamUsesContext)
	return ok && sensitive.GetStreamUsesContext()
}

func usesContextAfterBeginSet(store Store) bool {
	sensitive, ok := store.(BeginSetUsesContext)
	return ok && sensitive.BeginSetUsesContext()
}

func usesContextAfterFetch(fetcher Fetcher) bool {
	sensitive, ok := fetcher.(FetchUsesContext)
	return ok && sensitive.FetchUsesContext()
}

func (c *DaramjweeCache) getStreamContextForStore(requestCtx, setupCtx context.Context, store Store) context.Context {
	if usesContextAfterGetStream(store) {
		return requestCtx
	}
	return setupCtx
}

func (c *DaramjweeCache) beginSetContextForStore(requestCtx, setupCtx context.Context, store Store) context.Context {
	if usesContextAfterBeginSet(store) {
		return requestCtx
	}
	return setupCtx
}

func (c *DaramjweeCache) fetchContextForFetcher(requestCtx, setupCtx context.Context, fetcher Fetcher) context.Context {
	if usesContextAfterFetch(fetcher) {
		return requestCtx
	}
	return setupCtx
}

func cloneMetadata(meta *Metadata) *Metadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}

func newGetResponse(status GetStatus, body io.ReadCloser, meta *Metadata) *GetResponse {
	resp := &GetResponse{
		Status: status,
		Body:   body,
	}
	if meta != nil {
		resp.Metadata = *meta
	}
	return resp
}

// getStreamFromStore is a wrapper that calls the Store interface's GetStream method.
func (c *DaramjweeCache) getStreamFromStore(ctx context.Context, store Store, key string) (io.ReadCloser, *Metadata, error) {
	stream, meta, err := store.GetStream(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if meta == nil {
		if stream != nil {
			_ = stream.Close()
		}
		return nil, nil, ErrNilMetadata
	}
	return stream, meta, nil
}

// setStreamToStore is a wrapper that calls the Store interface's BeginSet method.
func (c *DaramjweeCache) setStreamToStore(ctx context.Context, store Store, key string, metadata *Metadata) (WriteSink, error) {
	return store.BeginSet(ctx, key, metadata)
}

// deleteFromStore is a wrapper that calls the Store interface's Delete method.
func (c *DaramjweeCache) deleteFromStore(ctx context.Context, store Store, key string) error {
	return store.Delete(ctx, key)
}

// statFromStore is a wrapper that calls the Store interface's Stat method.
func (c *DaramjweeCache) statFromStore(ctx context.Context, store Store, key string) (*Metadata, error) {
	opCtx, cancel := c.newCtxWithTimeout(ctx)
	defer cancel()
	return store.Stat(opCtx, key)
}

func (c *DaramjweeCache) fetchFromOrigin(ctx context.Context, fetcher Fetcher, oldMetadata *Metadata) (*FetchResult, error) {
	return fetcher.Fetch(ctx, oldMetadata)
}

func hasRealStore(store Store) bool {
	if store == nil {
		return false
	}
	_, isNullStore := store.(*nullStore)
	return !isNullStore
}

func (c *DaramjweeCache) topWriteStore() Store {
	if len(c.tiers) == 0 {
		return nil
	}
	return c.tiers[0]
}

type tierDestination struct {
	tierIndex int
	store     Store
}
