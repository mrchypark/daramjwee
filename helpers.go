package daramjwee

import (
	"bytes"
	"context"
	"io"
)

// --- Fetcher Adapters ---

// ByteFetchFunc is a function signature for a simple fetch operation that returns raw bytes.
type ByteFetchFunc func(ctx context.Context) ([]byte, error)

// StreamFetchFunc is a function signature for a fetch operation that returns a stream.
type StreamFetchFunc func(ctx context.Context) (io.ReadCloser, error)

// FromBytes is a helper function that adapts a ByteFetchFunc into a daramjwee.Fetcher.
// This is useful for origins that return small payloads like JSON APIs.
func FromBytes(fn ByteFetchFunc) Fetcher {
	return &byteFetcherAdapter{fetchFn: fn}
}

// FromStream is a helper function that adapts a StreamFetchFunc into a daramjwee.Fetcher.
// This is useful for origins that provide data as a stream.
func FromStream(fn StreamFetchFunc) Fetcher {
	return &streamFetcherAdapter{fetchFn: fn}
}

// --- Internal Adapter Implementations ---

// byteFetcherAdapter wraps a ByteFetchFunc to satisfy the Fetcher interface.
type byteFetcherAdapter struct {
	fetchFn ByteFetchFunc
}

func (a *byteFetcherAdapter) Fetch(ctx context.Context, oldETag string) (*FetchResult, error) {
	// This simple adapter does not support ETag checks.
	data, err := a.fetchFn(ctx)
	if err != nil {
		return nil, err
	}
	return &FetchResult{
		// []byte를 io.ReadCloser로 변환하여 스트림-온리 계약을 만족시킵니다.
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

// streamFetcherAdapter wraps a StreamFetchFunc to satisfy the Fetcher interface.
type streamFetcherAdapter struct {
	fetchFn StreamFetchFunc
}

func (a *streamFetcherAdapter) Fetch(ctx context.Context, oldETag string) (*FetchResult, error) {
	// This simple adapter does not support ETag checks.
	stream, err := a.fetchFn(ctx)
	if err != nil {
		return nil, err
	}
	return &FetchResult{
		Body: stream,
	}, nil
}
