package daramjwee

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func BenchmarkRefreshTopEntryCachedAt_1MiB(b *testing.B) {
	const payloadSize = 1 * 1024 * 1024

	payload := bytes.Repeat([]byte("r"), payloadSize)
	store := newBenchmarkRefreshStore(payload)
	cache := &DaramjweeCache{
		Tiers:        []Store{store},
		OpTimeout:    2 * time.Second,
		CloseTimeout: 2 * time.Second,
	}

	b.SetBytes(payloadSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		oldMetadata := store.resetStale()
		if err := cache.refreshTopEntryCachedAt(context.Background(), "refresh-key", oldMetadata, cache.currentTopWriteGeneration("refresh-key")); err != nil {
			b.Fatalf("refreshTopEntryCachedAt: %v", err)
		}
	}
}

type benchmarkRefreshStore struct {
	data []byte
	meta Metadata
}

func newBenchmarkRefreshStore(payload []byte) *benchmarkRefreshStore {
	return &benchmarkRefreshStore{
		data: bytes.Clone(payload),
		meta: Metadata{
			CacheTag: "v1",
			CachedAt: time.Now().Add(-time.Hour),
		},
	}
}

func (s *benchmarkRefreshStore) resetStale() *Metadata {
	s.meta.CachedAt = time.Now().Add(-time.Hour)
	copied := s.meta
	return &copied
}

func (s *benchmarkRefreshStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	metaCopy := s.meta
	return io.NopCloser(bytes.NewReader(s.data)), &metaCopy, nil
}

func (s *benchmarkRefreshStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	metaCopy := &Metadata{}
	if metadata != nil {
		*metaCopy = *metadata
	}

	var buf bytes.Buffer
	buf.Grow(len(s.data))
	return &benchmarkRefreshSink{
		store: s,
		meta:  metaCopy,
		buf:   &buf,
	}, nil
}

func (s *benchmarkRefreshStore) Delete(ctx context.Context, key string) error {
	s.data = nil
	s.meta = Metadata{}
	return nil
}

func (s *benchmarkRefreshStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	metaCopy := s.meta
	return &metaCopy, nil
}

type benchmarkRefreshSink struct {
	store *benchmarkRefreshStore
	meta  *Metadata
	buf   *bytes.Buffer
	done  bool
}

func (s *benchmarkRefreshSink) Write(p []byte) (int, error) {
	if s.done {
		return 0, io.ErrClosedPipe
	}
	return s.buf.Write(p)
}

func (s *benchmarkRefreshSink) Close() error {
	if s.done {
		return nil
	}
	s.done = true

	s.store.data = bytes.Clone(s.buf.Bytes())
	if s.meta != nil {
		s.store.meta = *s.meta
	} else {
		s.store.meta = Metadata{}
	}
	return nil
}

func (s *benchmarkRefreshSink) Abort() error {
	s.done = true
	return nil
}
