package daramjwee

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestRefreshTopEntryCachedAtReadsBodyBeforeBeginSet(t *testing.T) {
	store := &destructiveBeginSetStore{
		data: []byte("original-body"),
		meta: Metadata{
			CacheTag: "v1",
			CachedAt: time.Now().Add(-time.Hour),
		},
	}
	cache := &DaramjweeCache{
		Tiers:         []Store{store},
		OpTimeout:     2 * time.Second,
		WorkerTimeout: 2 * time.Second,
		CloseTimeout:  2 * time.Second,
	}

	old := store.snapshot()
	err := cache.refreshTopEntryCachedAt(context.Background(), "key", old, cache.currentTopWriteGeneration("key"))
	if err != nil {
		t.Fatalf("refreshTopEntryCachedAt: %v", err)
	}

	reader, meta, err := store.GetStream(context.Background(), "key")
	if err != nil {
		t.Fatalf("GetStream after refresh: %v", err)
	}
	defer reader.Close()

	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll after refresh: %v", err)
	}
	if string(body) != "original-body" {
		t.Fatalf("expected original body, got %q", string(body))
	}
	if !meta.CachedAt.After(old.CachedAt) {
		t.Fatalf("expected CachedAt to move forward")
	}
}

type destructiveBeginSetStore struct {
	mu   sync.Mutex
	data []byte
	meta Metadata
}

func (s *destructiveBeginSetStore) snapshot() *Metadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := s.meta
	return &copied
}

func (s *destructiveBeginSetStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := s.meta
	return io.NopCloser(bytes.NewReader(bytes.Clone(s.data))), &meta, nil
}

func (s *destructiveBeginSetStore) BeginSet(ctx context.Context, key string, metadata *Metadata) (WriteSink, error) {
	s.mu.Lock()
	s.data = nil
	s.mu.Unlock()

	var buf bytes.Buffer
	return &destructiveBeginSetSink{
		store: s,
		meta:  cloneMetadata(metadata),
		buf:   &buf,
	}, nil
}

func (s *destructiveBeginSetStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = nil
	s.meta = Metadata{}
	return nil
}

func (s *destructiveBeginSetStore) Stat(ctx context.Context, key string) (*Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := s.meta
	return &meta, nil
}

type destructiveBeginSetSink struct {
	store *destructiveBeginSetStore
	meta  *Metadata
	buf   *bytes.Buffer
	done  bool
}

func (s *destructiveBeginSetSink) Write(p []byte) (int, error) {
	return s.buf.Write(p)
}

func (s *destructiveBeginSetSink) Close() error {
	if s.done {
		return nil
	}
	s.done = true
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.data = bytes.Clone(s.buf.Bytes())
	if s.meta != nil {
		s.store.meta = *s.meta
	}
	return nil
}

func (s *destructiveBeginSetSink) Abort() error {
	s.done = true
	return nil
}
