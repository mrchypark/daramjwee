package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type persistSourceStore struct {
	data []byte
	meta *Metadata
}

func (s *persistSourceStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return io.NopCloser(bytes.NewReader(bytes.Clone(s.data))), cloneMetadata(s.meta), nil
}

func (s *persistSourceStore) BeginSet(context.Context, string, *Metadata) (WriteSink, error) {
	return nil, errors.New("not supported")
}

func (s *persistSourceStore) Delete(context.Context, string) error {
	return nil
}

func (s *persistSourceStore) Stat(context.Context, string) (*Metadata, error) {
	return cloneMetadata(s.meta), nil
}

type persistDestinationStore struct {
	mu             sync.Mutex
	data           map[string][]byte
	meta           map[string]*Metadata
	closeDelay     time.Duration
	closeStartedCh chan struct{}
	deleteCtxErrCh chan error
}

func newPersistDestinationStore(closeDelay time.Duration) *persistDestinationStore {
	return &persistDestinationStore{
		data:           make(map[string][]byte),
		meta:           make(map[string]*Metadata),
		closeDelay:     closeDelay,
		closeStartedCh: make(chan struct{}, 1),
		deleteCtxErrCh: make(chan error, 1),
	}
}

func (s *persistDestinationStore) GetStream(context.Context, string) (io.ReadCloser, *Metadata, error) {
	return nil, nil, ErrNotFound
}

func (s *persistDestinationStore) BeginSet(_ context.Context, key string, metadata *Metadata) (WriteSink, error) {
	var buf bytes.Buffer
	return &persistDestinationSink{
		buf:        &buf,
		store:      s,
		key:        key,
		metadata:   cloneMetadata(metadata),
		closeDelay: s.closeDelay,
	}, nil
}

func (s *persistDestinationStore) Delete(ctx context.Context, key string) error {
	ctxErr := ctx.Err()
	select {
	case s.deleteCtxErrCh <- ctxErr:
	default:
	}
	if ctxErr != nil {
		return ctxErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.meta, key)
	return nil
}

func (s *persistDestinationStore) Stat(context.Context, string) (*Metadata, error) {
	return nil, ErrNotFound
}

func (s *persistDestinationStore) hasKey(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.meta[key]
	return ok
}

type persistDestinationSink struct {
	buf        *bytes.Buffer
	store      *persistDestinationStore
	key        string
	metadata   *Metadata
	closeDelay time.Duration
	aborted    bool
}

func (s *persistDestinationSink) Write(p []byte) (int, error) {
	return s.buf.Write(p)
}

func (s *persistDestinationSink) Close() error {
	select {
	case s.store.closeStartedCh <- struct{}{}:
	default:
	}
	time.Sleep(s.closeDelay)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.store.data[s.key] = bytes.Clone(s.buf.Bytes())
	s.store.meta[s.key] = cloneMetadata(s.metadata)
	return nil
}

func (s *persistDestinationSink) Abort() error {
	s.aborted = true
	return nil
}

func TestSchedulePersistFromTop_InvalidationCleanupUsesFreshContext(t *testing.T) {
	src := &persistSourceStore{
		data: []byte("top-value"),
		meta: &Metadata{CacheTag: "v1", CachedAt: time.Now()},
	}
	dest := newPersistDestinationStore(80 * time.Millisecond)

	workerManager, err := worker.NewManager("pool", log.NewNopLogger(), 1, 1, 20*time.Millisecond)
	require.NoError(t, err)

	cache := &DaramjweeCache{
		tiers:        []Store{src},
		worker:       workerManager,
		logger:       log.NewNopLogger(),
		opTimeout:    time.Second,
		closeTimeout: time.Second,
	}
	t.Cleanup(cache.Close)

	key := "persist-cleanup-key"
	cache.schedulePersistFromTop(context.Background(), key, 0, tierDestination{tierIndex: 1, store: dest})

	select {
	case <-dest.closeStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("persist destination close did not start")
	}

	cache.noteTopWriteGeneration(key)

	select {
	case ctxErr := <-dest.deleteCtxErrCh:
		assert.NoError(t, ctxErr)
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup delete was not called")
	}

	require.Eventually(t, func() bool {
		return !dest.hasKey(key)
	}, 2*time.Second, 10*time.Millisecond)
}
