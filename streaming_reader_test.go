package daramjwee

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamThrough_FullReadAndClosePublishes(t *testing.T) {
	sink := &recordingWriteSink{}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
	assert.False(t, sink.closed)
	assert.False(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
	assert.Equal(t, []byte("hello"), sink.buf.Bytes())
}

func TestStreamThrough_FullReadWithoutCloseDoesNotPublish(t *testing.T) {
	sink := &recordingWriteSink{}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
	assert.False(t, sink.closed)
	assert.False(t, sink.aborted)
}

func TestStreamThrough_PartialReadThenCloseAborts(t *testing.T) {
	sink := &recordingWriteSink{}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	buf := make([]byte, 2)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "he", string(buf[:n]))

	require.NoError(t, stream.Close())
	assert.False(t, sink.closed)
	assert.True(t, sink.aborted)
}

func TestStreamThrough_SourceErrorAborts(t *testing.T) {
	sink := &recordingWriteSink{}
	readErr := errors.New("source read failed")
	stream := streamThrough(&errorReadCloser{chunks: [][]byte{[]byte("he")}, err: readErr}, sink, nil, nil)

	all, err := io.ReadAll(stream)
	require.ErrorIs(t, err, readErr)
	assert.Equal(t, []byte("he"), all)

	require.NoError(t, stream.Close())
	assert.False(t, sink.closed)
	assert.True(t, sink.aborted)
}

func TestStreamThrough_ShortWriteReturnsErrShortWrite(t *testing.T) {
	sink := &recordingWriteSink{shortWrite: true}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	buf := make([]byte, 5)
	n, err := stream.Read(buf)
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.Equal(t, 5, n)

	require.NoError(t, stream.Close())
	assert.True(t, sink.aborted)
}

func TestStreamThrough_CloseSurfacesPublishError(t *testing.T) {
	closeErr := errors.New("publish failed")
	sink := &recordingWriteSink{closeErr: closeErr}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	_, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.ErrorIs(t, stream.Close(), closeErr)
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
}

func TestStreamThrough_CopyUsesSourceWriterTo(t *testing.T) {
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.NoError(t, err)
	assert.EqualValues(t, 5, n)
	assert.True(t, src.writeToCalled, "streamThrough should preserve the source WriterTo fast path")
	assert.False(t, sink.closed)
	assert.False(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.closed)
	assert.Equal(t, []byte("hello"), sink.buf.Bytes())
}

func TestStreamThrough_CopyShortWriteWithWriterToAborts(t *testing.T) {
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{shortWrite: true}
	stream := streamThrough(src, sink, nil, nil)

	_, err := io.Copy(io.Discard, stream)
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.True(t, src.writeToCalled, "streamThrough should use the source WriterTo fast path")

	require.NoError(t, stream.Close())
	assert.True(t, sink.aborted)
	assert.False(t, sink.closed)
}

func TestStreamThrough_ConcurrentClosePublishesOnceAfterEOF(t *testing.T) {
	sink := &countingWriteSink{}
	var published atomic.Int32

	stream := streamThrough(
		io.NopCloser(bytes.NewReader([]byte("hello"))),
		sink,
		nil,
		func() { published.Add(1) },
	)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "hello", string(body))

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- stream.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, 1, sink.closeCalls())
	assert.Equal(t, 0, sink.abortCalls())
	assert.Equal(t, int32(1), published.Load())
	assert.Equal(t, []byte("hello"), sink.snapshot())
}

func TestStreamThrough_ConcurrentCloseBeforeEOFAbortsOnce(t *testing.T) {
	sink := &countingWriteSink{}
	var published atomic.Int32

	stream := streamThrough(
		io.NopCloser(bytes.NewReader([]byte("hello"))),
		sink,
		nil,
		func() { published.Add(1) },
	)

	buf := make([]byte, 2)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- stream.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, 0, sink.closeCalls())
	assert.Equal(t, 1, sink.abortCalls())
	assert.Equal(t, int32(0), published.Load())
	assert.Equal(t, []byte("he"), sink.snapshot())
}

type recordingWriteSink struct {
	buf        bytes.Buffer
	closeErr   error
	abortErr   error
	shortWrite bool
	closed     bool
	aborted    bool
}

func (s *recordingWriteSink) Write(p []byte) (int, error) {
	if s.shortWrite && len(p) > 0 {
		n, err := s.buf.Write(p[:len(p)-1])
		if err != nil {
			return n, err
		}
		return n, nil
	}
	return s.buf.Write(p)
}

func (s *recordingWriteSink) Close() error {
	s.closed = true
	return s.closeErr
}

func (s *recordingWriteSink) Abort() error {
	s.aborted = true
	return s.abortErr
}

type errorReadCloser struct {
	chunks [][]byte
	err    error
	index  int
}

func (r *errorReadCloser) Read(p []byte) (int, error) {
	if r.index < len(r.chunks) {
		n := copy(p, r.chunks[r.index])
		r.index++
		return n, nil
	}
	return 0, r.err
}

func (r *errorReadCloser) Close() error {
	return nil
}

type writerToSpyReadCloser struct {
	*bytes.Reader
	writeToCalled bool
}

func (r *writerToSpyReadCloser) WriteTo(w io.Writer) (int64, error) {
	r.writeToCalled = true
	return r.Reader.WriteTo(w)
}

func (r *writerToSpyReadCloser) Close() error {
	return nil
}

type countingWriteSink struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	closeCount int
	abortCount int
}

func (s *countingWriteSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *countingWriteSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCount++
	return nil
}

func (s *countingWriteSink) Abort() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortCount++
	return nil
}

func (s *countingWriteSink) closeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func (s *countingWriteSink) abortCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abortCount
}

func (s *countingWriteSink) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.buf.Bytes())
}
