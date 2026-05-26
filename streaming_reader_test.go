package daramjwee

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamThrough_FullReadAndClosePublishes(t *testing.T) {
	sink := &recordingWriteSink{}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
	assert.Equal(t, []byte("hello"), sink.buf.Bytes())
}

func TestStreamThrough_FullReadWithoutExplicitClosePublishes(t *testing.T) {
	sink := &recordingWriteSink{}
	src := &closeCountingReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	cancelCount := 0
	publishCount := 0
	stream := streamThrough(src, sink, func() { cancelCount++ }, func() { publishCount++ })

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
	assert.Equal(t, 1, src.closeCount)
	assert.Equal(t, 1, cancelCount)
	assert.Equal(t, 1, publishCount)
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
	assert.True(t, sink.aborted)

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
	assert.True(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.aborted)
}

func TestStreamThrough_CloseSurfacesPublishError(t *testing.T) {
	closeErr := errors.New("publish failed")
	sink := &recordingWriteSink{closeErr: closeErr}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	_, err := io.ReadAll(stream)
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, stream.Close(), closeErr)
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
}

func TestStreamThrough_AutoCloseSuppressesPureInvalidation(t *testing.T) {
	sink := &recordingWriteSink{closeErr: ErrTopWriteInvalidated}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	_, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.ErrorIs(t, stream.Close(), ErrTopWriteInvalidated)
}

func TestStreamThrough_AutoCloseReportsInvalidationCleanupError(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	sink := &recordingWriteSink{closeErr: errors.Join(ErrTopWriteInvalidated, cleanupErr)}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	_, err := io.ReadAll(stream)
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorIs(t, stream.Close(), cleanupErr)
	require.ErrorIs(t, stream.Close(), ErrTopWriteInvalidated)
}

func TestStreamThrough_AutoClosePreservesInvalidationCleanupContext(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	metadataErr := errors.New("metadata cleanup failed")
	wrappedCleanupErr := fmt.Errorf("cleanup context: %w", errors.Join(cleanupErr, metadataErr))
	sink := &recordingWriteSink{closeErr: errors.Join(ErrTopWriteInvalidated, wrappedCleanupErr)}
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	_, err := io.ReadAll(stream)
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorIs(t, err, metadataErr)
	require.NotErrorIs(t, err, ErrTopWriteInvalidated)
	require.ErrorContains(t, err, "cleanup context")

	closeErr := stream.Close()
	require.ErrorIs(t, closeErr, cleanupErr)
	require.ErrorIs(t, closeErr, metadataErr)
	require.ErrorIs(t, closeErr, ErrTopWriteInvalidated)
	require.ErrorContains(t, closeErr, "cleanup context")
}

func TestStreamThrough_CopyUsesSourceWriterTo(t *testing.T) {
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.NoError(t, err)
	assert.EqualValues(t, 5, n)
	assert.True(t, src.writeToCalled, "streamThrough should preserve the source WriterTo fast path")
	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.closed)
	assert.Equal(t, []byte("hello"), sink.buf.Bytes())
}

func TestStreamThrough_CopyAutoCloseSuppressesPureInvalidation(t *testing.T) {
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{closeErr: ErrTopWriteInvalidated}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.NoError(t, err)
	assert.EqualValues(t, 5, n)
	require.ErrorIs(t, stream.Close(), ErrTopWriteInvalidated)
}

func TestStreamThrough_CopyAutoCloseReportsInvalidationCleanupError(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{closeErr: errors.Join(ErrTopWriteInvalidated, cleanupErr)}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.ErrorIs(t, err, cleanupErr)
	assert.EqualValues(t, 5, n)
	require.ErrorIs(t, stream.Close(), cleanupErr)
	require.ErrorIs(t, stream.Close(), ErrTopWriteInvalidated)
}

func TestStreamThrough_CopyShortWriteWithWriterToAborts(t *testing.T) {
	src := &writerToSpyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	sink := &recordingWriteSink{shortWrite: true}
	stream := streamThrough(src, sink, nil, nil)

	_, err := io.Copy(io.Discard, stream)
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.True(t, src.writeToCalled, "streamThrough should use the source WriterTo fast path")
	assert.True(t, sink.aborted)

	require.NoError(t, stream.Close())
	assert.True(t, sink.aborted)
	assert.False(t, sink.closed)
}

func TestStreamThrough_CopyWriterToJoinsSourceAndSinkErrors(t *testing.T) {
	readErr := errors.New("source write-to failed")
	sinkErr := errors.New("sink write failed")
	src := &writerToErrorReadCloser{data: []byte("hello"), err: readErr}
	sink := &recordingWriteSink{writeErr: sinkErr}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.ErrorIs(t, err, readErr)
	require.ErrorIs(t, err, sinkErr)
	assert.EqualValues(t, 0, n)
	assert.True(t, sink.aborted)

	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, readErr)
}

func TestStreamThrough_CopyFallbackJoinsSourceAndSinkErrors(t *testing.T) {
	readErr := errors.New("source read failed")
	sinkErr := errors.New("sink write failed")
	src := &singleReadErrorCloser{data: []byte("hello"), err: readErr}
	sink := &recordingWriteSink{writeErr: sinkErr}
	stream := streamThrough(src, sink, nil, nil)

	n, err := io.Copy(io.Discard, stream)
	require.ErrorIs(t, err, readErr)
	require.ErrorIs(t, err, sinkErr)
	assert.EqualValues(t, 5, n)
	assert.True(t, sink.aborted)

	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, readErr)
}

func TestStreamThrough_ConcurrentCloseWaitsAndReturnsFinalError(t *testing.T) {
	closeErr := errors.New("abort failed")
	sink := newBlockingAbortSink(closeErr)
	stream := streamThrough(io.NopCloser(bytes.NewReader([]byte("hello"))), sink, nil, nil)

	const closeCalls = 8
	errs := make(chan error, closeCalls)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- stream.Close()
	}()
	<-sink.abortStarted

	wg.Add(closeCalls - 1)
	for i := 1; i < closeCalls; i++ {
		go func() {
			defer wg.Done()
			errs <- stream.Close()
		}()
	}

	close(sink.releaseAbort)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, closeErr)
	}
}

func TestStreamThrough_AutoPublishClosesSourceBeforeSinkClose(t *testing.T) {
	srcCloseErr := errors.New("source close failed")
	sink := &recordingWriteSink{}
	src := &closeCountingReadCloser{
		Reader:   bytes.NewReader([]byte("hello")),
		closeErr: srcCloseErr,
	}
	publishCount := 0
	stream := streamThrough(src, sink, nil, func() { publishCount++ })

	got, err := io.ReadAll(stream)
	require.ErrorIs(t, err, srcCloseErr)
	assert.Equal(t, []byte("hello"), got)
	assert.Equal(t, 1, src.closeCount)
	assert.False(t, sink.closed)
	assert.True(t, sink.aborted)
	assert.Equal(t, 0, publishCount)
}

func TestStreamThrough_ReadAfterAutoEOFDoesNotTouchClosedSource(t *testing.T) {
	afterCloseErr := errors.New("read after close")
	src := &readAfterCloseTrackingSource{Reader: bytes.NewReader([]byte("hello")), afterCloseErr: afterCloseErr}
	sink := &recordingWriteSink{}
	stream := streamThrough(src, sink, nil, nil)

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), got)

	buf := make([]byte, 1)
	n, err := stream.Read(buf)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, src.readAfterClose)
}

func TestStreamThrough_ReadAfterPartialCloseDoesNotTouchClosedSource(t *testing.T) {
	afterCloseErr := errors.New("read after close")
	src := &readAfterCloseTrackingSource{Reader: bytes.NewReader([]byte("hello")), afterCloseErr: afterCloseErr}
	sink := &recordingWriteSink{}
	stream := streamThrough(src, sink, nil, nil)

	buf := make([]byte, 2)
	n, err := stream.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "he", string(buf[:n]))

	require.NoError(t, stream.Close())

	n, err = stream.Read(buf)
	require.ErrorIs(t, err, io.ErrClosedPipe)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, src.readAfterClose)
	assert.True(t, sink.aborted)
	assert.False(t, sink.closed)
}

func TestStreamThrough_ReadWithDataJoinsReadAndSinkErrors(t *testing.T) {
	readErr := errors.New("source read failed")
	sinkErr := errors.New("sink write failed")
	src := &singleReadErrorCloser{data: []byte("hello"), err: readErr}
	sink := &recordingWriteSink{writeErr: sinkErr}
	stream := streamThrough(src, sink, nil, nil)

	buf := make([]byte, 8)
	n, err := stream.Read(buf)
	assert.Equal(t, 5, n)
	require.ErrorIs(t, err, readErr)
	require.ErrorIs(t, err, sinkErr)
	assert.True(t, sink.aborted)
}

func TestStreamThrough_AutoCloseSuppressesInvalidationButReportsSourceCloseError(t *testing.T) {
	srcCloseErr := errors.New("source close failed")
	src := &closeCountingReadCloser{
		Reader:   bytes.NewReader([]byte("hello")),
		closeErr: srcCloseErr,
	}
	sink := &recordingWriteSink{abortErr: ErrTopWriteInvalidated}
	stream := streamThrough(src, sink, nil, nil)

	got, err := io.ReadAll(stream)
	require.ErrorIs(t, err, srcCloseErr)
	require.NotErrorIs(t, err, ErrTopWriteInvalidated)
	assert.Equal(t, []byte("hello"), got)
	assert.False(t, sink.closed)
	assert.True(t, sink.aborted)

	closeErr := stream.Close()
	require.ErrorIs(t, closeErr, srcCloseErr)
	require.ErrorIs(t, closeErr, ErrTopWriteInvalidated)
}

func TestTopFillSinkPreemptDoesNotWaitForBlockedWrite(t *testing.T) {
	sink := newBlockingWriteSink()
	fill := newPendingTopFillSink(&writeCoordinator{}, nil, nil)
	require.True(t, fill.attach(sink))

	writeDone := make(chan error, 1)
	go func() {
		_, err := fill.Write([]byte("partial"))
		writeDone <- err
	}()

	<-sink.writeStarted

	preemptDone := make(chan error, 1)
	go func() {
		preemptDone <- fill.Preempt()
	}()

	select {
	case err := <-preemptDone:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		close(sink.releaseWrite)
		t.Fatal("preempt waited for blocked write")
	}

	close(sink.releaseWrite)
	require.NoError(t, <-writeDone)
	select {
	case <-sink.abortStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("preempt cleanup did not abort sink after write completed")
	}
}

func TestTopFillSinkPreemptReleasesCoordinatorBeforeBlockedWriteFinishes(t *testing.T) {
	sink := newBlockingWriteSink()
	coord := &writeCoordinator{}
	require.NoError(t, coord.acquireWrite(context.Background()))
	coord.stateMu.Lock()
	generation := coord.reserveGenerationLocked()
	coord.stateMu.Unlock()

	fill := newPendingTopFillSink(coord, nil, nil)
	require.True(t, fill.attach(&coordinatedTopWriteSink{
		WriteSink:   sink,
		coord:       coord,
		generation:  generation,
		waitTimeout: time.Second,
	}))

	writeDone := make(chan error, 1)
	go func() {
		_, err := fill.Write([]byte("partial"))
		writeDone <- err
	}()

	<-sink.writeStarted
	require.NoError(t, fill.Preempt())

	acquireCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, coord.acquireWrite(acquireCtx))
	coord.releaseWrite()

	close(sink.releaseWrite)
	require.NoError(t, <-writeDone)
	select {
	case <-sink.abortStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("preempt cleanup did not abort sink after write completed")
	}
}

func TestTopFillSinkCloseDoesNotPublishDuringBlockedWrite(t *testing.T) {
	sink := newBlockingWriteSink()
	fill := newPendingTopFillSink(&writeCoordinator{}, nil, nil)
	require.True(t, fill.attach(sink))

	writeDone := make(chan error, 1)
	go func() {
		_, err := fill.Write([]byte("partial"))
		writeDone <- err
	}()

	<-sink.writeStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- fill.Close()
	}()

	select {
	case <-sink.closeStarted:
		close(sink.releaseWrite)
		t.Fatal("close published while write was still blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(sink.releaseWrite)
	require.NoError(t, <-writeDone)
	require.NoError(t, <-closeDone)

	select {
	case <-sink.closeStarted:
	default:
		t.Fatal("close did not reach sink after write completed")
	}
}

func TestTopFillSinkTerminalMethodsCancelBeginContext(t *testing.T) {
	t.Run("close cancels after sink close", func(t *testing.T) {
		var cancelCalled bool
		sink := &cancelCheckingWriteSink{
			checkNotCanceled: func() {
				require.False(t, cancelCalled)
			},
		}
		fill := newPendingTopFillSink(&writeCoordinator{}, nil, func() { cancelCalled = true })
		require.True(t, fill.attach(sink))

		require.NoError(t, fill.Close())
		require.True(t, cancelCalled)
		require.True(t, sink.closed)
	})

	t.Run("abort cancels after sink abort", func(t *testing.T) {
		var cancelCalled bool
		sink := &cancelCheckingWriteSink{
			checkNotCanceled: func() {
				require.False(t, cancelCalled)
			},
		}
		fill := newPendingTopFillSink(&writeCoordinator{}, nil, func() { cancelCalled = true })
		require.True(t, fill.attach(sink))

		require.NoError(t, fill.Abort())
		require.True(t, cancelCalled)
		require.True(t, sink.aborted)
	})

	t.Run("setup failure cancels", func(t *testing.T) {
		var cancelCalled bool
		fill := newPendingTopFillSink(&writeCoordinator{}, nil, func() { cancelCalled = true })

		fill.failBeginSet(errors.New("begin failed"))
		require.True(t, cancelCalled)
	})

	t.Run("preempt cancels", func(t *testing.T) {
		var cancelCalled bool
		fill := newPendingTopFillSink(&writeCoordinator{}, nil, func() { cancelCalled = true })

		require.NoError(t, fill.Preempt())
		require.True(t, cancelCalled)
	})
}

type recordingWriteSink struct {
	buf        bytes.Buffer
	closeErr   error
	abortErr   error
	writeErr   error
	shortWrite bool
	closed     bool
	aborted    bool
}

func (s *recordingWriteSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
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

type writerToErrorReadCloser struct {
	data []byte
	err  error
}

func (r *writerToErrorReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (r *writerToErrorReadCloser) WriteTo(w io.Writer) (int64, error) {
	_, _ = w.Write(r.data)
	return 0, r.err
}

func (r *writerToErrorReadCloser) Close() error {
	return nil
}

type closeCountingReadCloser struct {
	io.Reader
	closeCount int
	closeErr   error
}

func (r *closeCountingReadCloser) Close() error {
	r.closeCount++
	return r.closeErr
}

type blockingAbortSink struct {
	recordingWriteSink
	abortErr     error
	abortStarted chan struct{}
	releaseAbort chan struct{}
	once         sync.Once
}

func newBlockingAbortSink(abortErr error) *blockingAbortSink {
	return &blockingAbortSink{
		abortErr:     abortErr,
		abortStarted: make(chan struct{}),
		releaseAbort: make(chan struct{}),
	}
}

func (s *blockingAbortSink) Abort() error {
	s.once.Do(func() {
		close(s.abortStarted)
	})
	<-s.releaseAbort
	s.aborted = true
	return s.abortErr
}

type blockingWriteSink struct {
	writeStarted chan struct{}
	releaseWrite chan struct{}
	closeStarted chan struct{}
	abortStarted chan struct{}
	once         sync.Once
	abortOnce    sync.Once
}

func newBlockingWriteSink() *blockingWriteSink {
	return &blockingWriteSink{
		writeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
		closeStarted: make(chan struct{}),
		abortStarted: make(chan struct{}),
	}
}

func (s *blockingWriteSink) Write(p []byte) (int, error) {
	s.once.Do(func() {
		close(s.writeStarted)
	})
	<-s.releaseWrite
	return len(p), nil
}

func (s *blockingWriteSink) Close() error {
	close(s.closeStarted)
	return nil
}

func (s *blockingWriteSink) Abort() error {
	s.abortOnce.Do(func() {
		close(s.abortStarted)
	})
	return nil
}

type cancelCheckingWriteSink struct {
	checkNotCanceled func()
	closed           bool
	aborted          bool
}

func (s *cancelCheckingWriteSink) Write(p []byte) (int, error) {
	return len(p), nil
}

func (s *cancelCheckingWriteSink) Close() error {
	if s.checkNotCanceled != nil {
		s.checkNotCanceled()
	}
	s.closed = true
	return nil
}

func (s *cancelCheckingWriteSink) Abort() error {
	if s.checkNotCanceled != nil {
		s.checkNotCanceled()
	}
	s.aborted = true
	return nil
}

type readAfterCloseTrackingSource struct {
	*bytes.Reader
	afterCloseErr  error
	closed         bool
	readAfterClose int
}

func (r *readAfterCloseTrackingSource) Read(p []byte) (int, error) {
	if r.closed {
		r.readAfterClose++
		return 0, r.afterCloseErr
	}
	return r.Reader.Read(p)
}

func (r *readAfterCloseTrackingSource) Close() error {
	r.closed = true
	return nil
}

type singleReadErrorCloser struct {
	data []byte
	err  error
	read bool
}

func (r *singleReadErrorCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, r.data), r.err
}

func (r *singleReadErrorCloser) Close() error {
	return nil
}
