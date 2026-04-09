package daramjwee

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spyReadCloser struct {
	io.Reader
	closeErr   error
	closeCount int32
}

func (s *spyReadCloser) Close() error {
	atomic.AddInt32(&s.closeCount, 1)
	return s.closeErr
}

func TestGetResponse_ReadAndCloseDelegateToBody(t *testing.T) {
	body := &spyReadCloser{Reader: bytes.NewReader([]byte("hello"))}
	resp := &GetResponse{Body: body}

	buf := make([]byte, 5)
	n, err := resp.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf[:n]))

	require.NoError(t, resp.Close())
	assert.EqualValues(t, 1, atomic.LoadInt32(&body.closeCount))
}

func TestGetResponse_NilBodyIsSafe(t *testing.T) {
	var resp *GetResponse

	n, err := resp.Read(make([]byte, 1))
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, resp.Close())

	resp = &GetResponse{}
	n, err = resp.Read(make([]byte, 1))
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, resp.Close())
}

func TestCancelOnCloseReadCloser_CancelsOnceAndReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	body := &spyReadCloser{
		Reader:   bytes.NewReader([]byte("hello")),
		closeErr: closeErr,
	}
	var cancelCalls int32

	rc := newCancelOnCloseReadCloser(body, func() {
		atomic.AddInt32(&cancelCalls, 1)
	})

	err := rc.Close()
	require.ErrorIs(t, err, closeErr)
	err = rc.Close()
	require.ErrorIs(t, err, closeErr)
	assert.EqualValues(t, 1, atomic.LoadInt32(&body.closeCount))
	assert.EqualValues(t, 1, atomic.LoadInt32(&cancelCalls))
}

func TestCancelWriteSink_CloseCancelsOnceAndPreservesError(t *testing.T) {
	closeErr := errors.New("publish failed")
	sink := &recordingWriteSink{closeErr: closeErr}
	var cancelCalls int32

	wrapped := newCancelWriteSink(sink, func() {
		atomic.AddInt32(&cancelCalls, 1)
	})

	err := wrapped.Close()
	require.ErrorIs(t, err, closeErr)
	err = wrapped.Close()
	require.ErrorIs(t, err, closeErr)

	assert.True(t, sink.closed)
	assert.False(t, sink.aborted)
	assert.EqualValues(t, 1, atomic.LoadInt32(&cancelCalls))
}

func TestCancelWriteSink_AbortCancelsOnceAndPreservesError(t *testing.T) {
	abortErr := errors.New("abort failed")
	sink := &recordingWriteSink{abortErr: abortErr}
	var cancelCalls int32

	wrapped := newCancelWriteSink(sink, func() {
		atomic.AddInt32(&cancelCalls, 1)
	})

	err := wrapped.Abort()
	require.ErrorIs(t, err, abortErr)
	err = wrapped.Abort()
	require.ErrorIs(t, err, abortErr)

	assert.True(t, sink.aborted)
	assert.False(t, sink.closed)
	assert.EqualValues(t, 1, atomic.LoadInt32(&cancelCalls))
}
