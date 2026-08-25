package daramjwee

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadSession_FinishExactlyOnce(t *testing.T) {
	var closeCount atomic.Int32
	source := &countingCloser{closeCount: &closeCount}

	var cancelCount atomic.Int32
	cancel := func() {
		cancelCount.Add(1)
	}

	session := newReadSession(source, cancel, nil)

	// Finish multiple times - should only execute once
	session.Finish(OutcomeEarlyClose)
	session.Finish(OutcomeEOF) // Should be ignored

	require.Equal(t, int32(1), closeCount.Load(), "source should be closed exactly once")
	require.Equal(t, int32(1), cancelCount.Load(), "cancel should be called exactly once")
}

func TestReadSession_SourceNotClosedOnEOF(t *testing.T) {
	var closeCount atomic.Int32
	source := &countingCloser{closeCount: &closeCount}

	session := newReadSession(source, nil, nil)

	// On EOF, source should NOT be closed (the caller is responsible)
	session.Finish(OutcomeEOF)

	require.Equal(t, int32(0), closeCount.Load(), "source should not be closed on EOF")
}

func TestReadSession_SourceClosedOnEarlyClose(t *testing.T) {
	var closeCount atomic.Int32
	source := &countingCloser{closeCount: &closeCount}

	session := newReadSession(source, nil, nil)

	session.Finish(OutcomeEarlyClose)

	require.Equal(t, int32(1), closeCount.Load(), "source should be closed on early close")
}

func TestReadSession_MissLeaderReleased(t *testing.T) {
	var releaseCount atomic.Int32

	session := newReadSession(nil, nil, nil)
	session.SetMissLeader(nil, func() {
		releaseCount.Add(1)
	})

	session.Finish(OutcomeEOF)

	require.Equal(t, int32(1), releaseCount.Load(), "miss leader should be released exactly once")
}

func TestReadSession_WaitForPublish(t *testing.T) {
	session := newReadSession(nil, nil, nil)

	done := make(chan struct{})
	go func() {
		session.WaitForPublish()
		close(done)
	}()

	// Finish the session - should unblock WaitForPublish
	go func() {
		session.Finish(OutcomeEOF)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("WaitForPublish did not unblock after Finish")
	}
}

type countingCloser struct {
	closeCount *atomic.Int32
}

func (c *countingCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (c *countingCloser) Close() error {
	c.closeCount.Add(1)
	return nil
}
