package daramjwee

import (
	"io"
	"sync"
)

// safeCloser pool for reducing allocations on hot path.
var safeCloserPool = sync.Pool{
	New: func() any { return &safeCloser{} },
}

// pooledSafeCloser returns a pooled safeCloser.
func pooledSafeCloser(rc io.ReadCloser, h closeHandler) *safeCloser {
	sc, _ := safeCloserPool.Get().(*safeCloser)
	sc.ReadCloser = rc
	sc.handler = h
	sc.closeOnce = sync.Once{}
	sc.closeErr = nil
	return sc
}

// releaseSafeCloser returns a safeCloser to the pool.
func releaseSafeCloser(sc *safeCloser) {
	if sc != nil {
		sc.ReadCloser = nil
		sc.handler = nil
		safeCloserPool.Put(sc)
	}
}

// cancelOnCloseReadCloser pool.
var cancelOnClosePool = sync.Pool{
	New: func() any { return &cancelOnCloseReadCloser{} },
}

// pooledCancelOnCloseReadCloser returns a pooled cancelOnCloseReadCloser.
func pooledCancelOnCloseReadCloser(rc io.ReadCloser, cancel func()) *cancelOnCloseReadCloser {
	cc, _ := cancelOnClosePool.Get().(*cancelOnCloseReadCloser)
	cc.ReadCloser = rc
	cc.cancel = cancel
	cc.closeOnce = sync.Once{}
	cc.closeErr = nil
	return cc
}

// releaseCancelOnCloseReadCloser returns a cancelOnCloseReadCloser to the pool.
func releaseCancelOnCloseReadCloser(cc *cancelOnCloseReadCloser) {
	if cc != nil {
		cc.ReadCloser = nil
		cc.cancel = nil
		cancelOnClosePool.Put(cc)
	}
}
