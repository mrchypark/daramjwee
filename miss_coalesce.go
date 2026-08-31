package daramjwee

import (
	"context"
	"sync"
	"time"
)

// missWaitCap bounds how long a miss waiter waits for the miss leader to
// publish to the top tier before falling back to its own origin fetch.
// It keeps waiter latency bounded when the leader's caller streams slowly
// or abandons the response, while still coalescing fast origin fetches.
const missWaitCap = 200 * time.Millisecond

// missLead is the leader state for a single key's miss fill.
// done is closed when the leader's response lifecycle ends (body closed
// for streaming responses, or immediately for non-body responses).
type missLead struct {
	done     chan struct{}
	signalOn sync.Once
}

func (l *missLead) signal() {
	if l == nil || l.done == nil {
		return
	}
	l.signalOn.Do(func() { close(l.done) })
}

// finished reports whether the lead's lifecycle already ended.
func (l *missLead) finished() bool {
	if l == nil || l.done == nil {
		return true
	}
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// wait blocks until the leader finishes, the context is done, or the
// internal wait cap elapses. It returns true when the caller should stop
// waiting (timeout or context cancellation).
func (l *missLead) wait(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(missWaitCap)
	defer timer.Stop()
	select {
	case <-l.done:
		return false
	case <-ctx.Done():
		return true
	case <-timer.C:
		return true
	}
}

// missCoordinator tracks the active miss leader per key.
type missCoordinator struct {
	leads sync.Map // key → *missLead
}

// tryLead attempts to become the miss leader for key. It returns the lead
// and true when this call won the leadership, otherwise the current lead
// and false. Finished leads are replaced atomically.
func (m *missCoordinator) tryLead(key string) (*missLead, bool) {
	for {
		if existing, ok := m.leads.Load(key); ok {
			lead, _ := existing.(*missLead)
			if lead.finished() {
				// Leader lifecycle ended: replace it so new callers can
				// take over without waiting on a stale lead.
				m.leads.CompareAndDelete(key, lead)
				continue
			}
			return lead, false
		}
		lead := &missLead{done: make(chan struct{})}
		if _, loaded := m.leads.LoadOrStore(key, lead); !loaded {
			return lead, true
		}
	}
}

// current returns the active miss leader for key, if any.
func (m *missCoordinator) current(key string) (*missLead, bool) {
	existing, ok := m.leads.Load(key)
	if !ok {
		return nil, false
	}
	lead, _ := existing.(*missLead)
	return lead, true
}

// release removes the leader registration for key when it belongs to lead.
func (m *missCoordinator) release(key string, lead *missLead) {
	if lead == nil {
		return
	}
	m.leads.CompareAndDelete(key, lead)
}
