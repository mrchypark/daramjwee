package daramjwee

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee/internal/worker"
)

type rejectReason string

const (
	rejectReasonGroupClosing rejectReason = "group_closing"
	rejectReasonCacheClosed  rejectReason = "cache_closed"
	rejectReasonQueueFull    rejectReason = "queue_full"
)

func (r rejectReason) String() string { return string(r) }

type groupRuntime struct {
	logger log.Logger

	mu             sync.Mutex
	cond           *sync.Cond
	beforeJobStart func(cacheID string, kind JobKind)
	caches         map[string]*groupRuntimeCacheState
	order          []string
	nextIdx        int
	closing        bool

	wg      sync.WaitGroup
	timeout time.Duration

	acceptedByKind  map[JobKind]int
	rejectedByKind  map[JobKind]int
	rejectedByCause map[rejectReason]int
}

type groupRuntimeCacheState struct {
	cacheID    string
	weight     int
	credit     int
	queueLimit int
	queue      chan queuedBackgroundJob
	ctx        context.Context
	cancel     context.CancelFunc
	active     int
	closed     bool
	closeDone  chan struct{}
	closeSent  bool
}

type queuedBackgroundJob struct {
	cacheID string
	kind    JobKind
	job     worker.Job
}

func newGroupRuntime(logger log.Logger, workers, queueLimit int, timeout time.Duration) (*groupRuntime, error) {
	if workers <= 0 {
		workers = 1
	}
	if queueLimit <= 0 {
		queueLimit = 1
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	rt := &groupRuntime{
		logger:          logger,
		caches:          make(map[string]*groupRuntimeCacheState),
		timeout:         timeout,
		acceptedByKind:  make(map[JobKind]int),
		rejectedByKind:  make(map[JobKind]int),
		rejectedByCause: make(map[rejectReason]int),
	}
	rt.cond = sync.NewCond(&rt.mu)

	rt.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go rt.workerLoop(i)
	}
	return rt, nil
}

func (r *groupRuntime) Register(cacheID string, cfg CacheRuntimeConfig) error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return &ConfigError{"cache group is closed"}
	}
	if cacheID == "" {
		return &ConfigError{"cache name cannot be empty"}
	}
	if _, exists := r.caches[cacheID]; exists {
		return &ConfigError{fmt.Sprintf("duplicate cache name %q", cacheID)}
	}

	cacheCtx, cancel := context.WithCancel(context.Background())
	queueLimit := maxInt(cfg.QueueLimit, 1)
	r.caches[cacheID] = &groupRuntimeCacheState{
		cacheID:    cacheID,
		weight:     maxInt(cfg.Weight, 1),
		queueLimit: queueLimit,
		queue:      make(chan queuedBackgroundJob, queueLimit),
		ctx:        cacheCtx,
		cancel:     cancel,
	}
	r.order = append(r.order, cacheID)
	r.cond.Broadcast()
	return nil
}

func (r *groupRuntime) Submit(cacheID string, kind JobKind, job worker.Job) bool {
	if r == nil {
		return false
	}

	r.mu.Lock()
	state, ok := r.caches[cacheID]
	if !ok {
		r.noteRejectLocked(cacheID, kind, rejectReasonCacheClosed, 0, 0)
		r.mu.Unlock()
		return false
	}
	if r.closing {
		r.noteRejectLocked(cacheID, kind, rejectReasonGroupClosing, len(state.queue), state.queueLimit)
		r.mu.Unlock()
		return false
	}
	if state.closed {
		r.noteRejectLocked(cacheID, kind, rejectReasonCacheClosed, len(state.queue), state.queueLimit)
		r.mu.Unlock()
		return false
	}
	depth := len(state.queue)
	if depth >= state.queueLimit {
		r.noteRejectLocked(cacheID, kind, rejectReasonQueueFull, depth, state.queueLimit)
		r.mu.Unlock()
		return false
	}
	state.queue <- queuedBackgroundJob{cacheID: cacheID, kind: kind, job: job}
	r.acceptedByKind[kind]++
	level.Debug(r.logger).Log(
		"msg", "queued background job",
		"cache_id", cacheID,
		"job_kind", kind.String(),
		"queue_depth", len(state.queue),
		"queue_limit", state.queueLimit,
	)
	r.cond.Signal()
	r.mu.Unlock()
	return true
}

func (r *groupRuntime) RemoveCache(cacheID string) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.caches[cacheID]; ok {
		delete(r.caches, cacheID)
		for i, id := range r.order {
			if id == cacheID {
				r.order = append(r.order[:i], r.order[i+1:]...)
				if r.nextIdx >= len(r.order) {
					r.nextIdx = 0
				}
				break
			}
		}
		r.cond.Broadcast()
	}
}

func (r *groupRuntime) noteRejectLocked(cacheID string, kind JobKind, reason rejectReason, depth, limit int) {
	r.rejectedByKind[kind]++
	r.rejectedByCause[reason]++
	level.Warn(r.logger).Log(
		"msg", "rejected background job",
		"cache_id", cacheID,
		"job_kind", kind.String(),
		"queue_depth", depth,
		"queue_limit", limit,
		"reject_reason", reason.String(),
	)
}

func (r *groupRuntime) CloseCache(cacheID string, timeout time.Duration) error {
	if r == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = r.timeout
	}

	r.mu.Lock()
	state, ok := r.caches[cacheID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	if state.closed {
		done := r.cacheCloseDoneLocked(state)
		r.mu.Unlock()
		return r.waitForCacheClose(cacheID, done, timeout)
	}

	state.closed = true
	state.cancel()
	dropped := len(state.queue)
	for len(state.queue) > 0 {
		<-state.queue
	}
	r.cond.Broadcast()
	done := r.cacheCloseDoneLocked(state)
	r.mu.Unlock()

	level.Info(r.logger).Log("msg", "closing cache runtime", "cache_id", cacheID, "dropped_jobs", dropped, "timeout", timeout)
	return r.waitForCacheClose(cacheID, done, timeout)
}

func (r *groupRuntime) Shutdown(timeout time.Duration) error {
	if r == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = r.timeout
	}

	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil
	}
	r.closing = true
	for cacheID, state := range r.caches {
		if !state.closed {
			state.closed = true
			state.cancel()
		}
		dropped := len(state.queue)
		for len(state.queue) > 0 {
			<-state.queue
		}
		r.cacheCloseDoneLocked(state)
		level.Info(r.logger).Log("msg", "closing cache runtime", "cache_id", cacheID, "dropped_jobs", dropped, "timeout", timeout)
	}
	r.cond.Broadcast()
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		level.Warn(r.logger).Log("msg", "group runtime shutdown timed out", "timeout", timeout)
		return worker.ErrShutdownTimeout
	}
}

func (r *groupRuntime) workerLoop(workerID int) {
	defer r.wg.Done()
	for {
		cacheID, job, state, ok := r.nextJob()
		if !ok {
			return
		}

		r.mu.Lock()
		closed := state.closed
		r.mu.Unlock()
		if closed {
			r.mu.Lock()
			state.active--
			r.notifyCacheActivityLocked(state)
			r.mu.Unlock()
			level.Debug(r.logger).Log("msg", "dropping dequeued job for closed cache", "cache_id", cacheID, "job_kind", job.kind.String())
			continue
		}

		if hook := r.beforeJobStart; hook != nil {
			hook(cacheID, job.kind)
		}
		ctx, cancel := context.WithTimeout(state.ctx, r.timeout)
		level.Debug(r.logger).Log("msg", "starting background job", "cache_id", cacheID, "job_kind", job.kind.String())
		job.job(ctx)
		cancel()
		r.mu.Lock()
		state.active--
		r.notifyCacheActivityLocked(state)
		r.mu.Unlock()
		level.Debug(r.logger).Log("msg", "finished background job", "cache_id", cacheID, "job_kind", job.kind.String())
	}
}

func (r *groupRuntime) nextJob() (string, queuedBackgroundJob, *groupRuntimeCacheState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		if r.closing && r.allQueuesEmptyLocked() {
			return "", queuedBackgroundJob{}, nil, false
		}

		cacheID, job, state, ok := r.pickNextLocked()
		if ok {
			return cacheID, job, state, true
		}

		r.cond.Wait()
	}
}

func (r *groupRuntime) allQueuesEmptyLocked() bool {
	for _, cacheID := range r.order {
		state := r.caches[cacheID]
		if state != nil && !state.closed && len(state.queue) > 0 {
			return false
		}
	}
	return true
}

func (r *groupRuntime) pickNextLocked() (string, queuedBackgroundJob, *groupRuntimeCacheState, bool) {
	if len(r.order) == 0 {
		return "", queuedBackgroundJob{}, nil, false
	}

	total := len(r.order)
	for checked := 0; checked < total; checked++ {
		idx := (r.nextIdx + checked) % total
		cacheID := r.order[idx]
		state := r.caches[cacheID]
		if state == nil || state.closed || len(state.queue) == 0 {
			continue
		}
		if state.credit <= 0 {
			state.credit = state.weight
		}
		var job queuedBackgroundJob
		select {
		case job = <-state.queue:
		default:
			continue
		}
		state.credit--
		if state.credit <= 0 {
			r.nextIdx = (idx + 1) % total
		} else {
			r.nextIdx = idx
		}
		state.active++
		return cacheID, job, state, true
	}
	return "", queuedBackgroundJob{}, nil, false
}

func (r *groupRuntime) cacheCloseDoneLocked(state *groupRuntimeCacheState) chan struct{} {
	if state.closeDone == nil {
		state.closeDone = make(chan struct{})
	}
	if state.closed && state.active == 0 && !state.closeSent {
		close(state.closeDone)
		state.closeSent = true
	}
	return state.closeDone
}

func (r *groupRuntime) notifyCacheActivityLocked(state *groupRuntimeCacheState) {
	if state.closed && state.active == 0 {
		r.cacheCloseDoneLocked(state)
	}
	if state.active == 0 {
		r.cond.Broadcast()
	}
}

func (r *groupRuntime) waitForCacheClose(cacheID string, done <-chan struct{}, timeout time.Duration) error {
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		level.Warn(r.logger).Log("msg", "cache close timed out", "cache_id", cacheID, "timeout", timeout)
		return worker.ErrShutdownTimeout
	}
}
