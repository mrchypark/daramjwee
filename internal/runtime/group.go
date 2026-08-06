package runtime

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

type Group struct {
	logger log.Logger

	mu      sync.Mutex
	cond    *sync.Cond
	caches  map[string]*groupCacheState
	order   []string
	nextIdx int
	closing bool

	wg      sync.WaitGroup
	timeout time.Duration
}

type groupCacheState struct {
	cacheID    string
	weight     int
	credit     int
	queueLimit int
	queue      chan queuedJob
	ctx        context.Context
	cancel     context.CancelFunc
	active     int
	closed     bool
	closeDone  chan struct{}
	closeSent  bool
}

type queuedJob struct {
	cacheID string
	kind    JobKind
	job     worker.Job
	onDrop  func()
}

func NewGroup(logger log.Logger, workers int, timeout time.Duration) Manager {
	if workers <= 0 {
		workers = 1
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	rt := &Group{
		logger:  logger,
		caches:  make(map[string]*groupCacheState),
		timeout: timeout,
	}
	rt.cond = sync.NewCond(&rt.mu)

	rt.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go rt.workerLoop(i)
	}
	return rt
}

func (r *Group) Register(cacheID string, cfg Config) error {
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
	r.caches[cacheID] = &groupCacheState{
		cacheID:    cacheID,
		weight:     maxInt(cfg.Weight, 1),
		queueLimit: queueLimit,
		queue:      make(chan queuedJob, queueLimit),
		ctx:        cacheCtx,
		cancel:     cancel,
	}
	r.order = append(r.order, cacheID)
	r.cond.Broadcast()
	return nil
}

func (r *Group) Submit(cacheID string, kind JobKind, job worker.Job) bool {
	return r.submit(cacheID, kind, job, nil)
}

func (r *Group) SubmitWithDropCleanup(cacheID string, kind JobKind, job worker.Job, onDrop func()) bool {
	return r.submit(cacheID, kind, job, onDrop)
}

func (r *Group) submit(cacheID string, kind JobKind, job worker.Job, onDrop func()) bool {
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
	select {
	case state.queue <- queuedJob{cacheID: cacheID, kind: kind, job: job, onDrop: onDrop}:
	default:
		r.noteRejectLocked(cacheID, kind, rejectReasonQueueFull, len(state.queue), state.queueLimit)
		r.mu.Unlock()
		return false
	}
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

func (j queuedJob) drop() {
	if j.onDrop != nil {
		j.onDrop()
	}
}

func (r *Group) RemoveCache(cacheID string) {
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
				if i < r.nextIdx {
					r.nextIdx--
				} else if r.nextIdx >= len(r.order) {
					r.nextIdx = 0
				}
				break
			}
		}
		r.cond.Broadcast()
	}
}

func (r *Group) noteRejectLocked(cacheID string, kind JobKind, reason rejectReason, depth, limit int) {
	level.Warn(r.logger).Log(
		"msg", "rejected background job",
		"cache_id", cacheID,
		"job_kind", kind.String(),
		"queue_depth", depth,
		"queue_limit", limit,
		"reject_reason", reason.String(),
	)
}

func (r *Group) CloseCache(cacheID string, timeout time.Duration) error {
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
	var droppedJobs []queuedJob
	for len(state.queue) > 0 {
		droppedJobs = append(droppedJobs, <-state.queue)
	}
	r.cond.Broadcast()
	done := r.cacheCloseDoneLocked(state)
	r.mu.Unlock()

	for _, job := range droppedJobs {
		job.drop()
	}

	level.Info(r.logger).Log("msg", "closing cache runtime", "cache_id", cacheID, "dropped_jobs", dropped, "timeout", timeout)
	return r.waitForCacheClose(cacheID, done, timeout)
}

func (r *Group) Shutdown(timeout time.Duration) error {
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
	var droppedJobs []queuedJob
	for cacheID, state := range r.caches {
		if !state.closed {
			state.closed = true
			state.cancel()
		}
		dropped := len(state.queue)
		for len(state.queue) > 0 {
			droppedJobs = append(droppedJobs, <-state.queue)
		}
		r.cacheCloseDoneLocked(state)
		level.Info(r.logger).Log("msg", "closing cache runtime", "cache_id", cacheID, "dropped_jobs", dropped, "timeout", timeout)
	}
	r.cond.Broadcast()
	r.mu.Unlock()

	for _, job := range droppedJobs {
		job.drop()
	}

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

func (r *Group) workerLoop(workerID int) {
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
			job.drop()
			r.mu.Lock()
			state.active--
			r.notifyCacheActivityLocked(state)
			r.mu.Unlock()
			level.Debug(r.logger).Log("msg", "dropping dequeued job for closed cache", "cache_id", cacheID, "job_kind", job.kind.String())
			continue
		}

		ctx, cancel := context.WithTimeout(state.ctx, r.timeout)
		level.Debug(r.logger).Log("msg", "starting background job", "cache_id", cacheID, "job_kind", job.kind.String())
		func() {
			defer func() {
				cancel()
				if rec := recover(); rec != nil {
					level.Error(r.logger).Log("msg", "background job panicked", "cache_id", cacheID, "job_kind", job.kind.String(), "panic", rec)
				} else {
					level.Debug(r.logger).Log("msg", "finished background job", "cache_id", cacheID, "job_kind", job.kind.String())
				}
				r.mu.Lock()
				state.active--
				r.notifyCacheActivityLocked(state)
				r.mu.Unlock()
			}()
			job.job(ctx)
		}()
	}
}

func (r *Group) nextJob() (string, queuedJob, *groupCacheState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		if r.closing && r.allQueuesEmptyLocked() {
			return "", queuedJob{}, nil, false
		}

		cacheID, job, state, ok := r.pickNextLocked()
		if ok {
			return cacheID, job, state, true
		}

		r.cond.Wait()
	}
}

func (r *Group) allQueuesEmptyLocked() bool {
	for _, cacheID := range r.order {
		state := r.caches[cacheID]
		if state != nil && !state.closed && len(state.queue) > 0 {
			return false
		}
	}
	return true
}

func (r *Group) pickNextLocked() (string, queuedJob, *groupCacheState, bool) {
	if len(r.order) == 0 {
		return "", queuedJob{}, nil, false
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
		var job queuedJob
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
	return "", queuedJob{}, nil, false
}

func (r *Group) cacheCloseDoneLocked(state *groupCacheState) chan struct{} {
	if state.closeDone == nil {
		state.closeDone = make(chan struct{})
	}
	if state.closed && state.active == 0 && !state.closeSent {
		close(state.closeDone)
		state.closeSent = true
	}
	return state.closeDone
}

func (r *Group) notifyCacheActivityLocked(state *groupCacheState) {
	if state.closed && state.active == 0 {
		r.cacheCloseDoneLocked(state)
	}
	if state.active == 0 {
		r.cond.Broadcast()
	}
}

func (r *Group) waitForCacheClose(cacheID string, done <-chan struct{}, timeout time.Duration) error {
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		level.Warn(r.logger).Log("msg", "cache close timed out", "cache_id", cacheID, "timeout", timeout)
		return worker.ErrShutdownTimeout
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type ConfigError struct {
	Msg string
}

func (e *ConfigError) Error() string {
	return e.Msg
}
