package daramjwee

import (
	"context"
	"errors"
	"time"
)

// closeCoreParams holds parameters for the shared close sequence.
type closeCoreParams struct {
	generation    uint64
	coord         *writeCoordinator
	waitTimeout   time.Duration
	onInvalidated func() error
	advanceGen    bool // whether to advance committedGeneration on success
}

// closeCore executes the shared close sequence for generation-coordinated sinks.
// Returns the error to be stored in the sink's err field.
// releaseLease is called on both success and error paths (for write lease).
func closeCore(
	_ context.Context,
	p closeCoreParams,
	commitFn func(ctx context.Context) error,
	abortFn func() error,
	releaseLease func(),
) error {
	defer p.coord.releaseReference()
	if releaseLease != nil {
		defer releaseLease()
	}

	waitCtx, cancelWait := newCoordinatorWaitContext(p.waitTimeout)
	defer cancelWait()

	// Phase 1: Wait for active deletes to complete
	if err := p.coord.waitForNoActiveDeletes(waitCtx); err != nil {
		p.coord.unregisterReservation(p.generation)
		if abortFn != nil {
			if abortErr := abortFn(); abortErr != nil {
				return errors.Join(err, abortErr)
			}
		}
		return err
	}

	// Phase 2: Pre-commit generation check
	p.coord.stateMu.Lock()
	if p.coord.committedGeneration.Load() > p.generation {
		p.coord.removeReservationLocked(p.generation)
		p.coord.stateMu.Unlock()
		if abortFn != nil {
			if abortErr := abortFn(); abortErr != nil {
				return errors.Join(ErrTopWriteInvalidated, abortErr)
			}
		}
		return ErrTopWriteInvalidated
	}
	p.coord.stateMu.Unlock()

	// Phase 3: Commit
	if commitErr := commitFn(waitCtx); commitErr != nil {
		p.coord.stateMu.Lock()
		p.coord.removeReservationLocked(p.generation)
		p.coord.stateMu.Unlock()
		return commitErr
	}

	// Phase 4: Advance generation on success (if applicable)
	if p.advanceGen {
		p.coord.stateMu.Lock()
		if p.coord.committedGeneration.Load() < p.generation {
			p.coord.committedGeneration.Store(p.generation)
		}
		p.coord.pruneReservationsThroughLocked(p.coord.committedGeneration.Load())
		p.coord.stateMu.Unlock()
	}

	// Phase 5: Post-close wait for active deletes
	postCloseWaitCtx, cancelPostCloseWait := newCoordinatorWaitContext(p.waitTimeout)
	defer cancelPostCloseWait()
	_ = p.coord.waitForNoActiveDeletes(postCloseWaitCtx)

	// Phase 6: Final invalidation check
	p.coord.stateMu.Lock()
	if p.coord.committedGeneration.Load() > p.generation {
		p.coord.stateMu.Unlock()
		err := ErrTopWriteInvalidated
		if p.onInvalidated != nil {
			if cleanupErr := p.onInvalidated(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
		return err
	}
	p.coord.stateMu.Unlock()

	return nil
}
