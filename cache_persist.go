package daramjwee

import (
	"context"
	"errors"
)

func (c *DaramjweeCache) persistDestinationsAfterTop() []tierDestination {
	if len(c.tiers) <= 1 {
		return nil
	}

	dests := make([]tierDestination, 0, len(c.tiers)-1)
	for idx, tier := range c.tiers[1:] {
		if hasRealStore(tier) {
			dests = append(dests, tierDestination{tierIndex: idx + 1, store: tier})
		}
	}
	return dests
}

func (c *DaramjweeCache) regularFanoutDestinations(sourceIndex int) []tierDestination {
	if sourceIndex <= 1 {
		return nil
	}

	dests := make([]tierDestination, 0, sourceIndex-1)
	for idx, tier := range c.tiers[1:sourceIndex] {
		if hasRealStore(tier) {
			dests = append(dests, tierDestination{tierIndex: idx + 1, store: tier})
		}
	}
	return dests
}

func (c *DaramjweeCache) schedulePersistFromCurrentTop(ctx context.Context, key string, destinations ...tierDestination) {
	expectedGeneration := c.currentTopWriteGeneration(key)
	defer expectedGeneration.release()
	c.schedulePersistFromTop(ctx, key, expectedGeneration, destinations...)
}

func (c *DaramjweeCache) schedulePersistFromTop(ctx context.Context, key string, expectedGeneration *topWriteGeneration, destinations ...tierDestination) {
	srcStore := c.topWriteStore()
	if !hasRealStore(srcStore) || len(destinations) == 0 {
		return
	}
	if c.runtime == nil {
		c.warnLog("msg", "worker is not configured, cannot schedule persistence", "key", key)
		return
	}

	valueCtx := detachedValueContext(ctx)
	for _, destination := range destinations {
		destStore := destination.store
		if !hasRealStore(destStore) || sameStoreInstance(destStore, srcStore) {
			continue
		}

		destTierIndex := destination.tierIndex
		dest := destStore
		jobGeneration := expectedGeneration.retain()
		job := func(jobCtx context.Context) {
			defer jobGeneration.release()
			persistCtx := overlayContextValues(jobCtx, valueCtx)
			c.infoLog("msg", "starting background set", "key", key, "dest_tier", destTierIndex)
			srcStream, meta, err := c.getStreamFromStore(persistCtx, srcStore, key)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					c.infoLog("msg", "skipping background set because top-tier entry is gone", "key", key, "dest_tier", destTierIndex)
				} else {
					c.errorLog("msg", "failed to get stream from top store for background set", "key", key, "err", err)
				}
				return
			}
			unlockFanout := c.fanoutWrites.lock(destTierIndex, key)
			defer unlockFanout()

			destWriter, err := c.setStreamToStore(persistCtx, dest, key, meta)
			if err != nil {
				if closeErr := srcStream.Close(); closeErr != nil {
					c.errorLog("msg", "failed to close source stream after destination writer failure", "key", key, "dest_tier", destTierIndex, "err", closeErr)
				}
				c.errorLog("msg", "failed to get writer for destination store", "key", key, "dest_tier", destTierIndex, "err", err)
				return
			}
			jobGeneration.coord.retainReference()
			destWriter = newConditionalGenerationWriteSink(destWriter, jobGeneration.coord, jobGeneration.generation, c.config.closeTimeout, func() error {
				cleanupCtx, cancel := c.newCtxWithTimeout(valueCtx)
				defer cancel()
				return c.deleteFromStore(cleanupCtx, dest, key)
			})
			defer destWriter.Abort()

			if persistErr := copyCloseSourceThenCommit(destWriter, srcStream); persistErr != nil {
				if errors.Is(persistErr, ErrTopWriteInvalidated) {
					c.infoLog("msg", "skipping background set because top-tier state changed", "key", key, "dest_tier", destTierIndex)
				} else {
					c.errorLog("msg", "failed background set", "key", key, "dest_tier", destTierIndex, "err", persistErr)
				}
				return
			}
			c.infoLog("msg", "background set successful", "key", key, "dest_tier", destTierIndex)
		}

		if !c.runtime.SubmitWithDropCleanup(c.cacheID, JobKindPersist, job, jobGeneration.release) {
			jobGeneration.release()
			c.warnLog("msg", "background set rejected", "key", key, "dest_tier", destTierIndex)
		}
	}
}
