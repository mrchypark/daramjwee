package daramjwee

import (
	"context"
	"io"
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
	c.schedulePersistFromTop(ctx, key, c.currentTopWriteGeneration(key), destinations...)
}

func (c *DaramjweeCache) schedulePersistFromTop(ctx context.Context, key string, expectedGeneration uint64, destinations ...tierDestination) {
	srcStore := c.topWriteStore()
	if !hasRealStore(srcStore) || len(destinations) == 0 {
		return
	}
	if c.worker == nil {
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
		job := func(jobCtx context.Context) {
			persistCtx := overlayContextValues(jobCtx, valueCtx)
			c.infoLog("msg", "starting background set", "key", key, "dest_tier", destTierIndex)
			srcStream, meta, err := c.getStreamFromStore(persistCtx, srcStore, key)
			if err != nil {
				c.errorLog("msg", "failed to get stream from top store for background set", "key", key, "err", err)
				return
			}
			defer srcStream.Close()

			unlockFanout := c.fanoutWrites.lock(destTierIndex, key)
			defer unlockFanout()

			destWriter, err := c.setStreamToStore(persistCtx, dest, key, meta)
			if err != nil {
				c.errorLog("msg", "failed to get writer for destination store", "key", key, "dest_tier", destTierIndex, "err", err)
				return
			}
			destWriter = newConditionalGenerationWriteSink(destWriter, c.topWrites.coordinator(key), expectedGeneration, func() error {
				cleanupCtx, cancel := c.newCtxWithTimeout(valueCtx)
				defer cancel()
				return c.deleteFromStore(cleanupCtx, dest, key)
			})

			_, copyErr := io.Copy(destWriter, srcStream)
			var closeErr error
			if copyErr != nil {
				closeErr = destWriter.Abort()
			} else {
				closeErr = destWriter.Close()
			}

			if copyErr != nil || closeErr != nil {
				c.errorLog("msg", "failed background set", "key", key, "dest_tier", destTierIndex, "copyErr", copyErr, "closeErr", closeErr)
				return
			}
			c.infoLog("msg", "background set successful", "key", key, "dest_tier", destTierIndex)
		}

		if !c.worker.Submit(job) {
			c.warnLog("msg", "background set rejected", "key", key, "dest_tier", destTierIndex)
		}
	}
}
