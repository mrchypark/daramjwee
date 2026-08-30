package daramjwee

import "errors"

func (c *DaramjweeCache) executeLowerTierPlan(p lowerTierHitParams, plan ReadPlan, metaToPromote *Metadata) (*GetResponse, error) {
	switch plan {
	case ReadPlan{Reply: ReplyNotModified}, ReadPlan{Reply: ReplyNotModified, Refresh: RefreshOnClose}:
		return c.handleConditionalLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.fetcher, p.src, p.meta, metaToPromote, p.cancel, plan.Refresh == RefreshOnClose, p.expectedGeneration)
	case ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose}, ReadPlan{Reply: ReplyNotFound, Refresh: RefreshOnClose}:
		return c.handleStaleLowerTierHit(p.requestCtx, p.key, p.tierIndex, p.fetcher, p.src, p.meta, p.cancel, p.expectedGeneration)
	case ReadPlan{Reply: ReplyNotFound, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}:
		return c.promoteNegativeLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.src, p.meta, metaToPromote, p.cancel, p.expectedGeneration)
	case ReadPlan{Reply: ReplyOK, Body: BodyStream, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}:
		return c.promotePositiveLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.src, p.meta, metaToPromote, p.cancel, p.expectedGeneration), nil
	case ReadPlan{Reply: ReplyOK, Body: BodyDirect}, ReadPlan{Reply: ReplyNotFound}:
		return c.serveLowerTierWithoutPromotion(p, false)
	default:
		p.cancel()
		return nil, errors.New("daramjwee: unknown lower tier plan")
	}
}
