package daramjwee

import "errors"

// executeLowerTierDecision executes the action determined by decideLowerTierHit.
func (c *DaramjweeCache) executeLowerTierDecision(p lowerTierHitParams, decision lowerTierDecision, metaToPromote *Metadata, isStale bool) (*GetResponse, error) {
	switch decision.action {
	case actionServeConditionalDirect:
		return c.serveLowerTierWithoutPromotion(p, isStale)
	case actionServeNotFoundDirect:
		return c.serveLowerTierWithoutPromotion(p, isStale)
	case actionServeBodyDirect:
		return c.serveLowerTierWithoutPromotion(p, isStale)

	case actionServeConditionalWithPromotion:
		return c.handleConditionalLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.fetcher, p.src, p.meta, metaToPromote, p.cancel, isStale, p.expectedGeneration)

	case actionServeNegativeNotFoundStale:
		return c.handleStaleLowerTierHit(p.requestCtx, p.key, p.tierIndex, p.fetcher, p.src, p.meta, p.cancel, p.expectedGeneration)

	case actionServeNegativeNotFoundPromote:
		return c.promoteNegativeLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.src, p.meta, metaToPromote, p.cancel, p.expectedGeneration)

	case actionServeStaleBodyRefresh:
		return c.handleStaleLowerTierHit(p.requestCtx, p.key, p.tierIndex, p.fetcher, p.src, p.meta, p.cancel, p.expectedGeneration)

	case actionPromotePositive:
		return c.promotePositiveLowerTierHit(p.requestCtx, p.setupCtx, p.key, p.tierIndex, p.src, p.meta, metaToPromote, p.cancel, p.expectedGeneration), nil

	default:
		p.cancel()
		return nil, errors.New("daramjwee: unknown lower tier action")
	}
}
