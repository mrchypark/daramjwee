package daramjwee

// lowerTierAction represents the possible actions for a lower-tier hit.
type lowerTierAction int

const (
	// Serve without promotion (higher tiers dirty)
	actionServeConditionalDirect lowerTierAction = iota
	actionServeNotFoundDirect
	actionServeBodyDirect

	// Serve conditional without promotion
	actionServeConditionalWithPromotion

	// Serve negative without promotion
	actionServeNegativeNotFoundStale
	actionServeNegativeNotFoundPromote

	// Serve stale with refresh
	actionServeStaleNotFoundRefresh
	actionServeStaleBodyRefresh

	// Promote positive
	actionPromotePositive
)

// lowerTierDecision holds the result of deciding how to handle a lower-tier hit.
type lowerTierDecision struct {
	action lowerTierAction
	isStale bool
}

// decideLowerTierHit determines the action to take for a lower-tier cache hit.
func (c *DaramjweeCache) decideLowerTierHit(p lowerTierHitParams, meta *Metadata, isStale bool) lowerTierDecision {
	if !p.higherTiersClean {
		return c.decideDirectServe(p, meta, isStale)
	}
	if c.isConditionalRequestSatisfied(p.req, meta) {
		if c.canServeConditionalLowerHit(p.key, p.expectedGeneration) {
			return lowerTierDecision{action: actionServeConditionalWithPromotion, isStale: isStale}
		}
		return lowerTierDecision{action: actionServeBodyDirect, isStale: isStale}
	}
	if meta.IsNegative {
		if isStale {
			return lowerTierDecision{action: actionServeNegativeNotFoundStale, isStale: true}
		}
		return lowerTierDecision{action: actionServeNegativeNotFoundPromote, isStale: false}
	}
	if isStale {
		return lowerTierDecision{action: actionServeStaleBodyRefresh, isStale: true}
	}
	return lowerTierDecision{action: actionPromotePositive, isStale: false}
}

// decideDirectServe determines the action when higher tiers are dirty.
func (c *DaramjweeCache) decideDirectServe(p lowerTierHitParams, meta *Metadata, isStale bool) lowerTierDecision {
	if c.isConditionalRequestSatisfied(p.req, meta) {
		return lowerTierDecision{action: actionServeConditionalDirect, isStale: isStale}
	}
	if meta.IsNegative {
		return lowerTierDecision{action: actionServeNotFoundDirect, isStale: isStale}
	}
	if isStale {
		return lowerTierDecision{action: actionServeStaleBodyRefresh, isStale: isStale}
	}
	return lowerTierDecision{action: actionServeBodyDirect, isStale: false}
}

