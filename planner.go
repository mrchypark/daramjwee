package daramjwee

// SourceKind identifies where a cache lookup found an entry.
type SourceKind int

const (
	SourceNone SourceKind = iota
	SourceTop
	SourceLower
)

// Freshness classifies a cache entry by its freshness window.
type Freshness int

const (
	FreshnessFresh Freshness = iota
	FreshnessStale
)

// UpperTierHealth reports whether every tier above the source was readable.
type UpperTierHealth int

const (
	UpperTiersClean UpperTierHealth = iota
	UpperTiersDirty
)

// AdmissionPolicy controls lower-tier promotion.
type AdmissionPolicy int

const (
	AdmissionAllowed AdmissionPolicy = iota
	AdmissionDeferred
)

// generationValidity reports whether the top-write generation observed before
// a lower-tier read is still valid. It stays private so adding runtime safety
// does not change the public Observation layout.
type generationValidity int

const (
	generationUnspecified generationValidity = iota
	generationValid
	generationInvalid
)

// Observation contains the I/O-free facts used to plan a cache read.
type Observation struct {
	Source             SourceKind
	SourceTier         int
	EntryNegative      bool
	Freshness          Freshness
	ConditionalMatched bool
	UpperTiersHealth   UpperTierHealth
	Admission          AdmissionPolicy
	HasTopStore        bool
}

type ReplySpec int

const (
	ReplyOK ReplySpec = iota
	ReplyNotModified
	ReplyNotFound
)

type BodySpec int

const (
	BodyNone BodySpec = iota
	BodyDirect
	BodyStream
)

type PublishSpec int

const (
	PublishNone PublishSpec = iota
	PublishOnEOF
)

type RefreshSpec int

const (
	RefreshNone RefreshSpec = iota
	RefreshOnClose
)

type FanoutSpec int

const (
	FanoutNone FanoutSpec = iota
	FanoutAfterPublish
)

// ReadPlan describes the response and side effects for a cache read.
type ReadPlan struct {
	Reply   ReplySpec
	Body    BodySpec
	Publish PublishSpec
	Refresh RefreshSpec
	Fanout  FanoutSpec
}

// Planner converts observations into read plans without performing I/O.
type Planner struct{}

func (p *Planner) Plan(obs Observation) ReadPlan {
	return p.plan(obs, generationUnspecified)
}

func (p *Planner) plan(obs Observation, topGeneration generationValidity) ReadPlan {
	switch obs.Source {
	case SourceNone:
		return ReadPlan{Reply: ReplyOK, Body: BodyStream, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}
	case SourceTop:
		return p.planTopTierHit(obs)
	case SourceLower:
		return p.planLowerTierHit(obs, topGeneration)
	default:
		return ReadPlan{Reply: ReplyNotFound}
	}
}

func (*Planner) planTopTierHit(obs Observation) (plan ReadPlan) {
	switch {
	case obs.ConditionalMatched:
		plan.Reply = ReplyNotModified
	case obs.EntryNegative:
		plan.Reply = ReplyNotFound
	default:
		plan.Body = BodyDirect
	}
	if obs.Freshness == FreshnessStale && (obs.ConditionalMatched || !obs.EntryNegative) {
		plan.Refresh = RefreshOnClose
	}
	return plan
}

func (p *Planner) planLowerTierHit(obs Observation, topGeneration generationValidity) (plan ReadPlan) {
	if obs.UpperTiersHealth == UpperTiersDirty {
		if obs.ConditionalMatched && topGeneration == generationUnspecified {
			return ReadPlan{Reply: ReplyNotModified}
		}
		return p.planDirectServe(obs)
	}
	if obs.ConditionalMatched {
		if topGeneration == generationUnspecified {
			plan.Reply = ReplyNotModified
			if obs.HasTopStore {
				plan.Refresh = RefreshOnClose
			}
			return plan
		}
		if topGeneration != generationValid {
			return p.planDirectServe(obs)
		}
		plan.Reply = ReplyNotModified
		if obs.Freshness == FreshnessStale {
			plan.Refresh = RefreshOnClose
		}
		return plan
	}
	if obs.EntryNegative {
		plan.Reply = ReplyNotFound
		if obs.Freshness == FreshnessStale {
			plan.Refresh = RefreshOnClose
		} else if obs.HasTopStore && obs.Admission == AdmissionAllowed {
			plan.Publish, plan.Fanout = PublishOnEOF, FanoutAfterPublish
		}
		return plan
	}

	plan.Reply = ReplyOK
	if obs.Freshness == FreshnessStale {
		plan.Body, plan.Refresh = BodyDirect, RefreshOnClose
	} else if obs.HasTopStore && obs.Admission == AdmissionAllowed {
		plan.Body, plan.Publish, plan.Fanout = BodyStream, PublishOnEOF, FanoutAfterPublish
	} else {
		plan.Body = BodyDirect
	}
	return plan
}

func (*Planner) planDirectServe(obs Observation) (plan ReadPlan) {
	switch {
	case obs.ConditionalMatched:
		plan.Body = BodyDirect
	case obs.EntryNegative:
		plan.Reply = ReplyNotFound
	default:
		plan.Body = BodyDirect
		if obs.Freshness == FreshnessStale {
			plan.Refresh = RefreshOnClose
		}
	}
	return plan
}
