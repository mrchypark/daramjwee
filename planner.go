package daramjwee

// SourceKind describes where the cache entry was found.
type SourceKind int

const (
	// SourceNone indicates no cache entry was found (full miss).
	SourceNone SourceKind = iota
	// SourceTop indicates the entry was found in tier 0 (top/hot tier).
	SourceTop
	// SourceLower indicates the entry was found in a tier below tier 0.
	SourceLower
)

// Freshness describes whether a cache entry is fresh or stale.
type Freshness int

const (
	// FreshnessFresh indicates the entry is within its freshness window.
	FreshnessFresh Freshness = iota
	// FreshnessStale indicates the entry is beyond its freshness window.
	FreshnessStale
)

// UpperTierHealth describes the health of tiers above the current one.
type UpperTierHealth int

const (
	// UpperTiersClean indicates all upper tiers responded without errors.
	UpperTiersClean UpperTierHealth = iota
	// UpperTiersDirty indicates at least one upper tier had an error.
	UpperTiersDirty
)

// AdmissionPolicy controls whether a lower-tier entry can be promoted.
type AdmissionPolicy int

const (
	// AdmissionAllowed indicates the entry can be promoted to top tier.
	AdmissionAllowed AdmissionPolicy = iota
	// AdmissionDeferred indicates the entry should not be promoted yet (probation).
	AdmissionDeferred
)

// Observation is a pure fact about what was discovered during a cache lookup.
// It contains no I/O objects or mutable state - just facts.
type Observation struct {
	// Source indicates where the entry was found.
	Source SourceKind

	// SourceTier is the tier index where the entry was found.
	// Only meaningful when Source is SourceTop or SourceLower.
	SourceTier int

	// EntryNegative indicates the cached entry is a negative cache marker.
	EntryNegative bool

	// Freshness indicates whether the entry is fresh or stale.
	Freshness Freshness

	// ConditionalMatched indicates the request's If-None-Match matched.
	ConditionalMatched bool

	// UpperTiersHealth indicates whether tiers above SourceTier are clean.
	UpperTiersHealth UpperTierHealth

	// Admission indicates whether promotion is allowed.
	Admission AdmissionPolicy

	// HasTopStore indicates whether a writable top tier exists.
	HasTopStore bool
}

// ReplySpec describes what response to send to the caller.
type ReplySpec int

const (
	// ReplyOK indicates a 200 OK response with a body.
	ReplyOK ReplySpec = iota
	// ReplyNotModified indicates a 304 Not Modified response.
	ReplyNotModified
	// ReplyNotFound indicates a 404 Not Found response.
	ReplyNotFound
)

// BodySpec describes how to handle the response body.
type BodySpec int

const (
	// BodyNone indicates no body (e.g., 304 or 404 response).
	BodyNone BodySpec = iota
	// BodyDirect indicates the body comes directly from the source.
	BodyDirect
	// BodyStream indicates the body should be streamed through a fill sink.
	BodyStream
)

// PublishSpec describes whether and how to publish to top tier.
type PublishSpec int

const (
	// PublishNone indicates no publish to top tier.
	PublishNone PublishSpec = iota
	// PublishOnEOF indicates the entry should be published after EOF.
	PublishOnEOF
)

// RefreshSpec describes whether to schedule a background refresh.
type RefreshSpec int

const (
	// RefreshNone indicates no background refresh.
	RefreshNone RefreshSpec = iota
	// RefreshOnClose indicates a refresh should be scheduled when the body closes.
	RefreshOnClose
)

// FanoutSpec describes whether to fanout to lower tiers after publish.
type FanoutSpec int

const (
	// FanoutNone indicates no fanout.
	FanoutNone FanoutSpec = iota
	// FanoutAfterPublish indicates fanout should happen after successful publish.
	FanoutAfterPublish
)

// ReadPlan is a pure description of what to do with a cache hit or miss.
// It is produced by the Planner and consumed by the Executor.
type ReadPlan struct {
	// Reply specifies the response status code.
	Reply ReplySpec

	// Body specifies how to handle the response body.
	Body BodySpec

	// Publish specifies whether to publish to top tier.
	Publish PublishSpec

	// Refresh specifies whether to schedule a background refresh.
	Refresh RefreshSpec

	// Fanout specifies whether to fanout to lower tiers.
	Fanout FanoutSpec
}

// Planner is a pure function that converts observations into read plans.
// It contains no state and performs no I/O.
type Planner struct{}

// Plan computes a ReadPlan from an Observation and request facts.
func (p *Planner) Plan(obs Observation) ReadPlan {
	switch obs.Source {
	case SourceNone:
		return p.planMiss(obs)
	case SourceTop:
		return p.planTopTierHit(obs)
	case SourceLower:
		return p.planLowerTierHit(obs)
	default:
		return ReadPlan{Reply: ReplyNotFound, Body: BodyNone}
	}
}

func (p *Planner) planMiss(_ Observation) ReadPlan {
	// Full miss: fetch from origin, stream through fill sink, publish on EOF.
	return ReadPlan{
		Reply:   ReplyOK,
		Body:    BodyStream,
		Publish: PublishOnEOF,
		Fanout:  FanoutAfterPublish,
	}
}

func (p *Planner) planTopTierHit(obs Observation) ReadPlan {
	// Conditional request satisfied
	if obs.ConditionalMatched {
		if obs.Freshness == FreshnessStale {
			return ReadPlan{
				Reply:   ReplyNotModified,
				Body:    BodyNone,
				Refresh: RefreshOnClose,
			}
		}
		return ReadPlan{
			Reply: ReplyNotModified,
			Body:  BodyNone,
		}
	}

	// Negative entry
	if obs.EntryNegative {
		return ReadPlan{
			Reply: ReplyNotFound,
			Body:  BodyNone,
		}
	}

	// Positive entry - serve directly from source
	if obs.Freshness == FreshnessStale {
		return ReadPlan{
			Reply:   ReplyOK,
			Body:    BodyDirect,
			Refresh: RefreshOnClose,
		}
	}
	return ReadPlan{
		Reply: ReplyOK,
		Body:  BodyDirect,
	}
}

func (p *Planner) planLowerTierHit(obs Observation) ReadPlan {
	// Higher tiers dirty - serve directly without promotion
	if obs.UpperTiersHealth == UpperTiersDirty {
		return p.planDirectServe(obs)
	}

	// Conditional request satisfied with clean upper tiers
	if obs.ConditionalMatched {
		if obs.HasTopStore {
			return ReadPlan{
				Reply:   ReplyNotModified,
				Body:    BodyNone,
				Refresh: RefreshOnClose,
			}
		}
		return ReadPlan{
			Reply: ReplyNotModified,
			Body:  BodyNone,
		}
	}

	// Negative entry
	if obs.EntryNegative {
		if obs.Freshness == FreshnessStale {
			return ReadPlan{
				Reply:   ReplyNotFound,
				Body:    BodyNone,
				Refresh: RefreshOnClose,
			}
		}
		if obs.HasTopStore && obs.Admission == AdmissionAllowed {
			return ReadPlan{
				Reply:   ReplyNotFound,
				Body:    BodyNone,
				Publish: PublishOnEOF,
				Fanout:  FanoutAfterPublish,
			}
		}
		return ReadPlan{
			Reply: ReplyNotFound,
			Body:  BodyNone,
		}
	}

	// Stale entry
	if obs.Freshness == FreshnessStale {
		return ReadPlan{
			Reply:   ReplyOK,
			Body:    BodyDirect,
			Refresh: RefreshOnClose,
		}
	}

	// Fresh positive entry - promote to top tier
	if obs.HasTopStore && obs.Admission == AdmissionAllowed {
		return ReadPlan{
			Reply:   ReplyOK,
			Body:    BodyStream,
			Publish: PublishOnEOF,
			Fanout:  FanoutAfterPublish,
		}
	}
	return ReadPlan{
		Reply: ReplyOK,
		Body:  BodyDirect,
	}
}

func (p *Planner) planDirectServe(obs Observation) ReadPlan {
	if obs.ConditionalMatched {
		return ReadPlan{
			Reply: ReplyNotModified,
			Body:  BodyNone,
		}
	}
	if obs.EntryNegative {
		return ReadPlan{
			Reply: ReplyNotFound,
			Body:  BodyNone,
		}
	}
	if obs.Freshness == FreshnessStale {
		return ReadPlan{
			Reply:   ReplyOK,
			Body:    BodyDirect,
			Refresh: RefreshOnClose,
		}
	}
	return ReadPlan{
		Reply: ReplyOK,
		Body:  BodyDirect,
	}
}
