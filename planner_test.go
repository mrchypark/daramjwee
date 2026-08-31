package daramjwee

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlannerPlans(t *testing.T) {
	top := func(obs Observation) Observation {
		obs.Source, obs.HasTopStore = SourceTop, true
		return obs
	}
	lower := func(obs Observation) Observation {
		obs.Source, obs.SourceTier = SourceLower, 1
		return obs
	}

	tests := []struct {
		name string
		obs  Observation
		want ReadPlan
	}{
		{"miss", Observation{Source: SourceNone, HasTopStore: true}, ReadPlan{Reply: ReplyOK, Body: BodyStream, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}},
		{"top fresh positive", top(Observation{}), ReadPlan{Reply: ReplyOK, Body: BodyDirect}},
		{"top fresh negative", top(Observation{EntryNegative: true}), ReadPlan{Reply: ReplyNotFound}},
		{"top conditional", top(Observation{ConditionalMatched: true}), ReadPlan{Reply: ReplyNotModified}},
		{"top stale positive", top(Observation{Freshness: FreshnessStale}), ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose}},
		{"top stale conditional", top(Observation{Freshness: FreshnessStale, ConditionalMatched: true}), ReadPlan{Reply: ReplyNotModified, Refresh: RefreshOnClose}},
		{"lower promote positive", lower(Observation{HasTopStore: true}), ReadPlan{Reply: ReplyOK, Body: BodyStream, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}},
		{"lower defer positive", lower(Observation{HasTopStore: true, Admission: AdmissionDeferred}), ReadPlan{Reply: ReplyOK, Body: BodyDirect}},
		{"lower positive without top", lower(Observation{}), ReadPlan{Reply: ReplyOK, Body: BodyDirect}},
		{"lower stale positive", lower(Observation{Freshness: FreshnessStale, HasTopStore: true}), ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose}},
		{"lower promote negative", lower(Observation{EntryNegative: true, HasTopStore: true}), ReadPlan{Reply: ReplyNotFound, Publish: PublishOnEOF, Fanout: FanoutAfterPublish}},
		{"lower defer negative", lower(Observation{EntryNegative: true, HasTopStore: true, Admission: AdmissionDeferred}), ReadPlan{Reply: ReplyNotFound}},
		{"lower stale negative", lower(Observation{EntryNegative: true, Freshness: FreshnessStale, HasTopStore: true}), ReadPlan{Reply: ReplyNotFound, Refresh: RefreshOnClose}},
		{"lower conditional legacy default", lower(Observation{ConditionalMatched: true, HasTopStore: true}), ReadPlan{Reply: ReplyNotModified, Refresh: RefreshOnClose}},
		{"lower conditional without top", lower(Observation{ConditionalMatched: true}), ReadPlan{Reply: ReplyNotModified}},
		{"lower stale conditional without top", lower(Observation{ConditionalMatched: true, Freshness: FreshnessStale}), ReadPlan{Reply: ReplyNotModified}},
		{"dirty lower positive", lower(Observation{UpperTiersHealth: UpperTiersDirty, HasTopStore: true}), ReadPlan{Reply: ReplyOK, Body: BodyDirect}},
		{"dirty lower stale", lower(Observation{Freshness: FreshnessStale, UpperTiersHealth: UpperTiersDirty, HasTopStore: true}), ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose}},
		{"dirty lower negative", lower(Observation{EntryNegative: true, UpperTiersHealth: UpperTiersDirty, HasTopStore: true}), ReadPlan{Reply: ReplyNotFound}},
		{"dirty lower conditional", lower(Observation{ConditionalMatched: true, UpperTiersHealth: UpperTiersDirty, HasTopStore: true}), ReadPlan{Reply: ReplyNotModified}},
	}

	planner := &Planner{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, planner.Plan(tt.obs))
		})
	}

	conditional := Observation{Source: SourceLower, ConditionalMatched: true, HasTopStore: true}
	require.Equal(t, ReadPlan{Reply: ReplyNotModified}, planner.plan(conditional, generationValid))
	require.Equal(t, ReadPlan{Reply: ReplyNotModified, Refresh: RefreshOnClose}, planner.plan(Observation{
		Source: SourceLower, ConditionalMatched: true, Freshness: FreshnessStale, HasTopStore: true,
	}, generationValid))
	require.Equal(t, ReadPlan{Reply: ReplyOK, Body: BodyDirect}, planner.plan(conditional, generationInvalid))
	require.Equal(t, ReadPlan{Reply: ReplyOK, Body: BodyDirect}, planner.plan(conditional, generationValidity(99)))
	dirtyConditional := lower(Observation{ConditionalMatched: true, UpperTiersHealth: UpperTiersDirty, HasTopStore: true})
	require.Equal(t, ReadPlan{Reply: ReplyOK, Body: BodyDirect}, planner.plan(dirtyConditional, generationInvalid))
}

func TestPlannerAllOutcomeCombinations(t *testing.T) {
	planner := &Planner{}
	bools := []bool{false, true}
	for _, source := range []SourceKind{SourceTop, SourceLower} {
		for _, freshness := range []Freshness{FreshnessFresh, FreshnessStale} {
			for _, conditional := range bools {
				for _, admission := range []AdmissionPolicy{AdmissionAllowed, AdmissionDeferred} {
					for _, health := range []UpperTierHealth{UpperTiersClean, UpperTiersDirty} {
						for _, negative := range bools {
							for _, hasTop := range bools {
								obs := Observation{Source: source, Freshness: freshness, ConditionalMatched: conditional, Admission: admission, UpperTiersHealth: health, EntryNegative: negative, HasTopStore: hasTop}
								plan := planner.Plan(obs)
								require.Contains(t, []ReplySpec{ReplyOK, ReplyNotModified, ReplyNotFound}, plan.Reply, "obs: %+v, plan: %+v", obs, plan)
							}
						}
					}
				}
			}
		}
	}
}
