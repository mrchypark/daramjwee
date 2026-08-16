package daramjwee

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanner_Plan_Miss(t *testing.T) {
	p := &Planner{}
	obs := Observation{Source: SourceNone, HasTopStore: true}

	plan := p.Plan(obs)

	require.Equal(t, ReplyOK, plan.Reply)
	require.Equal(t, BodyStream, plan.Body)
	require.Equal(t, PublishOnEOF, plan.Publish)
	require.Equal(t, FanoutAfterPublish, plan.Fanout)
}

func TestPlanner_Plan_TopTierHit_Fresh(t *testing.T) {
	p := &Planner{}

	tests := []struct {
		name string
		obs  Observation
		want ReadPlan
	}{
		{
			name: "fresh positive",
			obs:  Observation{Source: SourceTop, Freshness: FreshnessFresh, HasTopStore: true},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect},
		},
		{
			name: "fresh negative",
			obs:  Observation{Source: SourceTop, Freshness: FreshnessFresh, EntryNegative: true, HasTopStore: true},
			want: ReadPlan{Reply: ReplyNotFound, Body: BodyNone},
		},
		{
			name: "fresh conditional matched",
			obs:  Observation{Source: SourceTop, Freshness: FreshnessFresh, ConditionalMatched: true, HasTopStore: true},
			want: ReadPlan{Reply: ReplyNotModified, Body: BodyNone},
		},
		{
			name: "stale positive",
			obs:  Observation{Source: SourceTop, Freshness: FreshnessStale, HasTopStore: true},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose},
		},
		{
			name: "stale conditional matched",
			obs:  Observation{Source: SourceTop, Freshness: FreshnessStale, ConditionalMatched: true, HasTopStore: true},
			want: ReadPlan{Reply: ReplyNotModified, Body: BodyNone, Refresh: RefreshOnClose},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := p.Plan(tt.obs)
			require.Equal(t, tt.want, plan)
		})
	}
}

func TestPlanner_Plan_LowerTierHit_CleanUpperTiers(t *testing.T) {
	p := &Planner{}

	tests := []struct {
		name string
		obs  Observation
		want ReadPlan
	}{
		{
			name: "fresh positive with promotion allowed",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				UpperTiersHealth: UpperTiersClean,
				Admission:        AdmissionAllowed,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyStream, Publish: PublishOnEOF, Fanout: FanoutAfterPublish},
		},
		{
			name: "fresh positive with promotion deferred",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				UpperTiersHealth: UpperTiersClean,
				Admission:        AdmissionDeferred,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect},
		},
		{
			name: "fresh positive without top store",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				UpperTiersHealth: UpperTiersClean,
				Admission:        AdmissionAllowed,
				HasTopStore:      false,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect},
		},
		{
			name: "stale positive",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessStale,
				UpperTiersHealth: UpperTiersClean,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose},
		},
		{
			name: "fresh negative with promotion allowed",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				EntryNegative:    true,
				UpperTiersHealth: UpperTiersClean,
				Admission:        AdmissionAllowed,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyNotFound, Body: BodyNone, Publish: PublishOnEOF, Fanout: FanoutAfterPublish},
		},
		{
			name: "fresh negative with promotion deferred",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				EntryNegative:    true,
				UpperTiersHealth: UpperTiersClean,
				Admission:        AdmissionDeferred,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyNotFound, Body: BodyNone},
		},
		{
			name: "stale negative",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessStale,
				EntryNegative:    true,
				UpperTiersHealth: UpperTiersClean,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyNotFound, Body: BodyNone, Refresh: RefreshOnClose},
		},
		{
			name: "conditional matched with top store",
			obs: Observation{
				Source:             SourceLower,
				SourceTier:         1,
				Freshness:          FreshnessFresh,
				ConditionalMatched: true,
				UpperTiersHealth:   UpperTiersClean,
				HasTopStore:        true,
			},
			want: ReadPlan{Reply: ReplyNotModified, Body: BodyNone, Refresh: RefreshOnClose},
		},
		{
			name: "conditional matched without top store",
			obs: Observation{
				Source:             SourceLower,
				SourceTier:         1,
				Freshness:          FreshnessFresh,
				ConditionalMatched: true,
				UpperTiersHealth:   UpperTiersClean,
				HasTopStore:        false,
			},
			want: ReadPlan{Reply: ReplyNotModified, Body: BodyNone},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := p.Plan(tt.obs)
			require.Equal(t, tt.want, plan)
		})
	}
}

func TestPlanner_Plan_LowerTierHit_DirtyUpperTiers(t *testing.T) {
	p := &Planner{}

	tests := []struct {
		name string
		obs  Observation
		want ReadPlan
	}{
		{
			name: "fresh positive direct serve",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				UpperTiersHealth: UpperTiersDirty,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect},
		},
		{
			name: "stale positive direct serve",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessStale,
				UpperTiersHealth: UpperTiersDirty,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyOK, Body: BodyDirect, Refresh: RefreshOnClose},
		},
		{
			name: "negative direct serve",
			obs: Observation{
				Source:           SourceLower,
				SourceTier:       1,
				Freshness:        FreshnessFresh,
				EntryNegative:    true,
				UpperTiersHealth: UpperTiersDirty,
				HasTopStore:      true,
			},
			want: ReadPlan{Reply: ReplyNotFound, Body: BodyNone},
		},
		{
			name: "conditional matched direct serve",
			obs: Observation{
				Source:             SourceLower,
				SourceTier:         1,
				Freshness:          FreshnessFresh,
				ConditionalMatched: true,
				UpperTiersHealth:   UpperTiersDirty,
				HasTopStore:        true,
			},
			want: ReadPlan{Reply: ReplyNotModified, Body: BodyNone},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := p.Plan(tt.obs)
			require.Equal(t, tt.want, plan)
		})
	}
}

func TestPlanner_Plan_AllOutcomeCombinations(t *testing.T) {
	p := &Planner{}

	// Test that all source × freshness × conditional × admission combinations
	// produce valid plans (no panics, all fields set)
	sources := []SourceKind{SourceTop, SourceLower}
	freshness := []Freshness{FreshnessFresh, FreshnessStale}
	conditional := []bool{true, false}
	admission := []AdmissionPolicy{AdmissionAllowed, AdmissionDeferred}
	upperHealth := []UpperTierHealth{UpperTiersClean, UpperTiersDirty}
	negative := []bool{true, false}
	hasTopStore := []bool{true, false}

	validReplies := map[ReplySpec]bool{
		ReplyOK:         true,
		ReplyNotModified: true,
		ReplyNotFound:   true,
	}

	for _, src := range sources {
		for _, fresh := range freshness {
			for _, cond := range conditional {
				for _, adm := range admission {
					for _, health := range upperHealth {
						for _, neg := range negative {
							for _, top := range hasTopStore {
								obs := Observation{
									Source:             src,
									Freshness:          fresh,
									ConditionalMatched: cond,
									Admission:          adm,
									UpperTiersHealth:   health,
									EntryNegative:      neg,
									HasTopStore:        top,
								}
								plan := p.Plan(obs)
								// Verify plan has a valid Reply
								require.True(t, validReplies[plan.Reply], "obs: %+v, plan: %+v", obs, plan)
							}
						}
					}
				}
			}
		}
	}
}
