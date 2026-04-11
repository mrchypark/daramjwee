package objectstoredemo

import "testing"

func TestScenarioSummaryVerifyPassesForExpectedTransitions(t *testing.T) {
	summary := ScenarioSummary{
		AfterFirstGet: Observation{
			LocalFiles:    1,
			RemoteObjects: 3,
		},
		AfterTier0Wipe: Observation{
			LocalFiles:    0,
			RemoteObjects: 3,
		},
		AfterPromotion: Observation{
			LocalFiles:    1,
			RemoteObjects: 3,
		},
	}

	if err := summary.Verify(); err != nil {
		t.Fatalf("Verify returned error: %v", err)
	}
}

func TestScenarioSummaryVerifyFailsWhenTier0WasNeverCreated(t *testing.T) {
	summary := ScenarioSummary{
		AfterFirstGet: Observation{
			LocalFiles:    0,
			RemoteObjects: 3,
		},
		AfterTier0Wipe: Observation{
			LocalFiles:    0,
			RemoteObjects: 3,
		},
		AfterPromotion: Observation{
			LocalFiles:    1,
			RemoteObjects: 3,
		},
	}

	if err := summary.Verify(); err == nil {
		t.Fatal("expected Verify to fail when tier 0 was never populated")
	}
}

func TestScenarioSummaryVerifyFailsWhenRemoteObjectsDisappear(t *testing.T) {
	summary := ScenarioSummary{
		AfterFirstGet: Observation{
			LocalFiles:    1,
			RemoteObjects: 3,
		},
		AfterTier0Wipe: Observation{
			LocalFiles:    0,
			RemoteObjects: 0,
		},
		AfterPromotion: Observation{
			LocalFiles:    1,
			RemoteObjects: 0,
		},
	}

	if err := summary.Verify(); err == nil {
		t.Fatal("expected Verify to fail when remote objects disappear")
	}
}

func TestScenarioSummaryVerifyFailsWhenTier0DoesNotRepopulate(t *testing.T) {
	summary := ScenarioSummary{
		AfterFirstGet: Observation{
			LocalFiles:    1,
			RemoteObjects: 3,
		},
		AfterTier0Wipe: Observation{
			LocalFiles:    0,
			RemoteObjects: 3,
		},
		AfterPromotion: Observation{
			LocalFiles:    0,
			RemoteObjects: 3,
		},
	}

	if err := summary.Verify(); err == nil {
		t.Fatal("expected Verify to fail when tier 0 does not repopulate")
	}
}
