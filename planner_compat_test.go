package daramjwee_test

import daramjwee "github.com/mrchypark/daramjwee"

// Keep v0.15 unkeyed Observation literals source-compatible.
var _ = daramjwee.Observation{
	daramjwee.SourceLower,
	1,
	false,
	daramjwee.FreshnessFresh,
	false,
	daramjwee.UpperTiersClean,
	daramjwee.AdmissionAllowed,
	true,
}
