package daramjwee

import (
	"github.com/mrchypark/daramjwee/internal/runtime"
)

type backgroundRuntime = runtime.Runtime

type Job = runtime.Job
type DropReason = runtime.DropReason

const (
	DropReasonRejected = runtime.DropReasonRejected
	DropReasonShutdown = runtime.DropReasonShutdown
)

type JobKind = runtime.JobKind

const (
	JobKindRefresh = runtime.JobKindRefresh
	JobKindPersist = runtime.JobKindPersist
)

type CacheRuntimeConfig = runtime.Config

type cacheConstructionMode int

const (
	cacheConstructionStandalone cacheConstructionMode = iota
	cacheConstructionGroup
)
