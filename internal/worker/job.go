package worker

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func runJobSafely(logger log.Logger, job Job, ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			level.Error(logger).Log("msg", "worker job panicked", "panic", r)
		}
	}()

	job(ctx)
}
