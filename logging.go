package daramjwee

import (
	"reflect"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func isNoopLogger(logger log.Logger) bool {
	if logger == nil {
		return true
	}
	typ := reflect.TypeOf(logger)
	return typ != nil && typ.PkgPath() == "github.com/go-kit/log" && typ.Name() == "nopLogger"
}

func (c *DaramjweeCache) debugLog(keyvals ...any) {
	if c.loggingDisabled {
		return
	}
	_ = level.Debug(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) infoLog(keyvals ...any) {
	if c.loggingDisabled {
		return
	}
	_ = level.Info(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) warnLog(keyvals ...any) {
	if c.loggingDisabled {
		return
	}
	_ = level.Warn(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) errorLog(keyvals ...any) {
	if c.loggingDisabled {
		return
	}
	_ = level.Error(c.logger).Log(keyvals...)
}
