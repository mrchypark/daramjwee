package daramjwee

import (
	"os"
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
	if c.loggingDisabled || c.logger == nil {
		return
	}
	_ = level.Debug(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) infoLog(keyvals ...any) {
	if c.loggingDisabled || c.logger == nil {
		return
	}
	_ = level.Info(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) warnLog(keyvals ...any) {
	if c.loggingDisabled || c.logger == nil {
		return
	}
	_ = level.Warn(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) errorLog(keyvals ...any) {
	if c.loggingDisabled || c.logger == nil {
		return
	}
	_ = level.Error(c.logger).Log(keyvals...)
}

func (c *DaramjweeCache) diagnosticLog(event, key string, generation uint64, keyvals ...any) {
	if c.loggingDisabled || c.logger == nil || !cacheDiagnosticsEnabled() {
		return
	}
	diagnostic := []any{
		"msg", "cache diagnostic",
		"event", event,
		"key", key,
		"generation", generation,
	}
	diagnostic = append(diagnostic, keyvals...)
	c.debugLog(diagnostic...)
}

func cacheDiagnosticsEnabled() bool {
	return os.Getenv("DJ_CACHE_DIAGNOSTICS") == "1" || os.Getenv("DJ_REPRO_CACHE_STUCK") == "1"
}
