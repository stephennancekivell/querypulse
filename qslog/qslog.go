package qslog

import (
	"time"

	"log/slog"

	"github.com/stephennancekivell/querypulse"
)

// Registers a database driver that logs queries with slog.
// Uses the provided logger or the slog's default logger.
// Returns the name of the driver to use.
func Register(driverName string, log_ *slog.Logger) (string, error) {
	log := log_
	if log == nil {
		log = slog.Default()
	}

	fn := func(query string, args []any, duration time.Duration) {
		log.Info("query success", "query", query, "args", args, "took_ms", duration)
	}
	return querypulse.Register(driverName, querypulse.Options{OnSuccess: fn})
}
