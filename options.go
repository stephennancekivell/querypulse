package querypulse

import (
	"context"
	"database/sql/driver"
	"time"
)

type Options struct {
	OnSuccess func(ctx context.Context, query string, args []any, duration time.Duration)
}

func (o *Options) onSuccess(ctx context.Context, query string, args []driver.Value, duration time.Duration) {
	if o.OnSuccess != nil {
		o.OnSuccess(ctx, query, toAnyArgs(args), duration)
	}
}

func (o *Options) onSuccessNamed(ctx context.Context, query string, args []driver.NamedValue, duration time.Duration) {
	if o.OnSuccess != nil {
		o.OnSuccess(ctx, query, argsNamed(args), duration)
	}
}

func toAnyArgs(args []driver.Value) []any {
	out := make([]any, len(args))
	for i, v := range args {
		out[i] = v
	}
	return out
}

func argsNamed(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg.Value
	}
	return out
}
