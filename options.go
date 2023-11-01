package querypulse

import (
	"context"
	"database/sql/driver"
	"time"
)

type Options struct {
	OnSuccess func(ctx context.Context, query string, args []any, duration time.Duration)
	OnError   func(ctx context.Context, query string, args []any, duration time.Duration, err error)
}

func (o *Options) onComplete(ctx context.Context, query string, args []driver.Value, duration time.Duration, err error) {
	if err == nil && o.OnSuccess != nil {
		o.OnSuccess(ctx, query, toAnyArgs(args), duration)
	}
	if err != nil && o.OnError != nil {
		o.OnError(ctx, query, toAnyArgs(args), duration, err)
	}
}

func (o *Options) onCompleteNamed(ctx context.Context, query string, args []driver.NamedValue, duration time.Duration, err error) {
	if err == nil && o.OnSuccess != nil {
		o.OnSuccess(ctx, query, argsNamed(args), duration)
	}
	if err != nil && o.OnError != nil {
		o.OnError(ctx, query, argsNamed(args), duration, err)
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
