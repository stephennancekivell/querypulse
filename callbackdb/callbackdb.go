package callbackdb

import (
	"context"
	"database/sql"
)

type callbackdb struct {
	*sql.DB
	onSuccess func()
}

//var _ *sql.DB = (*callbackdb)(nil)

func Make(db *sql.DB, onSuccess func()) *callbackdb {
	return &callbackdb{
		DB:        db,
		onSuccess: onSuccess,
	}
}

func Make2(db *sql.DB, onSuccess func()) *callbackdb {
	return &callbackdb{
		DB:        db,
		onSuccess: onSuccess,
	}
}

func (c *callbackdb) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := c.DB.QueryContext(ctx, query, args...)
	c.onSuccess()
	return rows, err

}

func (c *callbackdb) Query(query string, args ...any) (*sql.Rows, error) {
	rows, err := c.DB.Query(query, args...)
	c.onSuccess()
	return rows, err
}
