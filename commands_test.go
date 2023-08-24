package querypulse

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var ctx context.Context = context.Background()

func TestDb(t *testing.T) {

	testCases := []struct {
		name string
		run  func(db *sql.DB, query string, args []any) error
	}{
		{
			name: "Exec",
			run: func(db *sql.DB, query string, args []any) error {
				_, err := db.Exec(query, args...)
				return err
			},
		},
		{
			name: "ExecContext",
			run: func(db *sql.DB, query string, args []any) error {
				_, err := db.ExecContext(ctx, query, args...)
				return err
			},
		},
		{
			name: "Query",
			run: func(db *sql.DB, query string, args []any) error {
				_, err := db.Query(query, args...)
				return err
			},
		},
		{
			name: "QueryContext",
			run: func(db *sql.DB, query string, args []any) error {
				_, err := db.QueryContext(ctx, query, args...)
				return err
			},
		},
		{
			name: "QueryRow",
			run: func(db *sql.DB, query string, args []any) error {
				rows := db.QueryRow(query, args...)
				return rows.Err()
			},
		},
		{
			name: "QueryRowContext",
			run: func(db *sql.DB, query string, args []any) error {
				rows := db.QueryRowContext(ctx, query, args...)
				return rows.Err()
			},
		},
	}

	for _, v := range testCases {
		t.Run(v.name, func(t *testing.T) {
			var meta queryMeta

			db, err := getDB(func(meta_ queryMeta) { meta = meta_ })
			assert.NoError(t, err)

			err = v.run(db, "select $1", []any{1})
			assert.NoError(t, err)

			assertQuery(t, meta, "select $1", []any{int64(1)}, 1*time.Microsecond, 30*time.Microsecond)
		})
	}
}

func TestTx(t *testing.T) {

	testCases := []struct {
		name string
		run  func(db *sql.Tx, query string, args []any) error
	}{
		{
			name: "Exec",
			run: func(db *sql.Tx, query string, args []any) error {
				_, err := db.Exec(query, args...)
				return err
			},
		},

		{
			name: "ExecContext",
			run: func(db *sql.Tx, query string, args []any) error {
				_, err := db.ExecContext(ctx, query, args...)
				return err
			},
		},
		{
			name: "Query",
			run: func(db *sql.Tx, query string, args []any) error {
				_, err := db.Query(query, args...)
				return err
			},
		},
		{
			name: "QueryContext",
			run: func(db *sql.Tx, query string, args []any) error {
				_, err := db.QueryContext(ctx, query, args...)
				return err
			},
		},
		{
			name: "QueryRow",
			run: func(db *sql.Tx, query string, args []any) error {
				rows := db.QueryRow(query, args...)
				return rows.Err()
			},
		},
		{
			name: "QueryRowContext",
			run: func(db *sql.Tx, query string, args []any) error {
				rows := db.QueryRowContext(ctx, query, args...)
				return rows.Err()
			},
		},
	}

	for _, v := range testCases {
		t.Run(v.name, func(t *testing.T) {
			var meta queryMeta

			db, err := getDB(func(meta_ queryMeta) { meta = meta_ })
			assert.NoError(t, err)

			tx, err := db.BeginTx(ctx, nil)
			assert.NoError(t, err)
			defer tx.Rollback()

			err = v.run(tx, "select $1", []any{1})
			assert.NoError(t, err)
			err = tx.Commit()
			assert.NoError(t, err)

			assertQuery(t, meta, "select $1", []any{int64(1)}, 1*time.Microsecond, 30*time.Microsecond)
		})
	}
}

func TestPrepare(t *testing.T) {

	testCases := []struct {
		name string
		run  func(db *sql.Stmt, args []any) error
	}{
		{
			name: "Exec",
			run: func(db *sql.Stmt, args []any) error {
				_, err := db.Exec(args...)
				return err
			},
		},

		{
			name: "ExecContext",
			run: func(db *sql.Stmt, args []any) error {
				_, err := db.ExecContext(ctx, args...)
				return err
			},
		},
		{
			name: "Query",
			run: func(db *sql.Stmt, args []any) error {
				_, err := db.Query(args...)
				return err
			},
		},
		{
			name: "QueryContext",
			run: func(db *sql.Stmt, args []any) error {
				_, err := db.QueryContext(ctx, args...)
				return err
			},
		},
		{
			name: "QueryRow",
			run: func(db *sql.Stmt, args []any) error {
				rows := db.QueryRow(args...)
				return rows.Err()
			},
		},
		{
			name: "QueryRowContext",
			run: func(db *sql.Stmt, args []any) error {
				rows := db.QueryRowContext(ctx, args...)
				return rows.Err()
			},
		},
	}

	for _, v := range testCases {
		t.Run(v.name, func(t *testing.T) {
			var meta queryMeta
			db, err := getDB(func(meta_ queryMeta) { meta = meta_ })
			assert.NoError(t, err)

			stmt, err := db.Prepare("select $1")
			assert.NoError(t, err)

			err = v.run(stmt, []any{1})
			assert.NoError(t, err)

			assertQuery(t, meta, "select $1", []any{int64(1)}, 3*time.Nanosecond, 30*time.Microsecond)
		})

		t.Run("Prepare Context: "+v.name, func(t *testing.T) {
			var meta queryMeta
			db, err := getDB(func(meta_ queryMeta) { meta = meta_ })
			assert.NoError(t, err)

			stmt, err := db.PrepareContext(ctx, "select $1")
			assert.NoError(t, err)

			err = v.run(stmt, []any{1})
			assert.NoError(t, err)

			assertQuery(t, meta, "select $1", []any{int64(1)}, 3*time.Nanosecond, 30*time.Microsecond)
		})
	}
}

func TestNoCallback(t *testing.T) {
	driverName, err := Register("sqlite3", Options{})
	assert.NoError(t, err)

	db, err := sql.Open(driverName, "file::memory:?cache=shared")
	assert.NoError(t, err)

	db.SetMaxOpenConns(1)

	_, err = db.Exec("select 1")
	assert.NoError(t, err)
}

func assertQuery(t *testing.T, meta queryMeta, expectedQuery string, expectedArgs []any, minDuration time.Duration, maxDuration time.Duration) {

	assert.Equal(t, expectedQuery, meta.query, "query doesnt match")
	assert.Equal(t, expectedArgs, meta.args, "args dont match")
	assertDurationWithin(t, minDuration, maxDuration, meta.duration)
}

func assertDurationWithin(t *testing.T, min time.Duration, max time.Duration, actual time.Duration) {
	assert.True(t, actual > min, fmt.Sprintf("Expected %v to be greater than %v", actual, min))
	assert.True(t, actual < max, fmt.Sprintf("Expected %v to be less than %v", actual, max))
}

type queryMeta struct {
	query    string
	args     []any
	duration time.Duration
}

func getDB(onSuccess func(meta queryMeta)) (*sql.DB, error) {

	fn := func(query string, args []any, duration time.Duration) {
		onSuccess(
			queryMeta{
				query:    query,
				args:     args,
				duration: duration,
			},
		)
	}

	driverName, err := Register("sqlite3", Options{OnSuccess: fn})
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, err
}
