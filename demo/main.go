package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stephennancekivell/querypulse"
	"github.com/stephennancekivell/querypulse/qslog"
)

func main() {
	ctx := context.Background()

	// Register the driver wrapping the postgres driver.
	driverName, err := querypulse.Register(
		"postgres",
		querypulse.Options{
			OnSuccess: func(query string, args []any, duration time.Duration) {
				fmt.Printf("OnSuccess: %v %v %v\n", query, args, duration)
			},
		})
	if err != nil {
		panic(err)
	}

	// Connect to database
	connStr := "postgresql://test:test@localhost/test?sslmode=disable"
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		panic(err)
	}

	// execute queries just an you normally would with the *sql.DB interface
	rows, err := db.Query("select $1", 100)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Also works with sqlx.
	dbx := sqlx.NewDb(db, "postgres") // sqlx requires the original postgres driver name.

	_, err = dbx.QueryContext(ctx, "select $1", 200)
	if err != nil {
		panic(err)
	}

	// works with sqlx's named queries.
	_, err = dbx.NamedQueryContext(ctx, "select :value", map[string]any{"value": 300})
	if err != nil {
		panic(err)
	}

	// Create a logger using slog.
	jsonlog := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slogDriver, err := qslog.Register("postgres", jsonlog)
	if err != nil {
		panic(err)
	}
	slogDb, err := sql.Open(slogDriver, connStr)
	if err != nil {
		panic(err)
	}

	rows, err = slogDb.Query("select $1", 300)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Create a logger that only logs slow queries.
	slowDriver, err := querypulse.Register(
		"postgres",
		querypulse.Options{
			OnSuccess: func(query string, args []any, duration time.Duration) {
				if duration > 10*time.Second {
					slog.Info("slow query", "query", query, "args", args, "took_ms", duration)
				}
			},
		})
	if err != nil {
		panic(err)
	}
	slowDb, err := sql.Open(slowDriver, connStr)
	if err != nil {
		panic(err)
	}

	rows, err = slowDb.Query("select $1", 400)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

}
