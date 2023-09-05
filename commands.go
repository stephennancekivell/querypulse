package querypulse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"
)

type conn interface {
	driver.Pinger
	driver.Execer
	driver.ExecerContext
	driver.Queryer
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
}

var (
	// Type assertions
	_ driver.Driver           = &zDriver{}
	_ conn                    = &zConn{}
	_ driver.Stmt             = &zStmt{}
	_ driver.StmtExecContext  = &zStmt{}
	_ driver.StmtQueryContext = &zStmt{}
)

var (
	regMu sync.Mutex
)

// Register initializes and registers our wrapped database driver
// identified by its driverName and using provided Options. On success it
// returns the generated driverName to use when calling sql.Open.
// It is possible to register multiple wrappers for the same database driver if
// needing different Options for different connections.
func Register(driverName string, options Options) (string, error) {
	// retrieve the driver implementation we need to wrap with instrumentation
	db, err := sql.Open(driverName, "")
	if err != nil {
		return "", err
	}
	dri := db.Driver()
	if err = db.Close(); err != nil {
		return "", err
	}

	regMu.Lock()
	defer regMu.Unlock()
	registerName := fmt.Sprintf("%s-zipkinsql-%d", driverName, len(sql.Drivers()))
	sql.Register(registerName, Wrap(dri, options))

	return registerName, nil
}

// Wrap takes a SQL driver and wraps it.
func Wrap(d driver.Driver, options Options) driver.Driver {
	return wrapDriver(d, options)
}

func (d zDriver) Open(name string) (driver.Conn, error) {
	c, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}
	return wrapConn(c, d.options), nil
}

// WrapConn allows an existing driver.Conn to be wrapped.
func WrapConn(c driver.Conn, options Options) driver.Conn {
	return wrapConn(c, options)
}

// zConn implements driver.Conn
type zConn struct {
	parent driver.Conn

	options Options
}

func (c zConn) Ping(ctx context.Context) error {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c zConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if exec, ok := c.parent.(driver.Execer); ok {
		start := time.Now()
		res, err := exec.Exec(query, args)
		if err != nil {
			return res, err
		}
		c.options.onSuccess(context.Background(), query, args, time.Since(start))
		fmt.Printf("Exec took %v %v %v\n", time.Since(start), query, args)
		return res, err
	}

	return nil, driver.ErrSkip
}

func (c zConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execCtx, ok := c.parent.(driver.ExecerContext); ok {
		start := time.Now()
		res, err := execCtx.ExecContext(ctx, query, args)
		if err != nil {
			return nil, err
		}
		c.options.onSuccessNamed(ctx, query, args, time.Since(start))
		return res, nil
	}

	return nil, driver.ErrSkip
}

func (c zConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		start := time.Now()
		rows, err := queryer.Query(query, args)
		if err != nil {
			return rows, err
		}
		c.options.onSuccess(context.Background(), query, args, time.Since(start))
		return rows, nil
	}

	return nil, driver.ErrSkip
}

func (c zConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryerCtx, ok := c.parent.(driver.QueryerContext); ok {
		start := time.Now()
		rows, err := queryerCtx.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}
		c.options.onSuccessNamed(ctx, query, args, time.Since(start))

		return rows, err
	}

	return nil, driver.ErrSkip
}

func (c zConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}

	return wrapStmt(stmt, query, c.options), nil
}

func (c *zConn) Close() error {
	return c.parent.Close()
}

func (c *zConn) Begin() (driver.Tx, error) {
	return c.parent.Begin()
}

func (c *zConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {

	if prepCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		stmt, err := prepCtx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return wrapStmt(stmt, query, c.options), nil

	} else {
		stmt, err := c.parent.Prepare(query)
		if err != nil {
			return nil, err
		}
		return wrapStmt(stmt, query, c.options), nil
	}
}

func (c *zConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {

	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err := connBeginTx.BeginTx(ctx, opts)

		if err != nil {
			return nil, err
		}
		return zTx{parent: tx, ctx: ctx, options: c.options}, nil
	}

	tx, err := c.parent.Begin()

	if err != nil {
		return nil, err
	}

	return zTx{parent: tx, ctx: ctx, options: c.options}, nil
}

// zStmt implements driver.Stmt
type zStmt struct {
	parent  driver.Stmt
	query   string
	options Options
}

func (s zStmt) Exec(args []driver.Value) (driver.Result, error) {
	res, err := s.parent.Exec(args)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s zStmt) Close() error {
	return s.parent.Close()
}

func (s zStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s zStmt) Query(args []driver.Value) (driver.Rows, error) {

	start := time.Now()
	rows, err := s.parent.Query(args)
	if err != nil {
		return nil, err
	}
	s.options.onSuccess(context.Background(), s.query, args, time.Since(start))

	return rows, err
}

func (s zStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	execContext := s.parent.(driver.StmtExecContext)
	res, err := execContext.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}
	s.options.onSuccessNamed(ctx, s.query, args, time.Since(start))

	return res, nil
}

func (s zStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	start := time.Now()
	// we already tested driver to implement StmtQueryContext
	queryContext := s.parent.(driver.StmtQueryContext)
	rows, err := queryContext.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	s.options.onSuccessNamed(ctx, s.query, args, time.Since(start))

	return rows, err
}

// zTx implemens driver.Tx
type zTx struct {
	parent  driver.Tx
	ctx     context.Context
	options Options
}

func (t zTx) Commit() error {
	return t.parent.Commit()
}

func (t zTx) Rollback() error {
	return t.parent.Rollback()
}
