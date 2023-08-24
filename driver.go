package querypulse

import (
	"context"
	"database/sql/driver"
)

// Compile time assertion
var (
	_ driver.DriverContext = &zDriver{}
	_ driver.Connector     = &zDriver{}
)

// WrapConnector allows wrapping a database driver.Connector which eliminates
// the need to register it as an available driver.Driver.
func WrapConnector(dc driver.Connector, options Options) driver.Connector {

	return &zDriver{
		parent:    dc.Driver(),
		connector: dc,
		options:   options,
	}
}

// zDriver implements driver.Driver
type zDriver struct {
	parent    driver.Driver
	connector driver.Connector

	options Options
}

func wrapDriver(d driver.Driver, o Options) driver.Driver {
	if _, ok := d.(driver.DriverContext); ok {
		return zDriver{parent: d, options: o}
	}
	return struct{ driver.Driver }{zDriver{parent: d, options: o}}
}

func wrapConn(parent driver.Conn, options Options) driver.Conn {
	var (
		n, hasNameValueChecker = parent.(driver.NamedValueChecker)
		s, hasSessionResetter  = parent.(driver.SessionResetter)
	)
	c := &zConn{parent: parent, options: options}
	switch {
	case !hasNameValueChecker && !hasSessionResetter:
		return c
	case hasNameValueChecker && !hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
		}{c, n}
	case !hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.SessionResetter
		}{c, s}
	case hasNameValueChecker && hasSessionResetter:
		return struct {
			conn
			driver.NamedValueChecker
			driver.SessionResetter
		}{c, n, s}
	}
	panic("unreachable")
}

func wrapStmt(stmt driver.Stmt, query string, options Options) driver.Stmt {
	var (
		_, hasExeCtx    = stmt.(driver.StmtExecContext)
		_, hasQryCtx    = stmt.(driver.StmtQueryContext)
		c, hasColConv   = stmt.(driver.ColumnConverter)
		n, hasNamValChk = stmt.(driver.NamedValueChecker)
	)

	s := zStmt{parent: stmt, query: query, options: options}
	switch {
	case !hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
		}{s}
	case !hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
		}{s, s}
	case hasExeCtx && !hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
		}{s, s}
	case hasExeCtx && hasQryCtx && !hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
		}{s, s, s}
	case !hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
		}{s, c}
	case !hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && !hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
		}{s, s, c}
	case hasExeCtx && hasQryCtx && hasColConv && !hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
		}{s, s, s, c}

	case !hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.NamedValueChecker
		}{s, n}
	case !hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && !hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.NamedValueChecker
		}{s, s, n}
	case hasExeCtx && hasQryCtx && !hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.NamedValueChecker
		}{s, s, s, n}
	case !hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, c, n}
	case !hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && !hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, c, n}
	case hasExeCtx && hasQryCtx && hasColConv && hasNamValChk:
		return struct {
			driver.Stmt
			driver.StmtExecContext
			driver.StmtQueryContext
			driver.ColumnConverter
			driver.NamedValueChecker
		}{s, s, s, c, n}
	}
	panic("unreachable")
}

func (d zDriver) OpenConnector(name string) (driver.Connector, error) {
	var err error
	d.connector, err = d.parent.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return d, err
}

func (d zDriver) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := d.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &zConn{parent: c, options: d.options}, nil
}

func (d zDriver) Driver() driver.Driver {
	return d
}
