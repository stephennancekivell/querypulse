package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

type CustomDriver struct {
	//innerDriver *pq.Driver
	innerDriver driver.Driver
	connector   driver.Connector
}

// Connect implements driver.Connector.
func (c *CustomDriver) Connect(ctx context.Context) (driver.Conn, error) {
	return c.connector.Connect(ctx) // TODO wrap
}

// Driver implements driver.Connector.
func (c *CustomDriver) Driver() driver.Driver {
	return c.innerDriver
}

var _ driver.Driver = (*CustomDriver)(nil)
var _ driver.Connector = (*CustomDriver)(nil)

// overwrites the registered driver with a wrapped one
func Register(driverName string) error {
	db, err := sql.Open("postgres", "")
	if err != nil {
		return err
	}
	dri := db.Driver()
	if err = db.Close(); err != nil {
		return err
	}
	d := &CustomDriver{innerDriver: dri}
	sql.Register(driverName, d)
	return nil

}

func (d *CustomDriver) Open(name string) (driverConn driver.Conn, err error) {
	fmt.Println("Opening a new connection...")
	conn, err := d.innerDriver.Open(name)
	if err != nil {
		return nil, err
	}

	return &CustomConn{conn}, nil
}

// CustomConn is a wrapper around a database connection that logs SQL statements.
type CustomConn struct {
	innerConn driver.Conn
}

var _ driver.Conn = (*CustomConn)(nil)

// Begin implements driver.Conn.
func (c *CustomConn) Begin() (driver.Tx, error) {
	return c.innerConn.Begin() // todo wrap
}

// Close implements driver.Conn.
func (c *CustomConn) Close() error {
	return c.innerConn.Close()
}

// Prepare is the method that gets called when preparing an SQL statement.
func (c *CustomConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Println("Preparing statement:", query)
	stmt, err := c.innerConn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &CustomStmt{stmt, query}, nil
}

// CustomStmt is a wrapper around a prepared statement that logs SQL statements.
type CustomStmt struct {
	innerStmt driver.Stmt
	query     string
}

var _ driver.Stmt = (*CustomStmt)(nil)

// var _ driver.StmtQueryContext = (*CustomStmt)(nil)

// Close implements driver.Stmt.
func (c *CustomStmt) Close() error {
	fmt.Println("close statement")
	return c.innerStmt.Close()
}

// NumInput implements driver.Stmt.
func (c *CustomStmt) NumInput() int {
	fmt.Println("NumInput")
	return c.innerStmt.NumInput()
}

// Query implements driver.Stmt.
func (c *CustomStmt) Query(args []driver.Value) (driver.Rows, error) {
	fmt.Printf("query %v %v", c.query, args)
	return c.innerStmt.Query(args)
}

// Exec is the method that gets called when executing an SQL statement.
func (s *CustomStmt) Exec(args []driver.Value) (driver.Result, error) {
	fmt.Println("Executing statement:", s.innerStmt)
	return s.innerStmt.Exec(args)
}

// QueryContext implements driver.StmtQueryContext.
// func (c *CustomStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
// 	fmt.Printf("query %v %v", c.query, args)
// 	if qc,ok :=  c.innerStmt.(driver.QueryerContext); ok {
// 		qc.QueryContext(ctx, args)
// 	}
// 	panic("unimplemented")
// }
