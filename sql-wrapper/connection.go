package sql

import (
	"context"
	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
)

// connectionImpl implements the ADBC Connection interface on top of database/sql.
type connectionImpl struct {
	driverbase.ConnectionImplBase

	// conn is the dedicated SQL connection for this ADBC session
	conn *sql.Conn
}

// newConnection creates a new ADBC Connection by acquiring a *sql.Conn from the pool.
func newConnection(ctx context.Context, db *databaseImpl) (adbc.Connection, error) {
	// Acquire a dedicated session
	sqlConn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	impl := &connectionImpl{
		ConnectionImplBase: base,
		conn:               sqlConn,
	}

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)
	return builder.Connection(), nil
}

// NewStatement satisfies adbc.Connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return newStatement(c), nil
}

// Commit is a no-op under auto-commit mode
func (c *connectionImpl) Commit(ctx context.Context) error {
	return nil
}

// Rollback is a no-op under auto-commit mode
func (c *connectionImpl) Rollback(ctx context.Context) error {
	return nil
}

// Close closes the underlying SQL connection
func (c *connectionImpl) Close() error {
	return c.conn.Close()
}
