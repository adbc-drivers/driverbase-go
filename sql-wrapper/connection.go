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
	// typeConverter handles SQL-to-Arrow type conversion
	typeConverter TypeConverter
}

// newConnection creates a new ADBC Connection by acquiring a *sql.Conn from the pool.
func newConnection(ctx context.Context, db *databaseImpl) (adbc.Connection, error) {
	// Acquire a dedicated session
	sqlConn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, db.DatabaseImplBase.ErrorHelper.Errorf(adbc.StatusIO, "failed to acquire database connection: %v", err)
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	impl := &connectionImpl{
		ConnectionImplBase: base,
		conn:               sqlConn,
		typeConverter:      &DefaultTypeConverter{}, // Use default converter, can be overridden by drivers
	}

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)
	return builder.Connection(), nil
}

// NewStatement satisfies adbc.Connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return newStatement(c), nil
}

// SetTypeConverter allows higher-level drivers to customize type conversion
func (c *connectionImpl) SetTypeConverter(converter TypeConverter) {
	c.typeConverter = converter
}

// SetOption sets a string option on this connection
func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	case OptionKeyTypeConverter:
		converter, exists := GetTypeConverter(value)
		if !exists {
			return c.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "unknown type converter: %s", value)
		}
		c.SetTypeConverter(converter)
		return nil
	default:
		return c.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "unsupported option: %s", key)
	}
}

// Commit is a no-op under auto-commit mode
func (c *connectionImpl) Commit(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Commit not supported in auto-commit mode",
	)
}

// Rollback is a no-op under auto-commit mode
func (c *connectionImpl) Rollback(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Rollback not supported in auto-commit mode",
	)
}

// Close closes the underlying SQL connection
func (c *connectionImpl) Close() error {
	if err := c.conn.Close(); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusIO, "failed to close connection: %v", err)
	}
	return nil
}
