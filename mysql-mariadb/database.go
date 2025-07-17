package mysql

import (
	"context"
	"fmt"

	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sqldriver"
	"github.com/apache/arrow-adbc/go/adbc"
)

// databaseImpl implements the ADBC Database interface on top of database/sql.
type databaseImpl struct {
	driverbase.DatabaseImplBase

	// db is the Go SQL handle (connection pool)
	db *sql.DB
}

// newDatabase constructs a new ADBC Database backed by *sql.DB.
func newDatabase(ctx context.Context, driver *Driver, opts map[string]string) (adbc.Database, error) {
	// Pull the DSN from the standard ADBC URI option
	dsn := opts[adbc.OptionKeyURI]
	if dsn == "" {
		return nil, fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// Open the underlying SQL pool
	sqlDB, err := sqldriver.New(
		sqldriver.WithDriverName("mysql"),
		sqldriver.WithDSN(dsn),
		sqldriver.WithMaxOpenConns(25),
	)
	if err != nil {
		return nil, err
	}

	// Initialize driverbase plumbing (SetOption/GetOption, etc.)
	base, err := driverbase.NewDatabaseImplBase(ctx, &driver.DriverImplBase)
	if err != nil {
		sqlDB.Close()
		return nil, err
	}

	// Construct and return the ADBC Database wrapper
	db := &databaseImpl{
		DatabaseImplBase: base,
		db:               sqlDB,
	}
	return driverbase.NewDatabase(db), nil
}

// Open creates a new ADBC Connection (session) by acquiring a *sql.Conn.
func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	return newConnection(d)
}
