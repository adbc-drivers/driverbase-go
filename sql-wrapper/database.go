package sql

import (
	"context"

	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// databaseImpl implements the ADBC Database interface on top of database/sql.
type databaseImpl struct {
	driverbase.DatabaseImplBase

	// db is the Go SQL handle (connection pool)
	db *sql.DB
}

// newDatabase constructs a new ADBC Database backed by *sql.DB.
func newDatabase(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	drvName, ok := opts["driver"]
	if !ok {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "missing 'driver' option in sql-wrapper",
		}
	}
	dsn, ok := opts[adbc.OptionKeyURI]
	if !ok || dsn == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "missing 'uri' option in sql-wrapper",
		}
	}

	// Initialize driverbase plumbing first to get ErrorHelper
	info := driverbase.DefaultDriverInfo(drvName)
	drvImpl := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)

	base, err := driverbase.NewDatabaseImplBase(ctx, &drvImpl)
	if err != nil {
		return nil, drvImpl.ErrorHelper.Errorf(adbc.StatusIO, "failed to initialize database base: %v", err)
	}

	// Open the underlying SQL pool
	sqlDB, err := sql.Open(drvName, dsn)
	if err != nil {
		return nil, base.ErrorHelper.Errorf(adbc.StatusIO, "failed to open database: %v", err)
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, base.ErrorHelper.Errorf(adbc.StatusIO, "failed to ping database: %v", err)
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
	return newConnection(ctx, d)
}

// Closes the database and its underlying connection pool.
func (d *databaseImpl) Close() error {
	if err := d.db.Close(); err != nil {
		return d.DatabaseImplBase.ErrorHelper.Errorf(adbc.StatusIO, "failed to close database: %v", err)
	}
	return nil
}
