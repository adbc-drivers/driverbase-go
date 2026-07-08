// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlwrapper

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type DatabaseFactory interface {
	CreateDatabase(db *DatabaseImplBase) (DatabaseImpl, error)
}

// ConnectionFactory allows custom connection implementations to be injected into sqlwrapper.
// Implementations can provide database-specific functionality like DbObjectsEnumerator.
type ConnectionFactory interface {
	// CreateConnection creates a custom connection implementation.
	// It receives a pre-built sqlwrapper ConnectionImpl and should return a connection
	// that embeds or wraps it to add database-specific functionality.
	CreateConnection(
		ctx context.Context,
		conn *ConnectionImplBase,
	) (ConnectionImpl, error)
}

type StatementFactory interface {
	CreateStatement(stmt *StatementImplBase) (StatementImpl, error)
}

// DBFactory handles creation of *sql.DB from connection options.
// Each driver is expected to implement this interface to provide database-specific
// DSN construction and connection logic for their particular database format.
type DBFactory interface {
	CreateDB(ctx context.Context, driverName string, opts map[string]string, logger *slog.Logger) (*sql.DB, error)
}

// Driver provides an ADBC driver implementation that wraps database/sql drivers.
type Driver struct {
	driverbase.DriverImplBase
	driverName        string
	vendorName        string
	databaseFactory   DatabaseFactory
	connectionFactory ConnectionFactory
	stmtFactory       StatementFactory
	dbFactory         DBFactory
	errorInspector    driverbase.ErrorInspector
}

// NewDriver creates a new sqlwrapper Driver with driver name, required DBFactory, and optional type converter.
func NewDriver(alloc memory.Allocator, driverName, vendorName string, dbFactory DBFactory) *Driver {
	info := driverbase.DefaultDriverInfo(vendorName)
	base := driverbase.NewDriverImplBase(info, alloc)
	// Use vendorName for error messages (e.g., "MySQL") to match CGO layer capitalization,
	base.ErrorHelper.DriverName = vendorName
	return &Driver{
		DriverImplBase:    base,
		driverName:        driverName,
		vendorName:        vendorName,
		databaseFactory:   nil, // No custom factory by default
		connectionFactory: nil,
		stmtFactory:       nil,
		dbFactory:         dbFactory,
	}
}

func (d *Driver) WithDatabaseFactory(factory DatabaseFactory) *Driver {
	d.databaseFactory = factory
	return d
}

// WithConnectionFactory sets a custom connection factory for this driver.
// This allows database-specific drivers to provide custom connection implementations
// with additional functionality like DbObjectsEnumerator.
func (d *Driver) WithConnectionFactory(factory ConnectionFactory) *Driver {
	d.connectionFactory = factory
	return d
}

func (d *Driver) WithStatementFactory(factory StatementFactory) *Driver {
	d.stmtFactory = factory
	return d
}

// WithErrorInspector sets a custom error inspector for extracting database error metadata.
// This allows drivers to map database-specific errors to ADBC status codes and extract
// SQLSTATE, vendor codes, and other error information.
func (d *Driver) WithErrorInspector(inspector driverbase.ErrorInspector) *Driver {
	d.errorInspector = inspector
	return d
}

type DatabaseImpl interface {
	driverbase.DatabaseImpl
}

// DatabaseImplBase implements the ADBC Database interface on top of database/sql.
type DatabaseImplBase struct {
	driverbase.DatabaseImplBase
	Derived DatabaseImpl

	// db is the Go SQL handle (connection pool)
	db         *sql.DB
	vendorName string
	// connectionFactory creates custom connection implementations if provided
	connectionFactory ConnectionFactory
	stmtFactory       StatementFactory
}

// NewDatabaseWithContext is the main entrypoint for driver‐agnostic ADBC database creation.
// It uses the driver name provided to NewDriver and expects opts[adbc.OptionKeyURI] to be the DSN/URI.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, d.ErrorHelper.WrapIO(err, "failed to initialize database base")
	}

	// Set error inspector if provided
	if d.errorInspector != nil {
		base.ErrorHelper.ErrorInspector = d.errorInspector
	}

	// Use DB factory to create the *sql.DB from options
	sqlDB, err := d.dbFactory.CreateDB(ctx, d.driverName, opts, d.Logger)
	if err != nil {
		return nil, base.ErrorHelper.WrapInvalidArgument(err, "failed to create database")
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		err = errors.Join(err, sqlDB.Close())
		return nil, base.ErrorHelper.WrapIO(err, "failed to ping database")
	}

	// Construct and return the ADBC Database wrapper
	wrapper := &DatabaseImplBase{
		DatabaseImplBase:  base,
		db:                sqlDB,
		vendorName:        d.vendorName,
		connectionFactory: d.connectionFactory,
		stmtFactory:       d.stmtFactory,
	}

	var impl DatabaseImpl
	if d.databaseFactory != nil {
		impl, err = d.databaseFactory.CreateDatabase(wrapper)
		if err != nil {
			err = errors.Join(err, sqlDB.Close())
			return nil, base.ErrorHelper.WrapIO(err, "failed to create custom database implementation")
		}
	} else {
		impl = wrapper
	}
	wrapper.Derived = impl
	return driverbase.NewDatabase(impl), nil
}

// Open creates a new ADBC Connection (session) by acquiring a *sql.Conn.
func (d *DatabaseImplBase) Open(ctx context.Context) (adbc.ConnectionWithContext, error) {
	return newConnection(ctx, d)
}

// Closes the database and its underlying connection pool.
func (d *DatabaseImplBase) Close(ctx context.Context) error {
	return d.db.Close()
}
