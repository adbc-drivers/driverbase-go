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

// DBFactory handles creation of *sql.DB from connection options.
// Each driver is expected to implement this interface to provide database-specific
// DSN construction and connection logic for their particular database format.
type DBFactory interface {
	CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error)
}

// DBFactoryWithLogger is an optional interface that DBFactory implementations
// can implement to receive the database's logger for use during connection creation.
type DBFactoryWithLogger interface {
	DBFactory
	SetLogger(logger *slog.Logger)
}

// Driver provides an ADBC driver implementation that wraps database/sql drivers.
// It uses a configurable TypeConverter for SQL-to-Arrow type mapping and conversion.
type Driver struct {
	driverbase.DriverImplBase
	driverName        string
	typeConverter     TypeConverter
	connectionFactory ConnectionFactory
	dbFactory         DBFactory
	errorInspector    driverbase.ErrorInspector
}

// NewDriver creates a new sqlwrapper Driver with driver name, required DBFactory, and optional type converter.
// If converter is nil, uses DefaultTypeConverter.
func NewDriver(alloc memory.Allocator, driverName, vendorName string, dbFactory DBFactory, converter TypeConverter) *Driver {
	if converter == nil {
		converter = DefaultTypeConverter{VendorName: vendorName}
	}
	info := driverbase.DefaultDriverInfo(vendorName)
	base := driverbase.NewDriverImplBase(info, alloc)
	// Use vendorName for error messages (e.g., "MySQL") to match CGO layer capitalization,
	base.ErrorHelper.DriverName = vendorName
	return &Driver{
		DriverImplBase:    base,
		driverName:        driverName,
		typeConverter:     converter,
		connectionFactory: nil, // No custom factory by default
		dbFactory:         dbFactory,
	}
}

// WithConnectionFactory sets a custom connection factory for this driver.
// This allows database-specific drivers to provide custom connection implementations
// with additional functionality like DbObjectsEnumerator.
func (d *Driver) WithConnectionFactory(factory ConnectionFactory) *Driver {
	d.connectionFactory = factory
	return d
}

// WithErrorInspector sets a custom error inspector for extracting database error metadata.
// This allows drivers to map database-specific errors to ADBC status codes and extract
// SQLSTATE, vendor codes, and other error information.
func (d *Driver) WithErrorInspector(inspector driverbase.ErrorInspector) *Driver {
	d.errorInspector = inspector
	return d
}

// NewDatabase is the main entrypoint for driver‐agnostic ADBC database creation.
// It uses the driver name provided to NewDriver and expects opts[adbc.OptionKeyURI] to be the DSN/URI.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// databaseImpl implements the ADBC Database interface on top of database/sql.
type databaseImpl struct {
	driverbase.DatabaseImplBase

	// db is the Go SQL handle (connection pool)
	db *sql.DB
	// typeConverter handles SQL-to-Arrow type conversion
	typeConverter TypeConverter
	// connectionFactory creates custom connection implementations if provided
	connectionFactory ConnectionFactory
}

// NewDatabaseWithContext is the same, but lets you pass in a context.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, d.ErrorHelper.WrapIO(err, "failed to initialize database base")
	}

	// Set error inspector if provided
	if d.errorInspector != nil {
		base.ErrorHelper.ErrorInspector = d.errorInspector
	}

	// If the DBFactory supports logging, provide the driver's logger
	if factoryWithLogger, ok := d.dbFactory.(DBFactoryWithLogger); ok {
		factoryWithLogger.SetLogger(d.Logger)
	}

	// Use DB factory to create the *sql.DB from options
	sqlDB, err := d.dbFactory.CreateDB(ctx, d.driverName, opts)
	if err != nil {
		return nil, base.ErrorHelper.WrapInvalidArgument(err, "failed to create database")
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		err = errors.Join(err, sqlDB.Close())
		return nil, base.ErrorHelper.WrapIO(err, "failed to ping database")
	}

	// Construct and return the ADBC Database wrapper
	db := &databaseImpl{
		DatabaseImplBase:  base,
		db:                sqlDB,
		typeConverter:     d.typeConverter,
		connectionFactory: d.connectionFactory,
	}
	return driverbase.NewDatabase(db), nil
}

// Open creates a new ADBC Connection (session) by acquiring a *sql.Conn.
func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	return newConnection(ctx, d)
}

// Closes the database and its underlying connection pool.
func (d *databaseImpl) Close() error {
	return d.db.Close()
}
