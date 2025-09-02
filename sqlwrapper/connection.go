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

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ConnectionImpl implements the ADBC Connection interface on top of database/sql.
type ConnectionImpl struct {
	driverbase.ConnectionImplBase

	// Conn is the dedicated SQL connection for this ADBC session
	Conn *sql.Conn
	// TypeConverter handles SQL-to-Arrow type conversion
	TypeConverter TypeConverter
	// db is the underlying database for metadata operations
	Db *sql.DB
}

// newConnection creates a new ADBC Connection by acquiring a *sql.Conn from the pool.
func newConnection(ctx context.Context, db *databaseImpl) (adbc.Connection, error) {
	// Acquire a dedicated session
	sqlConn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, db.ErrorHelper.IO("failed to acquire database connection: %v", err)
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)

	// Create the base sqlwrapper connection first
	sqlwrapperConn := &ConnectionImpl{
		ConnectionImplBase: base,
		Conn:               sqlConn,
		TypeConverter:      db.typeConverter,
		Db:                 db.db,
	}

	var impl driverbase.ConnectionImpl

	// Use custom factory if provided, otherwise use default implementation
	if db.connectionFactory != nil {
		impl, err = db.connectionFactory.CreateConnection(ctx, sqlwrapperConn)
		if err != nil {
			err = errors.Join(err, sqlConn.Close())
			return nil, err
		}
	} else {
		// Default sqlwrapper connection implementation
		impl = sqlwrapperConn
	}

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)

	// If the implementation supports any of these extras, register them
	if trait, ok := any(impl).(driverbase.AutocommitSetter); ok {
		builder.WithAutocommitSetter(trait)
	}
	if trait, ok := any(impl).(driverbase.CurrentNamespacer); ok {
		builder.WithCurrentNamespacer(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumerator); ok {
		builder.WithDbObjectsEnumerator(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumeratorFactory); ok {
		builder.WithDbObjectsEnumeratorFactory(trait)
	}
	if trait, ok := any(impl).(driverbase.DriverInfoPreparer); ok {
		builder.WithDriverInfoPreparer(trait)
	}
	if trait, ok := any(impl).(driverbase.TableTypeLister); ok {
		builder.WithTableTypeLister(trait)
	}

	return builder.Connection(), nil
}

// NewStatement satisfies adbc.Connection
func (c *ConnectionImpl) NewStatement() (adbc.Statement, error) {
	return newStatement(c), nil
}

// SetTypeConverter allows higher-level drivers to customize type conversion
func (c *ConnectionImpl) SetTypeConverter(converter TypeConverter) {
	c.TypeConverter = converter
}

// ExecuteBulkIngest performs bulk ingest operation.
// Default implementation returns not supported - drivers should override this method.
// TODO: Consider enhancing interface to return row count (int64, error) instead of just error
// to support proper ExecuteUpdate row count reporting from bulk ingest operations.
func (c *ConnectionImpl) ExecuteBulkIngest(ctx context.Context, options *driverbase.BulkIngestOptions, stream array.RecordReader) error {
	return c.Base().ErrorHelper.NotImplemented("bulk ingest not supported by this driver")
}

// SetOption sets a string option on this connection
func (c *ConnectionImpl) SetOption(key, value string) error {
	return c.ConnectionImplBase.SetOption(key, value)
}

func (c *ConnectionImpl) GetOption(key string) (string, error) {
	return c.ConnectionImplBase.GetOption(key)
}

// Commit is a no-op under auto-commit mode
// TODO (https://github.com/adbc-drivers/driverbase-go/issues/28): we'll likely want to utilize https://pkg.go.dev/database/sql#Tx
// to manage this here
func (c *ConnectionImpl) Commit(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Commit not supported in auto-commit mode",
	)
}

// Rollback is a no-op under auto-commit mode
// TODO (https://github.com/adbc-drivers/driverbase-go/issues/28): we'll likely want to utilize https://pkg.go.dev/database/sql#Tx
// to manage this here
func (c *ConnectionImpl) Rollback(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Rollback not supported in auto-commit mode",
	)
}

// Close closes the underlying SQL connection
func (c *ConnectionImpl) Close() error {
	return c.Conn.Close()
}
