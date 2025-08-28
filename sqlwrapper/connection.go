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
)

// ConnectionImpl implements the ADBC Connection interface on top of database/sql.
type ConnectionImpl struct {
	driverbase.ConnectionImplBase

	// Conn is the dedicated SQL connection for this ADBC session
	Conn *sql.Conn
	// TypeConverter handles SQL-to-Arrow type conversion
	TypeConverter TypeConverter
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

	var impl driverbase.ConnectionImpl

	// Use custom factory if provided, otherwise use default implementation
	if db.connectionFactory != nil {
		impl, err = db.connectionFactory.CreateConnection(ctx, base, sqlConn, db.typeConverter, db.db)
		if err != nil {
			err = errors.Join(err, sqlConn.Close())
			return nil, err
		}
	} else {
		// Default sqlwrapper connection implementation
		impl = &ConnectionImpl{
			ConnectionImplBase: base,
			Conn:               sqlConn,
			TypeConverter:      db.typeConverter,
		}
	}

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)

	// If impl supports DbObjectsEnumerator, register it
	if enumerator, ok := any(impl).(driverbase.DbObjectsEnumerator); ok {
		builder = builder.WithDbObjectsEnumerator(enumerator)
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

// SetOption sets a string option on this connection
func (c *ConnectionImpl) SetOption(key, value string) error {
	return c.ConnectionImplBase.SetOption(key, value)
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
