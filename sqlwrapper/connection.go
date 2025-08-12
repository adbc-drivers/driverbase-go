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
		return nil, db.ErrorHelper.IO("failed to acquire database connection: %v", err)
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)
	impl := &connectionImpl{
		ConnectionImplBase: base,
		conn:               sqlConn,
		typeConverter:      db.typeConverter, // Use the type converter from the database
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
	return c.ConnectionImplBase.SetOption(key, value)
}

// Commit is a no-op under auto-commit mode
// TODO (https://github.com/adbc-drivers/driverbase-go/issues/28): we'll likely want to utilize https://pkg.go.dev/database/sql#Tx
// to manage this here
func (c *connectionImpl) Commit(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Commit not supported in auto-commit mode",
	)
}

// Rollback is a no-op under auto-commit mode
// TODO (https://github.com/adbc-drivers/driverbase-go/issues/28): we'll likely want to utilize https://pkg.go.dev/database/sql#Tx
// to manage this here
func (c *connectionImpl) Rollback(ctx context.Context) error {
	return c.Base().ErrorHelper.Errorf(
		adbc.StatusNotImplemented,
		"Rollback not supported in auto-commit mode",
	)
}

// Close closes the underlying SQL connection
func (c *connectionImpl) Close() error {
	return c.conn.Close()
}
