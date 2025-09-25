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

// newDatabase constructs a new ADBC Database backed by *sql.DB.
func newDatabase(ctx context.Context, drvImpl *driverbase.DriverImplBase, driverName string, opts map[string]string, typeConverter TypeConverter, connectionFactory ConnectionFactory, dbFactory DBFactory) (adbc.Database, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, drvImpl)
	if err != nil {
		return nil, drvImpl.ErrorHelper.IO("failed to initialize database base: %v", err)
	}

	// Use DB factory to create the *sql.DB from options
	sqlDB, err := dbFactory.CreateDB(ctx, driverName, opts)
	if err != nil {
		return nil, base.ErrorHelper.InvalidArgument("failed to create database: %v", err)
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		err = errors.Join(err, sqlDB.Close())
		return nil, base.ErrorHelper.IO("failed to ping database: %v", err)
	}

	// Construct and return the ADBC Database wrapper
	db := &databaseImpl{
		DatabaseImplBase:  base,
		db:                sqlDB,
		typeConverter:     typeConverter,
		connectionFactory: connectionFactory,
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
