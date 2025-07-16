// Copyright (c) 2025 Columnar Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"fmt"

	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	_ "github.com/go-sql-driver/mysql"
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
	sqlDB, err := sql.Open("mysql", dsn)
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
