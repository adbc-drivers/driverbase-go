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
	"fmt"
	"net/url"

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
// Individual drivers can implement this to provide database-specific connection logic.
type DBFactory interface {
	CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error)
}

// DefaultDBFactory provides standard database connection creation using sql.Open.
// It builds a DSN using standard URI format with credential injection.
type DefaultDBFactory struct{}

// CreateDB creates a *sql.DB using sql.Open with a constructed DSN.
func (f *DefaultDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string) (*sql.DB, error) {
	dsn, err := f.buildDSN(opts)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// buildDSN constructs a DSN from the provided options using standard URI format.
func (f *DefaultDBFactory) buildDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	if baseURI == "" {
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// If no credentials provided, return original URI
	if username == "" && password == "" {
		return baseURI, nil
	}

	// Parse URI and inject credentials
	u, err := url.Parse(baseURI)
	if err != nil {
		return "", fmt.Errorf("invalid URI format: %w", err)
	}

	// Set user info if provided
	if username != "" {
		if password != "" {
			u.User = url.UserPassword(username, password)
		} else {
			u.User = url.User(username)
		}
	}

	return u.String(), nil
}

// Driver provides an ADBC driver implementation that wraps database/sql drivers.
// It uses a configurable TypeConverter for SQL-to-Arrow type mapping and conversion.
type Driver struct {
	driverbase.DriverImplBase
	driverName        string
	typeConverter     TypeConverter
	connectionFactory ConnectionFactory
	dbFactory         DBFactory
}

// NewDriver creates a new sqlwrapper Driver with driver name and optional type converter.
// If converter is nil, uses DefaultTypeConverter.
func NewDriver(alloc memory.Allocator, driverName, vendorName string, converter TypeConverter) *Driver {
	if converter == nil {
		converter = DefaultTypeConverter{}
	}
	info := driverbase.DefaultDriverInfo(vendorName)
	base := driverbase.NewDriverImplBase(info, alloc)
	base.ErrorHelper.DriverName = driverName
	return &Driver{
		DriverImplBase:    base,
		driverName:        driverName,
		typeConverter:     converter,
		connectionFactory: nil,                 // No custom factory by default
		dbFactory:         &DefaultDBFactory{}, // Default DB factory
	}
}

// WithConnectionFactory sets a custom connection factory for this driver.
// This allows database-specific drivers to provide custom connection implementations
// with additional functionality like DbObjectsEnumerator.
func (d *Driver) WithConnectionFactory(factory ConnectionFactory) *Driver {
	d.connectionFactory = factory
	return d
}

// WithDBFactory sets a custom DB factory for this driver.
// This allows database-specific drivers to provide custom sql.DB creation logic
// for handling URI, username, and password options.
func (d *Driver) WithDBFactory(factory DBFactory) *Driver {
	d.dbFactory = factory
	return d
}

// NewDatabase is the main entrypoint for driver‐agnostic ADBC database creation.
// It uses the driver name provided to NewDriver and expects opts[adbc.OptionKeyURI] to be the DSN/URI.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// NewDatabaseWithContext is the same, but lets you pass in a context.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	return newDatabase(ctx, &d.DriverImplBase, d.driverName, opts, d.typeConverter, d.connectionFactory, d.dbFactory)
}
