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

// Driver provides an ADBC driver implementation that wraps database/sql drivers.
// It uses a configurable TypeConverter for SQL-to-Arrow type mapping and conversion.
type Driver struct {
	driverbase.DriverImplBase
	driverName        string
	typeConverter     TypeConverter
	connectionFactory ConnectionFactory
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
		connectionFactory: nil, // No custom factory by default
	}
}

// WithConnectionFactory sets a custom connection factory for this driver.
// This allows database-specific drivers to provide custom connection implementations
// with additional functionality like DbObjectsEnumerator.
func (d *Driver) WithConnectionFactory(factory ConnectionFactory) *Driver {
	d.connectionFactory = factory
	return d
}

// NewDatabase is the main entrypoint for driver‐agnostic ADBC database creation.
// It uses the driver name provided to NewDriver and expects opts[adbc.OptionKeyURI] to be the DSN/URI.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// NewDatabaseWithContext is the same, but lets you pass in a context.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	return newDatabase(ctx, &d.DriverImplBase, d.driverName, opts, d.typeConverter, d.connectionFactory)
}
