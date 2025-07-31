// sql-wrapper/driver.go
package sqlwrapper

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
)

type Driver struct {
	typeConverter TypeConverter
}

// NewDriver creates a new sqlwrapper Driver with optional type converter.
// If converter is nil, uses DefaultTypeConverter.
func NewDriver(converter TypeConverter) *Driver {
	if converter == nil {
		converter = &DefaultTypeConverter{}
	}
	return &Driver{
		typeConverter: converter,
	}
}

// NewDatabase is the main entrypoint for driver‐agnostic ADBC database creation.
// It expects opts["driver"] to be the database/sql driver name (e.g. "mysql", "pgx")
// and opts[adbc.OptionKeyURI] to be the DSN/URI.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// NewDatabaseWithContext is the same, but lets you pass in a context.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	return newDatabase(ctx, opts, d.typeConverter)
}
