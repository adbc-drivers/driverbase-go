package mysql

import (
	"context"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/arrow/memory"
	_ "github.com/go-sql-driver/mysql"
)

// Driver implements the ADBC Driver interface for MySQL via database/sql.
// It delegates configuration and option handling to driverbase.
// The Go MySQL driver is registered via the blank import above.
type Driver struct {
	driverbase.DriverImplBase
}

var _ adbc.Driver = (*Driver)(nil)

// NewDriver constructs a new MySQL ADBC Driver using the provided Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("mysql")
	base := driverbase.NewDriverImplBase(info, alloc)
	return &Driver{DriverImplBase: base}
}

// NewDatabase opens a new ADBC Database with the given options.
// It satisfies the adbc.Driver interface.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// NewDatabaseWithContext opens a new ADBC Database with context.
func (d *Driver) NewDatabaseWithContext(
	ctx context.Context,
	opts map[string]string,
) (adbc.Database, error) {
	return newDatabase(ctx, d, opts)
}
