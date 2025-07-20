package mysql

import (
	"context"
	"fmt"

	// 1) register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sql"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Driver wraps the driverbase plumbing for MySQL.
type Driver struct {
	driverbase.DriverImplBase
}

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver() adbc.Driver {
	info := driverbase.DefaultDriverInfo("mysql")
	base := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	return &Driver{DriverImplBase: base}
}

// NewDatabase implements adbc.Driver interface.
// It injects opts["driver"]="mysql" and delegates to sql-wrapper.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

// NewDatabaseWithContext implements adbc.Driver interface with context.
func (d *Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	// ensure the URI is present
	if _, ok := opts[adbc.OptionKeyURI]; !ok {
		return nil, fmt.Errorf("mysql: missing option %q", adbc.OptionKeyURI)
	}

	// copy and inject the driver name
	m := make(map[string]string, len(opts)+1)
	for k, v := range opts {
		m[k] = v
	}
	m["driver"] = "mysql"

	// hand off to the core sql-wrapper
	return sql.Driver{}.NewDatabaseWithContext(ctx, m)
}
