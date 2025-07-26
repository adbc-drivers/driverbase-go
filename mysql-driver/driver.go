package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"strings"

	// 1) register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sql"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// MySQLTypeConverter provides MySQL-specific type conversion enhancements
type MySQLTypeConverter struct {
	*sqlwrapper.DefaultTypeConverter
}

// ConvertColumnType implements TypeConverter with MySQL-specific enhancements
func (m *MySQLTypeConverter) ConvertColumnType(colType *sql.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName())
	
	switch typeName {
	case "JSON":
		// Convert MySQL JSON to Arrow string with special metadata
		keys := []string{"sql.database_type_name", "sql.column_name", "mysql.is_json"}
		values := []string{colType.DatabaseTypeName(), colType.Name(), "true"}
		
		// Add length if available
		if length, ok := colType.Length(); ok {
			keys = append(keys, "sql.length")
			values = append(values, fmt.Sprintf("%d", length))
		}
		
		metadata := arrow.NewMetadata(keys, values)
		return arrow.BinaryTypes.String, true, metadata, nil
		
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		keys := []string{"sql.database_type_name", "sql.column_name", "mysql.is_spatial"}
		values := []string{colType.DatabaseTypeName(), colType.Name(), "true"}
		metadata := arrow.NewMetadata(keys, values)
		return arrow.BinaryTypes.Binary, true, metadata, nil
		
	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		keys := []string{"sql.database_type_name", "sql.column_name", "mysql.is_enum_set"}
		values := []string{colType.DatabaseTypeName(), colType.Name(), "true"}
		
		if length, ok := colType.Length(); ok {
			keys = append(keys, "sql.length")
			values = append(values, fmt.Sprintf("%d", length))
		}
		
		metadata := arrow.NewMetadata(keys, values)
		return arrow.BinaryTypes.String, true, metadata, nil
		
	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertColumnType(colType)
	}
}

// init registers the MySQL type converter
func init() {
	sqlwrapper.RegisterTypeConverter("mysql", func() sqlwrapper.TypeConverter {
		return &MySQLTypeConverter{DefaultTypeConverter: &sqlwrapper.DefaultTypeConverter{}}
	})
}

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
	m := maps.Clone(opts)
	m["driver"] = "mysql"

	// hand off to the core sql-wrapper
	sqlDB, err := sqlwrapper.Driver{}.NewDatabaseWithContext(ctx, m)
	if err != nil {
		return nil, err
	}

	// Wrap the database to automatically set MySQL type converter
	return &mysqlDatabase{Database: sqlDB}, nil
}

// mysqlDatabase wraps the sql-wrapper database to provide MySQL-specific enhancements
type mysqlDatabase struct {
	adbc.Database
}

// Open wraps the sql-wrapper connection and sets the MySQL type converter
func (db *mysqlDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	conn, err := db.Database.Open(ctx)
	if err != nil {
		return nil, err
	}

	// Set MySQL type converter using the ADBC standard way
	if optionSetter, ok := conn.(adbc.PostInitOptions); ok {
		err = optionSetter.SetOption(sqlwrapper.OptionKeyTypeConverter, "mysql")
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to set MySQL type converter: %w", err)
		}
	}

	return conn, nil
}
