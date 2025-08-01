package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	// register the "mysql" driver with database/sql
	_ "github.com/go-sql-driver/mysql"

	sqlwrapper "github.com/adbc-drivers/driverbase-go/sql"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
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
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_json":          "true",
		}

		// Add length if available
		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON":
		// Convert MySQL spatial types to binary with spatial metadata
		metadata := arrow.MetadataFrom(map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_spatial":       "true",
		})
		return arrow.BinaryTypes.Binary, true, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
			"mysql.is_enum_set":      "true",
		}

		if length, ok := colType.Length(); ok {
			metadataMap["sql.length"] = fmt.Sprintf("%d", length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, true, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertColumnType(colType)
	}
}

// NewDriver constructs the ADBC Driver for "mysql".
func NewDriver() adbc.Driver {
	// Create sqlwrapper driver with MySQL type converter and driver name
	return sqlwrapper.NewDriver("mysql", &MySQLTypeConverter{
		DefaultTypeConverter: &sqlwrapper.DefaultTypeConverter{},
	})
}
