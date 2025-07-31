package sqlwrapper

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// Custom option keys for type conversion
const (
	// OptionKeyTypeConverter sets the type converter for SQL-to-Arrow conversion
	OptionKeyTypeConverter = "adbc.sqlwrapper.type_converter"
)

// TypeConverter allows higher-level drivers to customize SQL-to-Arrow type conversion
type TypeConverter interface {
	// ConvertColumnType converts a SQL column type to an Arrow type and nullable flag
	// It also returns metadata that should be included in the Arrow field
	ConvertColumnType(colType *sql.ColumnType) (arrowType arrow.DataType, nullable bool, metadata arrow.Metadata, err error)
}

// DefaultTypeConverter provides the default SQL-to-Arrow type conversion
type DefaultTypeConverter struct{}

// TypeConverterRegistry manages registered type converters
var (
	typeConverterRegistry = map[string]func() TypeConverter{
		"default": func() TypeConverter { return &DefaultTypeConverter{} },
	}
	typeConverterMutex sync.RWMutex
)

// RegisterTypeConverter registers a type converter factory function with a name
func RegisterTypeConverter(name string, factory func() TypeConverter) {
	typeConverterMutex.Lock()
	defer typeConverterMutex.Unlock()
	typeConverterRegistry[name] = factory
}

// GetTypeConverter retrieves a type converter by name
func GetTypeConverter(name string) (TypeConverter, bool) {
	typeConverterMutex.RLock()
	factory, exists := typeConverterRegistry[name]
	typeConverterMutex.RUnlock()

	if !exists {
		return nil, false
	}
	return factory(), true
}

// ConvertColumnType implements TypeConverter interface with the default conversion logic
func (d *DefaultTypeConverter) ConvertColumnType(colType *sql.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName())
	nullable, _ := colType.Nullable()

	// Handle DECIMAL/NUMERIC types with proper precision and scale
	if typeName == "DECIMAL" || typeName == "NUMERIC" {
		if precision, scale, ok := colType.DecimalSize(); ok {
			// Create Arrow Decimal128 type with actual precision and scale
			arrowType := &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}

			// Build metadata with decimal information
			metadata := arrow.MetadataFrom(map[string]string{
				"sql.database_type_name": colType.DatabaseTypeName(),
				"sql.column_name":        colType.Name(),
				"sql.precision":          fmt.Sprintf("%d", precision),
				"sql.scale":              fmt.Sprintf("%d", scale),
			})

			return arrowType, nullable, metadata, nil
		}
		// Fall back to string if precision/scale not available
	}

	// Handle DATETIME/TIMESTAMP types with proper precision
	if typeName == "DATETIME" || typeName == "TIMESTAMP" {
		// Try to get precision from DecimalSize (which represents fractional seconds precision)
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
		}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-9)
			metadataMap["sql.fractional_seconds_precision"] = fmt.Sprintf("%d", precision)

			switch precision {
			case 0:
				// No fractional seconds - use seconds
				timestampType = arrow.FixedWidthTypes.Timestamp_s
			case 1, 2, 3:
				// 1-3 digits: milliseconds precision
				timestampType = arrow.FixedWidthTypes.Timestamp_ms
			case 4, 5, 6:
				// 4-6 digits: microseconds precision
				timestampType = arrow.FixedWidthTypes.Timestamp_us
			case 7, 8, 9:
				// 7-9 digits: nanoseconds precision
				timestampType = arrow.FixedWidthTypes.Timestamp_ns
			default:
				// Fallback to nanoseconds for unexpected values
				timestampType = arrow.FixedWidthTypes.Timestamp_ns
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = arrow.FixedWidthTypes.Timestamp_us
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, nullable, metadata, nil
	}

	// Handle TIME types with proper precision
	if typeName == "TIME" {
		// Try to get precision from DecimalSize (which represents fractional seconds precision)
		var timeType arrow.DataType
		metadataMap := map[string]string{
			"sql.database_type_name": colType.DatabaseTypeName(),
			"sql.column_name":        colType.Name(),
		}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-9)
			metadataMap["sql.fractional_seconds_precision"] = fmt.Sprintf("%d", precision)

			switch precision {
			case 0:
				// No fractional seconds - use 32-bit seconds (closest available)
				timeType = arrow.FixedWidthTypes.Time32s
			case 1, 2, 3:
				// 1-3 digits: use 32-bit milliseconds (closest available)
				timeType = arrow.FixedWidthTypes.Time32ms
			case 4, 5, 6:
				// 4-6 digits: microseconds precision
				timeType = arrow.FixedWidthTypes.Time64us
			case 7, 8, 9:
				// 7-9 digits: nanoseconds precision
				timeType = arrow.FixedWidthTypes.Time64ns
			default:
				// Fallback to nanoseconds for unexpected values
				timeType = arrow.FixedWidthTypes.Time64ns
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timeType = arrow.FixedWidthTypes.Time64us
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timeType, nullable, metadata, nil
	}

	// For all other types, use the existing conversion
	arrowType, nullable := sqlTypeToArrow(colType)

	// Build metadata with original SQL type information
	metadataMap := map[string]string{
		"sql.database_type_name": colType.DatabaseTypeName(),
		"sql.column_name":        colType.Name(),
	}

	// Add additional metadata if available
	if length, ok := colType.Length(); ok {
		metadataMap["sql.length"] = fmt.Sprintf("%d", length)
	}

	if precision, scale, ok := colType.DecimalSize(); ok {
		metadataMap["sql.precision"] = fmt.Sprintf("%d", precision)
		metadataMap["sql.scale"] = fmt.Sprintf("%d", scale)
	}

	// Create metadata with all collected information
	metadata := arrow.MetadataFrom(metadataMap)

	return arrowType, nullable, metadata, nil
}

// sqlTypeToArrow converts SQL column type to Arrow data type
func sqlTypeToArrow(colType *sql.ColumnType) (arrow.DataType, bool) {
	nullable, _ := colType.Nullable()

	// Get the database type name (e.g., "VARCHAR", "INT", etc.)
	typeName := strings.ToUpper(colType.DatabaseTypeName())

	switch typeName {
	// Integer types
	case "INT", "INTEGER", "MEDIUMINT":
		return arrow.PrimitiveTypes.Int32, nullable
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nullable
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nullable
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nullable

	// Unsigned integer types
	case "INT UNSIGNED", "INTEGER UNSIGNED", "MEDIUMINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint32, nullable
	case "BIGINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint64, nullable
	case "SMALLINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint16, nullable
	case "TINYINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint8, nullable

	// Floating point types
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nullable
	case "DOUBLE", "DOUBLE PRECISION":
		return arrow.PrimitiveTypes.Float64, nullable

	// String types
	case "CHAR", "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT":
		return arrow.BinaryTypes.String, nullable

	// Binary types
	case "BINARY", "VARBINARY", "BLOB", "MEDIUMBLOB", "LONGBLOB", "TINYBLOB":
		return arrow.BinaryTypes.Binary, nullable

	// Date/time types
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nullable

	// Boolean type
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean, nullable

	// JSON type
	case "JSON":
		jsonType, _ := extensions.NewJSONType(arrow.BinaryTypes.String)
		return jsonType, nullable

	// Default to string for unknown types
	default:
		return arrow.BinaryTypes.String, nullable
	}
}
