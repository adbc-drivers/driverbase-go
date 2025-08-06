package sqlwrapper

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

const (
	MetaKeyDatabaseTypeName           = "sql.database_type_name"
	MetaKeyColumnName                 = "sql.column_name"
	MetaKeyPrecision                  = "sql.precision"
	MetaKeyScale                      = "sql.scale"
	MetaKeyFractionalSecondsPrecision = "sql.fractional_seconds_precision"
	MetaKeyLength                     = "sql.length"
)

// TypeConverter allows higher-level drivers to customize SQL-to-Arrow type and value conversion
type TypeConverter interface {
	// ConvertColumnType converts a SQL column type to an Arrow type and nullable flag
	// It also returns metadata that should be included in the Arrow field
	ConvertColumnType(colType *sql.ColumnType) (arrowType arrow.DataType, nullable bool, metadata arrow.Metadata, err error)

	// ConvertSQLToArrow converts a SQL value to the appropriate Go value for Arrow builders
	// The sqlValue comes from database/sql scanning, arrowType is the target Arrow type
	ConvertSQLToArrow(sqlValue any, arrowType arrow.DataType) (any, error)

	// ConvertArrowToGo extracts a Go value from an Arrow array at the given index
	// This is used for parameter binding and value extraction
	ConvertArrowToGo(arrowArray arrow.Array, index int) (any, error)
}

// DefaultTypeConverter provides the default SQL-to-Arrow type conversion
type DefaultTypeConverter struct{}

// ConvertColumnType implements TypeConverter interface with the default conversion logic
func (d *DefaultTypeConverter) ConvertColumnType(colType *sql.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName())
	nullable, _ := colType.Nullable()

	// Handle DECIMAL/NUMERIC types with proper precision and scale
	if typeName == "DECIMAL" || typeName == "NUMERIC" {
		if precision, scale, ok := colType.DecimalSize(); ok {
			if scale == 0 {
				// Treat as integer type if precision fits in int64
				if precision <= 19 { // max digits for int64
					arrowType := arrow.PrimitiveTypes.Int64
					metadata := arrow.MetadataFrom(map[string]string{
						MetaKeyDatabaseTypeName: colType.DatabaseTypeName(),
						MetaKeyColumnName:       colType.Name(),
						MetaKeyPrecision:        fmt.Sprintf("%d", precision),
						MetaKeyScale:            fmt.Sprintf("%d", scale),
					})
					return arrowType, nullable, metadata, nil
				}
				// If precision too large for int64, keep as decimal
			}

			// Otherwise, create Decimal128 type
			arrowType, err := arrow.NarrowestDecimalType(int32(precision), int32(scale))
			if err != nil {
				// Fallback to Decimal128Type
				arrowType = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
			}

			// Build metadata with decimal information
			metadata := arrow.MetadataFrom(map[string]string{
				MetaKeyDatabaseTypeName: colType.DatabaseTypeName(),
				MetaKeyColumnName:       colType.Name(),
				MetaKeyPrecision:        fmt.Sprintf("%d", precision),
				MetaKeyScale:            fmt.Sprintf("%d", scale),
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
			MetaKeyDatabaseTypeName: colType.DatabaseTypeName(),
			MetaKeyColumnName:       colType.Name(),
		}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-9)
			metadataMap[MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)

			if precision > 9 {
				// Clamp to max supported precision (9 fractional digits = nanoseconds)
				precision = 9
			}

			timeUnit := arrow.TimeUnit(precision / 3)
			timestampType = &arrow.TimestampType{Unit: timeUnit}
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
			MetaKeyDatabaseTypeName: colType.DatabaseTypeName(),
			MetaKeyColumnName:       colType.Name(),
		}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-9)
			metadataMap[MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)

			if precision > 9 {
				// Clamp to max supported precision
				precision = 9
			}

			timeUnit := arrow.TimeUnit(precision / 3)

			// Note: Arrow distinguishes between Time32 and Time64 based on unit:
			// Time32 supports seconds and milliseconds; Time64 supports microseconds and nanoseconds.
			if timeUnit == arrow.Second || timeUnit == arrow.Millisecond {
				timeType = &arrow.Time32Type{Unit: timeUnit}
			} else {
				timeType = &arrow.Time64Type{Unit: timeUnit}
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timeType = arrow.FixedWidthTypes.Time64us
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timeType, nullable, metadata, nil
	}

	// For all other types, use the existing conversion with already-parsed values
	arrowType := mapSQLTypeNameToArrowType(typeName, nullable)

	// Build metadata with original SQL type information
	metadataMap := map[string]string{
		MetaKeyDatabaseTypeName: colType.DatabaseTypeName(),
		MetaKeyColumnName:       colType.Name(),
	}

	// Add additional metadata if available
	if length, ok := colType.Length(); ok {
		metadataMap[MetaKeyLength] = fmt.Sprintf("%d", length)
	}

	if precision, scale, ok := colType.DecimalSize(); ok {
		metadataMap[MetaKeyPrecision] = fmt.Sprintf("%d", precision)
		metadataMap[MetaKeyScale] = fmt.Sprintf("%d", scale)
	}

	// Create metadata with all collected information
	metadata := arrow.MetadataFrom(metadataMap)

	return arrowType, nullable, metadata, nil
}

// sqlTypeToArrow converts SQL type name to Arrow data type using pre-parsed values
func mapSQLTypeNameToArrowType(typeName string, nullable bool) arrow.DataType {

	switch typeName {
	// Integer types
	case "INT", "INTEGER", "MEDIUMINT":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8

	// Unsigned integer types
	case "INT UNSIGNED", "INTEGER UNSIGNED", "MEDIUMINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint32
	case "BIGINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint64
	case "SMALLINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint16
	case "TINYINT UNSIGNED":
		return arrow.PrimitiveTypes.Uint8

	// Floating point types
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE", "DOUBLE PRECISION":
		return arrow.PrimitiveTypes.Float64

	// String types
	case "CHAR", "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT":
		return arrow.BinaryTypes.String

	// Binary types
	case "BINARY", "VARBINARY", "BLOB", "MEDIUMBLOB", "LONGBLOB", "TINYBLOB":
		return arrow.BinaryTypes.Binary

	// Date/time types
	case "DATE":
		return arrow.FixedWidthTypes.Date64

	// Boolean type
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean

	// JSON type
	case "JSON":
		jsonType, _ := extensions.NewJSONType(arrow.BinaryTypes.String)
		return jsonType

	// Default to string for unknown types
	default:
		// TODO (https://github.com/adbc-drivers/driverbase-go/issues/30):
		// Consider using the Opaque extension type instead of treating unknown types as strings.
		return arrow.BinaryTypes.String
	}
}

// unwrap unwraps SQL nullable types using the driver.Valuer interface.
// Returns the underlying value or nil if the value was NULL in the database.
func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// ConvertSQLToArrow implements the default SQL value to Arrow value conversion
func (d *DefaultTypeConverter) ConvertSQLToArrow(sqlValue any, arrowType arrow.DataType) (any, error) {
	// Handle SQL nullable types first
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return nil, fmt.Errorf("failed to unwrap value: %w", err)
	}
	if unwrapped == nil {
		return nil, nil // NULL value
	}
	val := unwrapped

	// Convert based on the target Arrow type - using the same logic as appendValue
	switch arrowType.(type) {
	// Numeric types
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type:
		// Return value as-is for numeric types, appendValue will handle the conversion
		return val, nil

	// Boolean type
	case *arrow.BooleanType:
		// Convert to boolean using the same logic as appendBooleanToBuilder
		switch v := val.(type) {
		case bool:
			return v, nil
		case int:
			return v != 0, nil
		case int8:
			return v != 0, nil
		case int16:
			return v != 0, nil
		case int32:
			return v != 0, nil
		case int64:
			return v != 0, nil
		case uint:
			return v != 0, nil
		case uint8:
			return v != 0, nil
		case uint16:
			return v != 0, nil
		case uint32:
			return v != 0, nil
		case uint64:
			return v != 0, nil
		default:
			return val, nil // Let appendValue handle string conversion
		}

	// String types
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", val), nil
		}

	// Binary types
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		switch v := val.(type) {
		case string:
			return []byte(v), nil
		case []byte:
			return v, nil
		default:
			return []byte(fmt.Sprintf("%v", val)), nil
		}

	// Date types - return as-is, let appendValue handle the conversion
	case *arrow.Date32Type, *arrow.Date64Type:
		return val, nil

	// Time types - return as-is, let appendValue handle the conversion
	case *arrow.Time32Type, *arrow.Time64Type:
		return val, nil

	// Timestamp types
	case *arrow.TimestampType:
		// For timestamp types, return the value as-is and let appendValue handle the conversion
		// This avoids precision issues and lets the builder handle the conversion properly
		return val, nil

	// Decimal types - return as-is, let appendValue handle the conversion
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		return val, nil

	// Default: return value as-is
	default:
		return val, nil
	}
}

// ConvertArrowToGo implements the default Arrow value to Go value conversion
func (d *DefaultTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Direct array value extraction without scalars for optimal performance
	switch a := arrowArray.(type) {
	// Integer types
	case *array.Int8:
		return a.Value(index), nil
	case *array.Int16:
		return a.Value(index), nil
	case *array.Int32:
		return a.Value(index), nil
	case *array.Int64:
		return a.Value(index), nil
	case *array.Uint8:
		return a.Value(index), nil
	case *array.Uint16:
		return a.Value(index), nil
	case *array.Uint32:
		return a.Value(index), nil
	case *array.Uint64:
		return a.Value(index), nil

	// Floating point types
	case *array.Float32:
		return a.Value(index), nil
	case *array.Float64:
		return a.Value(index), nil

	// Boolean type
	case *array.Boolean:
		return a.Value(index), nil

	// String types
	case *array.String:
		return a.Value(index), nil
	case *array.LargeString:
		return a.Value(index), nil
	case *array.StringView:
		return a.Value(index), nil

	// Binary types
	case *array.Binary:
		return a.Value(index), nil
	case *array.BinaryView:
		return a.Value(index), nil
	case *array.FixedSizeBinary:
		return a.Value(index), nil
	case *array.LargeBinary:
		return a.Value(index), nil

	// Date/Time types -  use Arrow's built-in ToTime() methods
	case *array.Date32:
		return a.Value(index).ToTime(), nil
	case *array.Date64:
		return a.Value(index).ToTime(), nil
	case *array.Time32:
		timeType := a.DataType().(*arrow.Time32Type)
		return a.Value(index).ToTime(timeType.Unit), nil
	case *array.Time64:
		timeType := a.DataType().(*arrow.Time64Type)
		return a.Value(index).ToTime(timeType.Unit), nil
	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		tz, err := timestampType.GetZone()
		if err != nil {
			return nil, err
		}

		return a.Value(index).ToTime(timestampType.Unit).In(tz), nil

	// Fallback for any unhandled array types (including Decimal types)
	default:
		return a.ValueStr(index), nil
	}
}
