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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"golang.org/x/exp/constraints"
)

const (
	MetaKeyDatabaseTypeName           = "sql.database_type_name"
	MetaKeyColumnName                 = "sql.column_name"
	MetaKeyPrecision                  = "sql.precision"
	MetaKeyScale                      = "sql.scale"
	MetaKeyFractionalSecondsPrecision = "sql.fractional_seconds_precision"
	MetaKeyLength                     = "sql.length"
)

type ColumnType struct {
	Name             string
	DatabaseTypeName string
	Nullable         bool
	Length           *int64
	Precision        *int64
	Scale            *int64
}

// TypeConverter allows higher-level drivers to customize SQL-to-Arrow type and value conversion
type TypeConverter interface {
	// ConvertColumnType converts a raw ColumnType (with metadata from strings or internal struct) to an Arrow type and nullable flag
	// It also returns metadata that should be included in the Arrow field.
	ConvertRawColumnType(colType ColumnType) (arrowType arrow.DataType, nullable bool, metadata arrow.Metadata, err error)

	// ConvertSQLToArrow converts a SQL value to the appropriate Go value for Arrow builders
	// The sqlValue comes from database/sql scanning, field contains the target Arrow type and metadata
	ConvertSQLToArrow(sqlValue any, field *arrow.Field) (any, error)

	// ConvertArrowToGo extracts a Go value from an Arrow array at the given index
	// This is used for parameter binding and value extraction
	// The field parameter provides access to the Arrow field metadata
	ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error)
}

// DefaultTypeConverter provides the default SQL-to-Arrow type conversion
type DefaultTypeConverter struct{}

// convertPrecisionToTimeUnit converts fractional seconds precision to Arrow TimeUnit
// Clamps precision to maximum supported value (9 fractional digits = nanoseconds)
func convertPrecisionToTimeUnit(precision int64) arrow.TimeUnit {
	if precision > 9 {
		// Clamp to max supported precision
		precision = 9
	}
	return arrow.TimeUnit(precision / 3)
}

// convertToNumericType converts a SQL value to the target numeric type T
func convertToNumericType[T constraints.Integer | constraints.Float](val any) (T, error) {
	switch v := val.(type) {
	case int:
		return T(v), nil
	case uint:
		return T(v), nil
	case int8:
		return T(v), nil
	case uint8:
		return T(v), nil
	case int16:
		return T(v), nil
	case uint16:
		return T(v), nil
	case int32:
		return T(v), nil
	case uint32:
		return T(v), nil
	case int64:
		return T(v), nil
	case uint64:
		return T(v), nil
	case float32:
		return T(v), nil
	case float64:
		return T(v), nil
	default:
		// Fallback to string parsing
		strVal := fmt.Sprintf("%v", val)
		var zero T
		switch any(zero).(type) {
		case int8, int16, int32, int64:
			parsed, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				return zero, fmt.Errorf("cannot convert %q to %T: %w", strVal, zero, err)
			}
			return T(parsed), nil
		case uint8, uint16, uint32, uint64:
			parsed, err := strconv.ParseUint(strVal, 10, 64)
			if err != nil {
				return zero, fmt.Errorf("cannot convert %q to %T: %w", strVal, zero, err)
			}
			return T(parsed), nil
		case float32, float64:
			parsed, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return zero, fmt.Errorf("cannot convert %q to %T: %w", strVal, zero, err)
			}
			return T(parsed), nil
		default:
			return zero, fmt.Errorf("unsupported numeric type conversion to %T", zero)
		}
	}
}

// convertToBool converts a SQL value to bool type
func convertToBool(val any) (bool, error) {
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
		// Use strconv.ParseBool for string parsing
		strVal := fmt.Sprintf("%v", val)
		boolVal, err := strconv.ParseBool(strVal)
		if err != nil {
			return false, fmt.Errorf("cannot convert %q to bool: %w", strVal, err)
		}
		return boolVal, nil
	}
}

// convertToString converts a SQL value to string type
func convertToString(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// convertToBinary converts a SQL value to []byte type
func convertToBinary(val any) ([]byte, error) {
	switch v := val.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return fmt.Appendf(nil, "%v", val), nil
	}
}

// convertToDate32 converts a SQL value to arrow.Date32 type
func convertToDate32(val any) (arrow.Date32, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Date32FromTime(v), nil
	case []byte:
		layout := "2006-01-02"
		t, err := time.Parse(layout, string(v))
		if err != nil {
			return 0, fmt.Errorf("cannot parse date from %v: %w", string(v), err)
		}
		return arrow.Date32FromTime(t), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to Date32, expected time.Time", val)
	}
}

// convertToDate64 converts a SQL value to arrow.Date64 type
func convertToDate64(val any) (arrow.Date64, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Date64FromTime(v), nil
	case []byte:
		// parse string from []byte
		return parseDateAndConvert(string(v))
	case string:
		return parseDateAndConvert(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to Date64, expected time.Time", val)
	}
}

// helper to parse string date and convert to arrow.Date64
func parseDateAndConvert(s string) (arrow.Date64, error) {
	// Use common date formats to parse
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z07:00",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return arrow.Date64FromTime(t), nil
		}
	}
	return 0, fmt.Errorf("cannot parse date string %q", s)
}

// convertToTime32 converts a SQL value to arrow.Time32 type
func convertToTime32(val any) (arrow.Time32, error) {
	switch v := val.(type) {
	case time.Time:
		// Convert to time since midnight - assume milliseconds for Time32
		return arrow.Time32(v.Hour()*3600000 + v.Minute()*60000 + v.Second()*1000 + v.Nanosecond()/1000000), nil
	case []byte:
		return arrow.Time32FromString(string(v), arrow.Millisecond)
	case string:
		return arrow.Time32FromString(v, arrow.Millisecond)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time32, expected time.Time", val)
	}
}

// convertToTime64 converts a SQL value to arrow.Time64 type
func convertToTime64(val any) (arrow.Time64, error) {
	switch v := val.(type) {
	case time.Time:
		// Convert to time since midnight - assume microseconds for Time64
		return arrow.Time64(v.Hour()*3600000000 + v.Minute()*60000000 + v.Second()*1000000 + v.Nanosecond()/1000), nil
	case []byte:
		return arrow.Time64FromString(string(v), arrow.Microsecond)
	case string:
		return arrow.Time64FromString(v, arrow.Microsecond)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time64, expected time.Time", val)
	}
}

// convertToTimestamp converts a SQL value to time.Time for TimestampBuilder.AppendTime
func convertToTimestamp(val any) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case []byte:
		ts, err := arrow.TimestampFromString(string(v), arrow.Microsecond)
		return ts.ToTime(arrow.Microsecond), err
	case string:
		ts, err := arrow.TimestampFromString(v, arrow.Microsecond)
		return ts.ToTime(arrow.Microsecond), err
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to timestamp, expected time.Time", val)
	}
}

// convertToDecimalString converts a SQL value to string for decimal AppendValueFromString
func convertToDecimalString(val any) (string, error) {
	switch v := val.(type) {
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

func ConvertColumnType(colType *sql.ColumnType, typeConverter TypeConverter) (arrow.DataType, bool, arrow.Metadata, error) {
	// Unpack sql.ColumnType to our ColumnType struct
	nullable, _ := colType.Nullable()

	var length, precision, scale *int64
	if l, ok := colType.Length(); ok {
		length = &l
	}
	if p, s, ok := colType.DecimalSize(); ok {
		precision = &p
		scale = &s
	}

	ourColType := ColumnType{
		Name:             colType.Name(),
		DatabaseTypeName: colType.DatabaseTypeName(),
		Nullable:         nullable,
		Length:           length,
		Precision:        precision,
		Scale:            scale,
	}

	return typeConverter.ConvertRawColumnType(ourColType)
}

// ConvertRawColumnType implements TypeConverter interface with the default conversion logic
func (d DefaultTypeConverter) ConvertRawColumnType(colType ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	switch typeName {
	case "DECIMAL", "NUMERIC":
		if colType.Precision != nil && colType.Scale != nil {
			precision := *colType.Precision
			scale := *colType.Scale
			if scale == 0 && precision <= 19 { // max digits for int64
				// Treat as integer type if precision fits in int64
				arrowType := arrow.PrimitiveTypes.Int64
				metadata := arrow.MetadataFrom(map[string]string{
					MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
					MetaKeyColumnName:       colType.Name,
					MetaKeyPrecision:        fmt.Sprintf("%d", precision),
					MetaKeyScale:            fmt.Sprintf("%d", scale),
				})
				return arrowType, nullable, metadata, nil
			}

			// Otherwise, create appropriate decimal type
			arrowType, err := arrow.NarrowestDecimalType(int32(precision), int32(scale))
			if err != nil {
				return nil, false, arrow.Metadata{}, fmt.Errorf("invalid decimal precision/scale (%d, %d): %w", precision, scale, err)
			}

			// Build metadata with decimal information
			metadata := arrow.MetadataFrom(map[string]string{
				MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
				MetaKeyColumnName:       colType.Name,
				MetaKeyPrecision:        fmt.Sprintf("%d", precision),
				MetaKeyScale:            fmt.Sprintf("%d", scale),
			})

			return arrowType, nullable, metadata, nil
		}
		// Fall back to string if precision/scale not available

	case "DATETIME", "TIMESTAMP":
		// Try to get precision from Precision field (which represents fractional seconds precision)
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			// precision represents fractional seconds digits (0-9)
			precision := *colType.Precision
			metadataMap[MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			timeUnit := convertPrecisionToTimeUnit(precision)
			timestampType = &arrow.TimestampType{Unit: timeUnit}
		} else {
			// No precision info available, default to microseconds (most common)
			// Use manual TimestampType creation to avoid automatic UTC timezone
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, nullable, metadata, nil

	case "TIME":
		// Try to get precision from Precision field (which represents fractional seconds precision)
		var timeType arrow.DataType
		metadataMap := map[string]string{
			MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			// precision represents fractional seconds digits (0-9)
			precision := *colType.Precision
			metadataMap[MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			timeUnit := convertPrecisionToTimeUnit(precision)

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
	arrowType := mapSQLTypeNameToArrowType(typeName)

	// Build metadata with original SQL type information
	metadataMap := map[string]string{
		MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
		MetaKeyColumnName:       colType.Name,
	}

	// Add additional metadata if available
	if colType.Length != nil {
		metadataMap[MetaKeyLength] = fmt.Sprintf("%d", *colType.Length)
	}

	if colType.Precision != nil && colType.Scale != nil {
		metadataMap[MetaKeyPrecision] = fmt.Sprintf("%d", *colType.Precision)
		metadataMap[MetaKeyScale] = fmt.Sprintf("%d", *colType.Scale)
	}

	// Create metadata with all collected information
	metadata := arrow.MetadataFrom(metadataMap)

	return arrowType, nullable, metadata, nil
}

// sqlTypeToArrow converts SQL type name to Arrow data type using pre-parsed values
func mapSQLTypeNameToArrowType(typeName string) arrow.DataType {
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
		return arrow.FixedWidthTypes.Date32

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
func (d DefaultTypeConverter) ConvertSQLToArrow(sqlValue any, field *arrow.Field) (any, error) {
	arrowType := field.Type
	// Handle SQL nullable types first
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return nil, fmt.Errorf("failed to unwrap value: %w", err)
	}
	if unwrapped == nil {
		return nil, nil // NULL value
	}

	// Convert based on the target Arrow type - doing all conversions here to simplify append logic
	switch arrowType.(type) {
	// Numeric types - handle conversion here instead of in appendValue
	case *arrow.Int8Type:
		return convertToNumericType[int8](unwrapped)
	case *arrow.Int16Type:
		return convertToNumericType[int16](unwrapped)
	case *arrow.Int32Type:
		return convertToNumericType[int32](unwrapped)
	case *arrow.Int64Type:
		return convertToNumericType[int64](unwrapped)
	case *arrow.Uint8Type:
		return convertToNumericType[uint8](unwrapped)
	case *arrow.Uint16Type:
		return convertToNumericType[uint16](unwrapped)
	case *arrow.Uint32Type:
		return convertToNumericType[uint32](unwrapped)
	case *arrow.Uint64Type:
		return convertToNumericType[uint64](unwrapped)
	case *arrow.Float32Type:
		return convertToNumericType[float32](unwrapped)
	case *arrow.Float64Type:
		return convertToNumericType[float64](unwrapped)

	// Boolean type
	case *arrow.BooleanType:
		return convertToBool(unwrapped)

	// String types
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return convertToString(unwrapped)

	// Binary types
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return convertToBinary(unwrapped)

	// Date types
	case *arrow.Date32Type:
		return convertToDate32(unwrapped)
	case *arrow.Date64Type:
		return convertToDate64(unwrapped)

	// Time types
	case *arrow.Time32Type:
		return convertToTime32(unwrapped)
	case *arrow.Time64Type:
		return convertToTime64(unwrapped)

	// Timestamp types
	case *arrow.TimestampType:
		return convertToTimestamp(unwrapped)

	// Decimal types - return as string for AppendValueFromString
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type, *arrow.Decimal256Type:
		return convertToDecimalString(unwrapped)

	// Default: return value as-is
	default:
		return unwrapped, nil
	}
}

// ConvertArrowToGo implements the default Arrow value to Go value conversion
func (d DefaultTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
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

// buildArrowSchemaFromColumnTypes creates an Arrow schema from SQL column types using the type converter
func buildArrowSchemaFromColumnTypes(columnTypes []*sql.ColumnType, typeConverter TypeConverter) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable, metadata, err := ConvertColumnType(colType, typeConverter)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}
	return arrow.NewSchema(fields, nil), nil
}
