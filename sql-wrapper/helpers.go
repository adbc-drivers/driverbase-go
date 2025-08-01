package sqlwrapper

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// extractArrowValue extracts a value from an Arrow array at the given index
func extractArrowValue(arr arrow.Array, index int) (interface{}, error) {
	if arr.IsNull(index) {
		return nil, nil
	}

	// Direct array value extraction without scalars for optimal performance
	switch a := arr.(type) {
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

	// Date/Time types - convert to time.Time
	case *array.Date32:
		// Date32 stores days since epoch
		days := a.Value(index)
		return time.Unix(int64(days)*24*3600, 0).UTC(), nil
	case *array.Date64:
		// Date64 stores milliseconds since epoch
		millis := a.Value(index)
		return time.Unix(0, int64(millis)*int64(time.Millisecond)).UTC(), nil
	case *array.Time32:
		// Time32 - convert to time.Time (time of day)
		timeType := a.DataType().(*arrow.Time32Type)
		timeVal := a.Value(index)
		switch timeType.Unit {
		case arrow.Second:
			return time.Unix(int64(timeVal), 0).UTC(), nil
		case arrow.Millisecond:
			return time.Unix(0, int64(timeVal)*int64(time.Millisecond)).UTC(), nil
		default:
			return time.Unix(0, int64(timeVal)*int64(time.Millisecond)).UTC(), nil
		}
	case *array.Time64:
		// Time64 - convert to time.Time (time of day)
		timeType := a.DataType().(*arrow.Time64Type)
		timeVal := a.Value(index)
		switch timeType.Unit {
		case arrow.Microsecond:
			return time.Unix(0, int64(timeVal)*int64(time.Microsecond)).UTC(), nil
		case arrow.Nanosecond:
			return time.Unix(0, int64(timeVal)).UTC(), nil
		default:
			return time.Unix(0, int64(timeVal)*int64(time.Microsecond)).UTC(), nil
		}
	case *array.Timestamp:
		// Timestamp - convert to time.Time
		timestampType := a.DataType().(*arrow.TimestampType)
		timestampVal := a.Value(index)
		switch timestampType.Unit {
		case arrow.Second:
			return time.Unix(int64(timestampVal), 0).UTC(), nil
		case arrow.Millisecond:
			return time.Unix(0, int64(timestampVal)*int64(time.Millisecond)).UTC(), nil
		case arrow.Microsecond:
			return time.Unix(0, int64(timestampVal)*int64(time.Microsecond)).UTC(), nil
		case arrow.Nanosecond:
			return time.Unix(0, int64(timestampVal)).UTC(), nil
		default:
			return time.Unix(0, int64(timestampVal)*int64(time.Microsecond)).UTC(), nil
		}

	// Decimal types - use string representation
	case *array.Decimal32:
		return a.ValueStr(index), nil
	case *array.Decimal64:
		return a.ValueStr(index), nil
	case *array.Decimal128:
		return a.ValueStr(index), nil
	case *array.Decimal256:
		return a.ValueStr(index), nil

	// Fallback for any unhandled array types
	default:
		return nil, fmt.Errorf("unsupported Arrow array type: %T", arr)
	}
}

// buildArrowSchemaFromColumnTypes creates an Arrow schema from SQL column types using the type converter
func buildArrowSchemaFromColumnTypes(columnTypes []*sql.ColumnType, typeConverter TypeConverter) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable, metadata, err := typeConverter.ConvertColumnType(colType)
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