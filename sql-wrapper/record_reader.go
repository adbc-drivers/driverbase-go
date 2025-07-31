package sqlwrapper

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// unwrapNullable unwraps SQL nullable types and returns the underlying value and validity.
// Returns (value, isNull) where isNull indicates if the value was NULL in the database.
func unwrapNullable(val interface{}) (interface{}, bool) {
	switch v := val.(type) {
	case sql.NullBool:
		if !v.Valid {
			return nil, true
		}
		return v.Bool, false
	case sql.NullByte:
		if !v.Valid {
			return nil, true
		}
		return v.Byte, false
	case sql.NullFloat64:
		if !v.Valid {
			return nil, true
		}
		return v.Float64, false
	case sql.NullInt16:
		if !v.Valid {
			return nil, true
		}
		return v.Int16, false
	case sql.NullInt32:
		if !v.Valid {
			return nil, true
		}
		return v.Int32, false
	case sql.NullInt64:
		if !v.Valid {
			return nil, true
		}
		return v.Int64, false
	case sql.NullString:
		if !v.Valid {
			return nil, true
		}
		return v.String, false
	case sql.NullTime:
		if !v.Valid {
			return nil, true
		}
		return v.Time, false
	default:
		// Not a nullable type, return as-is
		return val, false
	}
}

// appendValue is the unified value appender that handles all Arrow builder types.
// It unwraps SQL nullable types and converts values to the appropriate Arrow format.
func appendValue(builder array.Builder, val interface{}) error {
	// Handle SQL nullable types first
	if unwrapped, isNull := unwrapNullable(val); isNull {
		builder.AppendNull()
		return nil
	} else {
		val = unwrapped
	}

	// Handle different builder types with unified type switching
	switch b := builder.(type) {
	// Integer types
	case *array.Int8Builder:
		switch v := val.(type) {
		case int8:
			b.Append(v)
		case int, int16, int32, int64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int8:
			b.Append(int16(v))
		case int, int32, int64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Int32Builder:
		switch v := val.(type) {
		case int32:
			b.Append(v)
		case int8:
			b.Append(int32(v))
		case int16:
			b.Append(int32(v))
		case int:
			b.Append(int32(v))
		case int64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int8:
			b.Append(int64(v))
		case int16:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Unsigned integer types
	case *array.Uint8Builder:
		switch v := val.(type) {
		case uint8:
			b.Append(v)
		case uint, uint16, uint32, uint64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Uint16Builder:
		switch v := val.(type) {
		case uint16:
			b.Append(v)
		case uint8:
			b.Append(uint16(v))
		case uint, uint32, uint64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Uint32Builder:
		switch v := val.(type) {
		case uint32:
			b.Append(v)
		case uint8:
			b.Append(uint32(v))
		case uint16:
			b.Append(uint32(v))
		case uint:
			b.Append(uint32(v))
		case uint64:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Uint64Builder:
		switch v := val.(type) {
		case uint64:
			b.Append(v)
		case uint8:
			b.Append(uint64(v))
		case uint16:
			b.Append(uint64(v))
		case uint32:
			b.Append(uint64(v))
		case uint:
			b.Append(uint64(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Floating point types
	case *array.Float32Builder:
		switch v := val.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Boolean type
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// String types
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.LargeStringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.StringViewBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Binary types
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.BinaryViewBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.FixedSizeBinaryBuilder:
		if v, ok := val.([]byte); ok {
			b.Append(v)
		} else {
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Date types
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			// Date32 stores days since epoch
			days := int32(v.Unix() / (24 * 3600))
			b.Append(arrow.Date32(days))
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Date64Builder:
		switch v := val.(type) {
		case time.Time:
			// Date64 stores milliseconds since epoch
			ms := v.UnixMilli()
			b.Append(arrow.Date64(ms))
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Time types
	case *array.Time32Builder:
		switch v := val.(type) {
		case time.Time:
			// Convert to time since midnight based on unit
			timeType := b.Type().(*arrow.Time32Type)
			switch timeType.Unit {
			case arrow.Second:
				seconds := int32(v.Hour()*3600 + v.Minute()*60 + v.Second())
				b.Append(arrow.Time32(seconds))
			case arrow.Millisecond:
				ms := int32(v.Hour()*3600000 + v.Minute()*60000 + v.Second()*1000 + v.Nanosecond()/1000000)
				b.Append(arrow.Time32(ms))
			}
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Time64Builder:
		switch v := val.(type) {
		case time.Time:
			// Convert to time since midnight based on unit
			timeType := b.Type().(*arrow.Time64Type)
			switch timeType.Unit {
			case arrow.Microsecond:
				us := int64(v.Hour()*3600000000 + v.Minute()*60000000 + v.Second()*1000000 + v.Nanosecond()/1000)
				b.Append(arrow.Time64(us))
			case arrow.Nanosecond:
				ns := int64(v.Hour()*3600000000000 + v.Minute()*60000000000 + v.Second()*1000000000 + v.Nanosecond())
				b.Append(arrow.Time64(ns))
			}
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Timestamp types
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			timestampType := b.Type().(*arrow.TimestampType)
			switch timestampType.Unit {
			case arrow.Second:
				b.Append(arrow.Timestamp(v.Unix()))
			case arrow.Millisecond:
				b.Append(arrow.Timestamp(v.UnixMilli()))
			case arrow.Microsecond:
				b.Append(arrow.Timestamp(v.UnixMicro()))
			case arrow.Nanosecond:
				b.Append(arrow.Timestamp(v.UnixNano()))
			}
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Decimal types
	case *array.Decimal128Builder:
		switch v := val.(type) {
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Decimal256Builder:
		switch v := val.(type) {
		case []byte:
			return b.AppendValueFromString(string(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Fallback for any unhandled builder types
	default:
		if stringAppender, ok := builder.(interface{ AppendValueFromString(string) error }); ok {
			return stringAppender.AppendValueFromString(fmt.Sprintf("%v", val))
		}
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// sqlRecordReaderImpl implements RecordReaderImpl interface for SQL result sets.
// This is the row-wise implementation that BaseRecordReader converts to batch-wise.
type sqlRecordReaderImpl struct {
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	values      []interface{}
	valuePtrs   []interface{}
	schema      *arrow.Schema
	builders    []array.Builder // Array builders for each column
}

// NewSQLRecordReader creates a RecordReader using driverbase.BaseRecordReader for streaming SQL results.
// It wraps the row-wise sqlRecordReaderImpl with BaseRecordReader to provide batch processing.
func NewSQLRecordReader(ctx context.Context, mem memory.Allocator, rows *sql.Rows, schema *arrow.Schema, columnTypes []*sql.ColumnType, batchSize int64) (array.RecordReader, error) {
	impl := &sqlRecordReaderImpl{
		rows:        rows,
		columnTypes: columnTypes,
		values:      make([]interface{}, len(columnTypes)),
		valuePtrs:   make([]interface{}, len(columnTypes)),
		schema:      schema,
	}

	// Create pointers to the values for Scan
	for i := range impl.values {
		impl.valuePtrs[i] = &impl.values[i]
	}

	// Initialize BaseRecordReader with our implementation
	reader := &driverbase.BaseRecordReader{}
	if err := reader.Init(ctx, mem, nil, batchSize, impl); err != nil {
		rows.Close()
		return nil, err
	}
	return reader, nil
}

// NextResultSet returns the Arrow schema for the current result set.
// For SQL queries, there's typically only one result set with a predetermined schema.
// The rec and rowIdx parameters are for bind parameter support (usually nil/0 for simple queries).
func (s *sqlRecordReaderImpl) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	// For SQL queries, we have a single result set with a known schema
	return s.schema, nil
}

// BeginAppending prepares for appending rows by initializing the column builders.
// This is called once before the first AppendRow call to set up the builders.
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	// Grab each builder for the columns from the RecordBuilder
	s.builders = make([]array.Builder, len(s.columnTypes))
	for i := range s.columnTypes {
		s.builders[i] = builder.Field(i)
	}
	return nil
}

// AppendRow reads one row from the SQL result set and appends it to the Arrow record builder.
// Returns io.EOF when no more rows are available in the current result set.
func (s *sqlRecordReaderImpl) AppendRow(builder *array.RecordBuilder) error {
	// Try to advance to the next row
	if !s.rows.Next() {
		// Check for SQL errors first
		if err := s.rows.Err(); err != nil {
			return err
		}
		// No more rows available
		return io.EOF
	}

	// Scan the current row values into our holders
	if err := s.rows.Scan(s.valuePtrs...); err != nil {
		return err
	}

	// Append each column value to its corresponding Arrow builder
	for i, b := range s.builders {
		if s.values[i] == nil {
			// Handle SQL NULL values
			b.AppendNull()
		} else {
			// Use the unified appendValue function to handle type conversion
			if err := appendValue(b, s.values[i]); err != nil {
				return fmt.Errorf("failed to append value to column %d: %w", i, err)
			}
		}
	}

	return nil
}

// Close closes the underlying SQL rows and releases resources.
func (s *sqlRecordReaderImpl) Close() error {
	if s.rows != nil {
		return s.rows.Close()
	}
	return nil
}