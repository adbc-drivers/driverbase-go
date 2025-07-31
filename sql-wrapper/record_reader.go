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

// fieldInfo holds pre-computed information about each field to avoid redundant type checking
type fieldInfo struct {
	builder  array.Builder
	appender func(builder array.Builder, val interface{}) error
}

// sqlRecordReaderImpl implements RecordReaderImpl for SQL result sets
type sqlRecordReaderImpl struct {
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	values      []interface{}
	valuePtrs   []interface{}
	schema      *arrow.Schema
	fields      []fieldInfo // Pre-computed field information to avoid redundant type checking
}

// NewSQLRecordReader creates a RecordReader using driverbase.BaseRecordReader for streaming SQL results
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

	reader := &driverbase.BaseRecordReader{}
	if err := reader.Init(ctx, mem, nil, batchSize, impl); err != nil {
		rows.Close()
		return nil, err
	}
	return reader, nil
}

// NextResultSet opens the result set for reading
func (s *sqlRecordReaderImpl) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	// For SQL queries, there's only one result set and the schema is already determined
	return s.schema, nil
}

// BeginAppending prepares for appending rows by pre-computing field information
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	// Pre-compute field information to avoid redundant type checking during AppendRow
	s.fields = make([]fieldInfo, len(s.columnTypes))
	for i, field := range s.schema.Fields() {
		fieldBuilder := builder.Field(i)
		appender := createAppenderForType(field.Type)
		s.fields[i] = fieldInfo{
			builder:  fieldBuilder,
			appender: appender,
		}
	}
	return nil
}

// createAppenderForType creates an optimized appender function for a specific Arrow type
// This avoids redundant type checking for every row and column
func createAppenderForType(arrowType arrow.DataType) func(builder array.Builder, val interface{}) error {
	switch arrowType.(type) {
	// Integer types
	case *arrow.Int8Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToInt8Builder(builder.(*array.Int8Builder), val)
		}
	case *arrow.Int16Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToInt16Builder(builder.(*array.Int16Builder), val)
		}
	case *arrow.Int32Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToInt32Builder(builder.(*array.Int32Builder), val)
		}
	case *arrow.Int64Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToInt64Builder(builder.(*array.Int64Builder), val)
		}
	case *arrow.Uint8Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToUint8Builder(builder.(*array.Uint8Builder), val)
		}
	case *arrow.Uint16Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToUint16Builder(builder.(*array.Uint16Builder), val)
		}
	case *arrow.Uint32Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToUint32Builder(builder.(*array.Uint32Builder), val)
		}
	case *arrow.Uint64Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToUint64Builder(builder.(*array.Uint64Builder), val)
		}
	// Floating point types
	case *arrow.Float32Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToFloat32Builder(builder.(*array.Float32Builder), val)
		}
	case *arrow.Float64Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToFloat64Builder(builder.(*array.Float64Builder), val)
		}
	// Boolean type
	case *arrow.BooleanType:
		return func(builder array.Builder, val interface{}) error {
			return appendToBooleanBuilder(builder.(*array.BooleanBuilder), val)
		}
	// String types
	case *arrow.StringType:
		return func(builder array.Builder, val interface{}) error {
			return appendToStringBuilder(builder.(*array.StringBuilder), val)
		}
	case *arrow.LargeStringType:
		return func(builder array.Builder, val interface{}) error {
			return appendToLargeStringBuilder(builder.(*array.LargeStringBuilder), val)
		}
	case *arrow.StringViewType:
		return func(builder array.Builder, val interface{}) error {
			return appendToStringViewBuilder(builder.(*array.StringViewBuilder), val)
		}
	// Binary types
	case *arrow.BinaryType:
		return func(builder array.Builder, val interface{}) error {
			return appendToBinaryBuilder(builder.(*array.BinaryBuilder), val)
		}
	case *arrow.BinaryViewType:
		return func(builder array.Builder, val interface{}) error {
			return appendToBinaryViewBuilder(builder.(*array.BinaryViewBuilder), val)
		}
	case *arrow.FixedSizeBinaryType:
		return func(builder array.Builder, val interface{}) error {
			return appendToFixedSizeBinaryBuilder(builder.(*array.FixedSizeBinaryBuilder), val)
		}
	// Date/Time types
	case *arrow.Date32Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToDate32Builder(builder.(*array.Date32Builder), val)
		}
	case *arrow.Date64Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToDate64Builder(builder.(*array.Date64Builder), val)
		}
	case *arrow.Time32Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToTime32Builder(builder.(*array.Time32Builder), val)
		}
	case *arrow.Time64Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToTime64Builder(builder.(*array.Time64Builder), val)
		}
	case *arrow.TimestampType:
		return func(builder array.Builder, val interface{}) error {
			return appendToTimestampBuilder(builder.(*array.TimestampBuilder), val)
		}
	// Decimal types
	case *arrow.Decimal128Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToDecimal128Builder(builder.(*array.Decimal128Builder), val)
		}
	case *arrow.Decimal256Type:
		return func(builder array.Builder, val interface{}) error {
			return appendToDecimal256Builder(builder.(*array.Decimal256Builder), val)
		}
	// Fallback for unsupported types
	default:
		return func(builder array.Builder, val interface{}) error {
			if val == nil {
				builder.AppendNull()
				return nil
			}
			return builder.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	}
}

// AppendRow appends a single row from the SQL result set to the record builder
func (s *sqlRecordReaderImpl) AppendRow(builder *array.RecordBuilder) error {
	if !s.rows.Next() {
		// Check for SQL errors
		if err := s.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	// Scan the current row into our value holders
	if err := s.rows.Scan(s.valuePtrs...); err != nil {
		return err
	}

	// Append values to the record builder using pre-computed appenders
	for colIdx, val := range s.values {
		if val == nil {
			s.fields[colIdx].builder.AppendNull()
		} else {
			// Use pre-computed type-specific appender (no more redundant type checking!)
			if err := s.fields[colIdx].appender(s.fields[colIdx].builder, val); err != nil {
				return fmt.Errorf("failed to append value to column %d: %w", colIdx, err)
			}
		}
	}

	return nil
}

// Close closes the SQL rows
func (s *sqlRecordReaderImpl) Close() error {
	if s.rows != nil {
		return s.rows.Close()
	}
	return nil
}

// Type-specific appender functions to avoid redundant type checking
// These functions handle SQL nullable types and perform direct appending

func appendToInt8Builder(b *array.Int8Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullInt16:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int16
	case sql.NullInt32:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int32
	case sql.NullInt64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int64
	}
	
	// Direct type conversions
	if v, ok := val.(int8); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(int); ok {
		b.Append(int8(v))
		return nil
	}
	if v, ok := val.(int32); ok {
		b.Append(int8(v))
		return nil
	}
	if v, ok := val.(int64); ok {
		b.Append(int8(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToInt16Builder(b *array.Int16Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullInt16:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int16
	case sql.NullInt32:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int32
	case sql.NullInt64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int64
	}
	
	if v, ok := val.(int16); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(int); ok {
		b.Append(int16(v))
		return nil
	}
	if v, ok := val.(int32); ok {
		b.Append(int16(v))
		return nil
	}
	if v, ok := val.(int64); ok {
		b.Append(int16(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToInt32Builder(b *array.Int32Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullInt32:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int32
	case sql.NullInt64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int64
	case sql.NullInt16:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int16
	}
	
	if v, ok := val.(int32); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(int); ok {
		b.Append(int32(v))
		return nil
	}
	if v, ok := val.(int64); ok {
		b.Append(int32(v))
		return nil
	}
	if v, ok := val.(int16); ok {
		b.Append(int32(v))
		return nil
	}
	if v, ok := val.(int8); ok {
		b.Append(int32(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToInt64Builder(b *array.Int64Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullInt64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int64
	case sql.NullInt32:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int32
	case sql.NullInt16:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Int16
	}
	
	if v, ok := val.(int64); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(int); ok {
		b.Append(int64(v))
		return nil
	}
	if v, ok := val.(int32); ok {
		b.Append(int64(v))
		return nil
	}
	if v, ok := val.(int16); ok {
		b.Append(int64(v))
		return nil
	}
	if v, ok := val.(int8); ok {
		b.Append(int64(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToUint8Builder(b *array.Uint8Builder, val interface{}) error {
	// Handle SQL nullable byte type first
	switch v := val.(type) {
	case sql.NullByte:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Byte
	}
	
	if v, ok := val.(uint8); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(uint); ok {
		b.Append(uint8(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToUint16Builder(b *array.Uint16Builder, val interface{}) error {
	if v, ok := val.(uint16); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(uint); ok {
		b.Append(uint16(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToUint32Builder(b *array.Uint32Builder, val interface{}) error {
	if v, ok := val.(uint32); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(uint); ok {
		b.Append(uint32(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToUint64Builder(b *array.Uint64Builder, val interface{}) error {
	if v, ok := val.(uint64); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(uint); ok {
		b.Append(uint64(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToFloat32Builder(b *array.Float32Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullFloat64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Float64
	}
	
	if v, ok := val.(float32); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(float64); ok {
		b.Append(float32(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToFloat64Builder(b *array.Float64Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullFloat64:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Float64
	}
	
	if v, ok := val.(float64); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(float32); ok {
		b.Append(float64(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToBooleanBuilder(b *array.BooleanBuilder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullBool:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Bool
	}
	
	if v, ok := val.(bool); ok {
		b.Append(v)
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToStringBuilder(b *array.StringBuilder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullString:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.String
	}
	
	if v, ok := val.(string); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.([]byte); ok {
		b.Append(string(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToLargeStringBuilder(b *array.LargeStringBuilder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullString:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.String
	}
	
	if v, ok := val.(string); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.([]byte); ok {
		b.Append(string(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToStringViewBuilder(b *array.StringViewBuilder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullString:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.String
	}
	
	if v, ok := val.(string); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.([]byte); ok {
		b.Append(string(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToBinaryBuilder(b *array.BinaryBuilder, val interface{}) error {
	if v, ok := val.([]byte); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(string); ok {
		b.Append([]byte(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToBinaryViewBuilder(b *array.BinaryViewBuilder, val interface{}) error {
	if v, ok := val.([]byte); ok {
		b.Append(v)
		return nil
	}
	if v, ok := val.(string); ok {
		b.Append([]byte(v))
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToFixedSizeBinaryBuilder(b *array.FixedSizeBinaryBuilder, val interface{}) error {
	if v, ok := val.([]byte); ok {
		b.Append(v)
		return nil
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToDate32Builder(b *array.Date32Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullTime:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Time
	}
	
	if v, ok := val.(time.Time); ok {
		// Convert time.Time to days since epoch
		days := int32(v.Unix() / (24 * 3600))
		b.Append(arrow.Date32(days))
		return nil
	}
	// Handle byte slice dates that need string conversion
	if v, ok := val.([]byte); ok {
		return b.AppendValueFromString(string(v))
	}
	// Handle string date formats
	if v, ok := val.(string); ok {
		return b.AppendValueFromString(v)
	}
	// Final fallback
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToDate64Builder(b *array.Date64Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullTime:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Time
	}
	
	if v, ok := val.(time.Time); ok {
		// Convert time.Time to milliseconds since epoch
		millis := v.UnixMilli()
		b.Append(arrow.Date64(millis))
		return nil
	}
	// Handle byte slice dates that need string conversion
	if v, ok := val.([]byte); ok {
		return b.AppendValueFromString(string(v))
	}
	// Handle string date formats
	if v, ok := val.(string); ok {
		return b.AppendValueFromString(v)
	}
	// Final fallback
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToTime32Builder(b *array.Time32Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullTime:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Time
	}
	
	if v, ok := val.(time.Time); ok {
		// Extract time of day and convert based on unit
		timeType := b.Type().(*arrow.Time32Type)
		switch timeType.Unit {
		case arrow.Second:
			seconds := int32(v.Hour()*3600 + v.Minute()*60 + v.Second())
			b.Append(arrow.Time32(seconds))
		case arrow.Millisecond:
			millis := int32(v.Hour()*3600000 + v.Minute()*60000 + v.Second()*1000 + v.Nanosecond()/1000000)
			b.Append(arrow.Time32(millis))
		}
		return nil
	}
	// Handle byte slice times that need string conversion
	if v, ok := val.([]byte); ok {
		return b.AppendValueFromString(string(v))
	}
	// Handle string time formats
	if v, ok := val.(string); ok {
		return b.AppendValueFromString(v)
	}
	// Final fallback
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToTime64Builder(b *array.Time64Builder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullTime:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Time
	}
	
	if v, ok := val.(time.Time); ok {
		// Extract time of day and convert based on unit
		timeType := b.Type().(*arrow.Time64Type)
		switch timeType.Unit {
		case arrow.Microsecond:
			micros := int64(v.Hour()*3600000000 + v.Minute()*60000000 + v.Second()*1000000 + v.Nanosecond()/1000)
			b.Append(arrow.Time64(micros))
		case arrow.Nanosecond:
			nanos := int64(v.Hour()*3600000000000 + v.Minute()*60000000000 + v.Second()*1000000000 + v.Nanosecond())
			b.Append(arrow.Time64(nanos))
		}
		return nil
	}
	// Handle byte slice times that need string conversion
	if v, ok := val.([]byte); ok {
		return b.AppendValueFromString(string(v))
	}
	// Handle string time formats
	if v, ok := val.(string); ok {
		return b.AppendValueFromString(v)
	}
	// Final fallback
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToTimestampBuilder(b *array.TimestampBuilder, val interface{}) error {
	// Handle SQL nullable types first
	switch v := val.(type) {
	case sql.NullTime:
		if !v.Valid {
			b.AppendNull()
			return nil
		}
		val = v.Time
	}
	
	if v, ok := val.(time.Time); ok {
		// Convert time.Time to timestamp based on unit, but be careful about precision
		timestampType := b.Type().(*arrow.TimestampType)
		switch timestampType.Unit {
		case arrow.Second:
			// Truncate to seconds to avoid precision errors
			truncated := v.Truncate(time.Second)
			b.Append(arrow.Timestamp(truncated.Unix()))
		case arrow.Millisecond:
			b.Append(arrow.Timestamp(v.UnixMilli()))
		case arrow.Microsecond:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		case arrow.Nanosecond:
			b.Append(arrow.Timestamp(v.UnixNano()))
		}
		return nil
	}
	// Handle byte slice timestamps that need string conversion
	if v, ok := val.([]byte); ok {
		return b.AppendValueFromString(string(v))
	}
	// Handle string timestamp formats
	if v, ok := val.(string); ok {
		return b.AppendValueFromString(v)
	}
	// Final fallback
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToDecimal128Builder(b *array.Decimal128Builder, val interface{}) error {
	// Handle string representation
	if v, ok := val.(string); ok && v != "" {
		return b.AppendValueFromString(v)
	}
	// Handle numeric types by converting to string
	if v, ok := val.(float64); ok {
		return b.AppendValueFromString(fmt.Sprintf("%g", v))
	}
	if v, ok := val.(float32); ok {
		return b.AppendValueFromString(fmt.Sprintf("%g", v))
	}
	// Handle byte arrays that might contain decimal strings
	if v, ok := val.([]byte); ok && len(v) > 0 {
		return b.AppendValueFromString(string(v))
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}

func appendToDecimal256Builder(b *array.Decimal256Builder, val interface{}) error {
	// Handle string representation
	if v, ok := val.(string); ok && v != "" {
		return b.AppendValueFromString(v)
	}
	// Handle numeric types by converting to string
	if v, ok := val.(float64); ok {
		return b.AppendValueFromString(fmt.Sprintf("%g", v))
	}
	if v, ok := val.(float32); ok {
		return b.AppendValueFromString(fmt.Sprintf("%g", v))
	}
	// Handle byte arrays that might contain decimal strings
	if v, ok := val.([]byte); ok && len(v) > 0 {
		return b.AppendValueFromString(string(v))
	}
	return b.AppendValueFromString(fmt.Sprintf("%v", val))
}