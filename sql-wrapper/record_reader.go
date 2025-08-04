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
		case int:
			b.Append(int8(v))
		case int16:
			b.Append(int8(v))
		case int32:
			b.Append(int8(v))
		case int64:
			b.Append(int8(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int8:
			b.Append(int16(v))
		case int:
			b.Append(int16(v))
		case int32:
			b.Append(int16(v))
		case int64:
			b.Append(int16(v))
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
			b.Append(int32(v))
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
		case uint:
			b.Append(uint8(v))
		case uint16:
			b.Append(uint8(v))
		case uint32:
			b.Append(uint8(v))
		case uint64:
			b.Append(uint8(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}
	case *array.Uint16Builder:
		switch v := val.(type) {
		case uint16:
			b.Append(v)
		case uint8:
			b.Append(uint16(v))
		case uint:
			b.Append(uint16(v))
		case uint32:
			b.Append(uint16(v))
		case uint64:
			b.Append(uint16(v))
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
			b.Append(uint32(v))
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
		case int:
			b.Append(float32(v))
		case int8:
			b.Append(float32(v))
		case int16:
			b.Append(float32(v))
		case int32:
			b.Append(float32(v))
		case int64:
			b.Append(float32(v))
		case uint:
			b.Append(float32(v))
		case uint8:
			b.Append(float32(v))
		case uint16:
			b.Append(float32(v))
		case uint32:
			b.Append(float32(v))
		case uint64:
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
		case int:
			b.Append(float64(v))
		case int8:
			b.Append(float64(v))
		case int16:
			b.Append(float64(v))
		case int32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		case uint:
			b.Append(float64(v))
		case uint8:
			b.Append(float64(v))
		case uint16:
			b.Append(float64(v))
		case uint32:
			b.Append(float64(v))
		case uint64:
			b.Append(float64(v))
		default:
			return b.AppendValueFromString(fmt.Sprintf("%v", val))
		}

	// Boolean type
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			b.Append(v)
		case int:
			b.Append(v != 0)
		case int8:
			b.Append(v != 0)
		case int16:
			b.Append(v != 0)
		case int32:
			b.Append(v != 0)
		case int64:
			b.Append(v != 0)
		case uint:
			b.Append(v != 0)
		case uint8:
			b.Append(v != 0)
		case uint16:
			b.Append(v != 0)
		case uint32:
			b.Append(v != 0)
		case uint64:
			b.Append(v != 0)
		default:
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
	// Current result set data
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	values      []interface{}
	valuePtrs   []interface{}
	schema      *arrow.Schema
	builders    []array.Builder // Array builders for each column

	// For bind parameter support
	conn          *sql.Conn     // Database connection to execute queries
	query         string        // Original SQL query with placeholders
	stmt          *sql.Stmt     // Prepared statement (optional)
	typeConverter TypeConverter // Type converter for building schemas
}

// NewSQLRecordReader creates a RecordReader using driverbase.BaseRecordReader for streaming SQL results.
// It wraps the row-wise sqlRecordReaderImpl with BaseRecordReader to provide batch processing.
// For bind parameter support, pass conn, query, stmt, and typeConverter. For simple queries, these can be nil.
// If bind parameters will be used later, all of conn, query, and typeConverter must be non-nil.
func NewSQLRecordReader(ctx context.Context, mem memory.Allocator, rows *sql.Rows, schema *arrow.Schema, columnTypes []*sql.ColumnType, batchSize int64, conn *sql.Conn, query string, stmt *sql.Stmt, typeConverter TypeConverter) (array.RecordReader, error) {
	// Validate that if any bind parameter components are provided, the essential ones are present
	hasBindSupport := conn != nil || query != "" || typeConverter != nil
	if hasBindSupport && (conn == nil || query == "" || typeConverter == nil) {
		return nil, fmt.Errorf("bind parameter support requires all of: connection, query, and type converter")
	}
	impl := &sqlRecordReaderImpl{
		rows:          rows,
		columnTypes:   columnTypes,
		schema:        schema,
		conn:          conn,
		query:         query,
		stmt:          stmt,
		typeConverter: typeConverter,
	}

	// Initialize value buffers for the initial schema
	impl.ensureValueBuffers(len(columnTypes))

	// Initialize BaseRecordReader with our implementation
	reader := &driverbase.BaseRecordReader{}
	if err := reader.Init(ctx, mem, nil, batchSize, impl); err != nil {
		rows.Close()
		return nil, err
	}
	return reader, nil
}

// NextResultSet returns the Arrow schema for the current result set.
// For SQL queries with bind parameters, this method executes the query with the
// specific parameter set (rec[rowIdx]) and returns the resulting schema.
func (s *sqlRecordReaderImpl) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	// Case 1: Simple queries without bind parameters
	if rec == nil {
		// For simple queries, we already have the result set and schema
		return s.schema, nil
	}

	// Case 2: Bind parameter queries - validate requirements
	if s.conn == nil || s.query == "" || s.typeConverter == nil {
		return nil, fmt.Errorf("bind parameter support requires connection, query, and type converter")
	}

	// Close any previous result set
	if s.rows != nil {
		s.rows.Close()
		s.rows = nil
	}

	// Extract parameters from the Arrow record for this row
	n := int(rec.NumCols())
	args := make([]interface{}, n)
	for i := 0; i < n; i++ {
		v, err := extractArrowValue(rec.Column(i), rowIdx)
		if err != nil {
			return nil, fmt.Errorf("failed to extract parameter %d: %w", i, err)
		}
		args[i] = v
	}

	// Execute the query with bind parameters
	var rows *sql.Rows
	var err error
	if s.stmt != nil {
		rows, err = s.stmt.QueryContext(ctx, args...)
	} else {
		rows, err = s.conn.QueryContext(ctx, s.query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute query with bind parameters: %w", err)
	}

	// Get column type information for the new result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build Arrow schema from column types
	schema, err := buildArrowSchemaFromColumnTypes(columnTypes, s.typeConverter)
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to build Arrow schema: %w", err)
	}

	// Update implementation state for new result set
	s.rows = rows
	s.columnTypes = columnTypes
	s.schema = schema
	s.ensureValueBuffers(len(columnTypes))

	return s.schema, nil
}

// ensureValueBuffers ensures the value buffers are sized correctly for the given column count
func (s *sqlRecordReaderImpl) ensureValueBuffers(numCols int) {
	// Only reallocate if size has changed
	if len(s.values) != numCols {
		s.values = make([]interface{}, numCols)
		s.valuePtrs = make([]interface{}, numCols)
	}

	// Always update pointers since values array might be reallocated
	for i := range s.values {
		s.valuePtrs[i] = &s.values[i]
	}
}

// BeginAppending prepares for appending rows by initializing the column builders.
// This is called once before the first AppendRow call to set up the builders.
// Note: BaseRecordReader may need to call this again after schema changes in NextResultSet.
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	// Ensure we have the right number of builders for the current schema
	numCols := len(s.columnTypes)
	if len(s.builders) != numCols {
		s.builders = make([]array.Builder, numCols)
	}

	// Grab each builder for the columns from the RecordBuilder
	for i := 0; i < numCols; i++ {
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
	var err error

	// Close the current result set
	if s.rows != nil {
		if closeErr := s.rows.Close(); closeErr != nil {
			err = closeErr
		}
		s.rows = nil
	}

	// Do not close the prepared statement - it is owned by the calling code (statementImpl)
	// and will be closed by statementImpl.Close()

	return err
}
