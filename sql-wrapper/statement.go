package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Custom option keys for the sql-wrapper
const (
	// OptionKeyBatchSize controls how many Arrow records to accumulate before executing as a batch
	OptionKeyBatchSize = "adbc.statement.batch_size"
	// OptionKeyTypeConverter sets the type converter for SQL-to-Arrow conversion
	OptionKeyTypeConverter = "adbc.connection.type_converter"
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
var typeConverterRegistry = map[string]func() TypeConverter{
	"default": func() TypeConverter { return &DefaultTypeConverter{} },
}

// RegisterTypeConverter registers a type converter factory function with a name
func RegisterTypeConverter(name string, factory func() TypeConverter) {
	typeConverterRegistry[name] = factory
}

// GetTypeConverter retrieves a type converter by name
func GetTypeConverter(name string) (TypeConverter, bool) {
	factory, exists := typeConverterRegistry[name]
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
			keys := []string{"sql.database_type_name", "sql.column_name", "sql.precision", "sql.scale"}
			values := []string{colType.DatabaseTypeName(), colType.Name(), fmt.Sprintf("%d", precision), fmt.Sprintf("%d", scale)}
			metadata := arrow.NewMetadata(keys, values)

			return arrowType, nullable, metadata, nil
		}
		// Fall back to string if precision/scale not available
	}

	// Handle DATETIME/TIMESTAMP types with proper precision
	if typeName == "DATETIME" || typeName == "TIMESTAMP" {
		// Try to get precision from DecimalSize (which represents fractional seconds precision)
		var timestampType arrow.DataType
		keys := []string{"sql.database_type_name", "sql.column_name"}
		values := []string{colType.DatabaseTypeName(), colType.Name()}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-6)
			keys = append(keys, "sql.fractional_seconds_precision")
			values = append(values, fmt.Sprintf("%d", precision))

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
			default:
				// Fallback to microseconds for unexpected values
				timestampType = arrow.FixedWidthTypes.Timestamp_us
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = arrow.FixedWidthTypes.Timestamp_us
		}

		metadata := arrow.NewMetadata(keys, values)
		return timestampType, nullable, metadata, nil
	}

	// Handle TIME types with proper precision
	if typeName == "TIME" {
		// Try to get precision from DecimalSize (which represents fractional seconds precision)
		var timeType arrow.DataType
		keys := []string{"sql.database_type_name", "sql.column_name"}
		values := []string{colType.DatabaseTypeName(), colType.Name()}

		if precision, _, ok := colType.DecimalSize(); ok {
			// precision represents fractional seconds digits (0-6)
			keys = append(keys, "sql.fractional_seconds_precision")
			values = append(values, fmt.Sprintf("%d", precision))

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
			default:
				// Fallback to microseconds for unexpected values
				timeType = arrow.FixedWidthTypes.Time64us
			}
		} else {
			// No precision info available, default to microseconds (most common)
			timeType = arrow.FixedWidthTypes.Time64us
		}

		metadata := arrow.NewMetadata(keys, values)
		return timeType, nullable, metadata, nil
	}

	// For all other types, use the existing conversion
	arrowType, nullable := sqlTypeToArrow(colType)

	// Build metadata with original SQL type information
	keys := []string{"sql.database_type_name", "sql.column_name"}
	values := []string{colType.DatabaseTypeName(), colType.Name()}

	// Add additional metadata if available
	if length, ok := colType.Length(); ok {
		keys = append(keys, "sql.length")
		values = append(values, fmt.Sprintf("%d", length))
	}

	if precision, scale, ok := colType.DecimalSize(); ok {
		keys = append(keys, "sql.precision", "sql.scale")
		values = append(values, fmt.Sprintf("%d", precision), fmt.Sprintf("%d", scale))
	}

	// Create metadata with all collected information
	metadata := arrow.NewMetadata(keys, values)

	return arrowType, nullable, metadata, nil
}

// statementImpl implements the ADBC Statement interface on top of database/sql.
type statementImpl struct {
	driverbase.StatementImplBase

	// conn is the dedicated SQL connection
	conn *sql.Conn
	// query holds the SQL to execute
	query string
	// stmt holds the prepared statement, if Prepare() was called
	stmt *sql.Stmt
	// boundRecord holds the bound Arrow record for bulk operations
	boundRecord arrow.Record
	// boundParams holds converted parameters for batch execution
	boundParams [][]interface{}
	// boundStream holds the bound Arrow record stream for streaming bulk operations
	boundStream array.RecordReader
	// batchSize controls how many records to process at once during streaming execution
	batchSize int
	// typeConverter handles SQL-to-Arrow type conversion
	typeConverter TypeConverter
}

// Base returns the embedded StatementImplBase for driverbase plumbing
func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

// newStatement constructs a new statementImpl wrapped by driverbase
func newStatement(c *connectionImpl) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ConnectionImplBase.ErrorHelper)
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: base,
		conn:              c.conn,
		batchSize:         1000, // Default batch size for streaming operations
		typeConverter:     c.typeConverter,
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *statementImpl) SetSqlQuery(query string) error {
	// if someone resets the SQL after Prepare, clean up the old stmt
	if s.stmt != nil {
		s.stmt.Close()
		s.stmt = nil
	}
	s.query = query
	return nil
}

// GetQuery returns the stored SQL text
func (s *statementImpl) GetQuery() string {
	return s.query
}

// SetOption sets a string option on this statement
func (s *statementImpl) SetOption(key, val string) error {
	switch key {
	case OptionKeyBatchSize:
		size, err := strconv.Atoi(val)
		if err != nil {
			return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "invalid batch size: %v", err)
		}
		return s.SetBatchSize(size)
	default:
		return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "unsupported option: %s", key)
	}
}

// Bind uses an arrow record batch to bind parameters to the query
func (s *statementImpl) Bind(ctx context.Context, record arrow.Record) error {
	if record == nil {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "record cannot be nil")
	}

	// Release any previous bound record
	if s.boundRecord != nil {
		s.boundRecord.Release()
		s.boundRecord = nil
	}
	s.boundParams = nil

	// Convert Arrow record to SQL parameters
	params, err := s.convertArrowRecordToParams(record)
	if err != nil {
		return err
	}

	s.boundParams = params
	return nil
}

// BindStream uses a record batch stream to bind parameters for bulk operations
func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if stream == nil {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "stream cannot be nil")
	}

	// Clear any previous bound parameters and records
	if s.boundRecord != nil {
		s.boundRecord.Release()
		s.boundRecord = nil
	}
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}
	s.boundParams = nil

	// Store the stream for lazy consumption during execution
	stream.Retain()
	s.boundStream = stream

	return nil
}

// ExecuteUpdate runs DML/DDL and returns rows affected
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	// If we have a bound stream, execute it with streaming batches
	if s.boundStream != nil {
		return s.executeStreamBatch(ctx)
	}

	// If we have bound parameters, execute them as a batch
	if s.boundParams != nil {
		return s.executeBatch(ctx)
	}

	// Nothing to execute if neither prepared stmt nor raw SQL is set
	if s.stmt == nil && s.query == "" {
		return 0, s.Base().ErrorHelper.Errorf(
			adbc.StatusInvalidArgument,
			"no SQL statement provided",
		)
	}

	// Regular execution without parameters
	var res sql.Result
	var err error

	if s.stmt != nil {
		res, err = s.stmt.ExecContext(ctx)
	} else {
		res, err = s.conn.ExecContext(ctx, s.query)
	}
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ExecuteSchema returns the Arrow schema by querying zero rows
func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if s.query == "" {
		return nil, s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query set")
	}

	// Execute query with LIMIT 0 to get schema without data
	limitQuery := fmt.Sprintf("SELECT * FROM (%s) AS subquery LIMIT 0", s.query)

	var rows *sql.Rows
	var err error

	// Can't use prepared statement with modified query, fall back to direct execution
	rows, err = s.conn.QueryContext(ctx, limitQuery)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column type information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert SQL column types to Arrow schema
	return s.buildArrowSchemaFromColumnTypes(columnTypes)
}

// buildArrowSchemaFromColumnTypes creates an Arrow schema from SQL column types using the type converter
func (s *statementImpl) buildArrowSchemaFromColumnTypes(columnTypes []*sql.ColumnType) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable, metadata, err := s.typeConverter.ConvertColumnType(colType)
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

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		err := s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query set")
		return nil, -1, err
	}

	// Execute the query
	var rows *sql.Rows
	var err error

	if s.stmt != nil {
		rows, err = s.stmt.QueryContext(ctx)
	} else {
		rows, err = s.conn.QueryContext(ctx, s.query)
	}

	if err != nil {
		return nil, -1, err
	}

	// Get column type information for schema
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, -1, err
	}

	// Build Arrow schema
	schema, err := s.buildArrowSchemaFromColumnTypes(columnTypes)
	if err != nil {
		rows.Close()
		return nil, -1, err
	}

	// Create a record reader that will stream the results
	reader := &sqlRecordReader{
		schema:      schema,
		rows:        rows,
		columnTypes: columnTypes,
		allocator:   memory.DefaultAllocator,
		batchSize:   s.batchSize,
		done:        false,
	}

	// Note: We return -1 for row count since we don't know without reading all rows
	return reader, -1, nil
}

// Close shuts down the prepared stmt (if any) and releases bound resources
func (s *statementImpl) Close() error {
	// Release bound record if any
	if s.boundRecord != nil {
		s.boundRecord.Release()
		s.boundRecord = nil
	}

	// Release bound stream if any
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Clear bound parameters
	s.boundParams = nil

	// Close prepared statement
	if s.stmt != nil {
		return s.stmt.Close()
	}
	return nil
}

// ExecutePartitions handles partitioned execution; not supported here
func (s *statementImpl) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	err := s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions not supported")
	return nil, adbc.Partitions{}, 0, err
}

// GetParameterSchema returns the schema for query parameters.
// Since database/sql doesn't provide parameter introspection, we return an empty schema.
func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	// Count parameter placeholders in the query
	paramCount := strings.Count(s.query, "?")

	if paramCount == 0 {
		// No parameters - return empty schema
		return arrow.NewSchema([]arrow.Field{}, nil), nil
	}

	// For queries with parameters, we can't determine types without execution
	// Return a schema indicating unknown parameter types
	fields := make([]arrow.Field, paramCount)
	for i := 0; i < paramCount; i++ {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("param_%d", i),
			Type:     arrow.BinaryTypes.String, // Default to string type
			Nullable: true,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (s *statementImpl) Prepare(ctx context.Context) (err error) {
	if s.query == "" {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query to prepare")
	}

	s.stmt, err = s.conn.PrepareContext(ctx, s.query)
	return
}

// SetBatchSize configures the batch size for streaming operations
func (s *statementImpl) SetBatchSize(size int) error {
	if size <= 0 {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "batch size must be positive")
	}
	s.batchSize = size
	return nil
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *statementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan not supported")
}

// sqlRecordReader implements array.RecordReader for streaming SQL results as Arrow records
type sqlRecordReader struct {
	schema        *arrow.Schema
	rows          *sql.Rows
	columnTypes   []*sql.ColumnType
	allocator     memory.Allocator
	batchSize     int
	done          bool
	err           error
	currentRecord arrow.Record
}

// Schema returns the Arrow schema for the result set
func (r *sqlRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Next reads the next batch of records and returns true if successful
func (r *sqlRecordReader) Next() bool {
	if r.done || r.err != nil {
		return false
	}

	// Release previous record
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Read a batch of rows using the configurable batch size
	// Use Arrow RecordBuilder to handle all column builders automatically
	recordBuilder := array.NewRecordBuilder(r.allocator, r.schema)
	defer recordBuilder.Release()

	rowCount := 0
	for rowCount < r.batchSize && r.rows.Next() {
		// Create a slice to hold the column values for this row
		values := make([]interface{}, len(r.columnTypes))
		valuePtrs := make([]interface{}, len(r.columnTypes))

		// Create pointers to the values for Scan
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := r.rows.Scan(valuePtrs...); err != nil {
			r.err = err
			return false
		}

		// Append values to the record builder
		for colIdx, val := range values {
			builder := recordBuilder.Field(colIdx)
			if val == nil {
				builder.AppendNull()
			} else {
				// Use Arrow's built-in scalar creation and appending
				if err := r.appendValue(builder, val); err != nil {
					r.err = fmt.Errorf("failed to append value to column %d: %w", colIdx, err)
					return false
				}
			}
		}

		rowCount++
	}

	if rowCount == 0 {
		r.done = true
		return false
	}

	// Check for errors after reading
	if err := r.rows.Err(); err != nil {
		r.err = err
		return false
	}

	// Build Arrow record from the record builder
	r.currentRecord = recordBuilder.NewRecord()

	// Check if we've reached the end
	if rowCount < r.batchSize {
		r.done = true
	}

	return true
}

// Record returns the current Arrow record batch
func (r *sqlRecordReader) Record() arrow.Record {
	return r.currentRecord
}

// Err returns any error that occurred during reading
func (r *sqlRecordReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Err()
}

// Release closes the reader and releases resources
func (r *sqlRecordReader) Release() {
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}
	if r.rows != nil {
		r.rows.Close()
		r.rows = nil
	}
}

// Retain is a no-op for this implementation (required by array.RecordReader interface)
func (r *sqlRecordReader) Retain() {
	// No-op: this reader doesn't use reference counting
}

// appendValue appends a value to an Arrow builder using Arrow's built-in scalar system
// This replaces the complex custom type handling with Arrow's MakeScalar + AppendScalar pattern
func (r *sqlRecordReader) appendValue(builder array.Builder, val interface{}) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}
	
	// Handle SQL nullable types via the driver.Valuer interface
	// All sql.NullXxx types implement driver.Valuer
	if valuer, ok := val.(driver.Valuer); ok {
		actualVal, err := valuer.Value()
		if err != nil {
			return fmt.Errorf("failed to get value from driver.Valuer: %w", err)
		}
		if actualVal == nil {
			builder.AppendNull()
			return nil
		}
		val = actualVal
	}
	
	// Special handling for []byte to string conversion for string builders
	// This preserves the existing behavior for MySQL CHAR/VARCHAR columns
	if bytes, ok := val.([]byte); ok {
		switch builder.(type) {
		case *array.StringBuilder, *array.LargeStringBuilder, *array.StringViewBuilder:
			val = string(bytes)
		}
	}
	
	// Use Arrow's built-in scalar creation and appending
	sc := scalar.MakeScalar(val)
	if sc == nil {
		// Fallback to string conversion for types Arrow doesn't recognize
		return builder.AppendValueFromString(fmt.Sprintf("%v", val))
	}
	
	// Try to append the scalar directly
	if scalarAppender, ok := builder.(interface{ AppendScalar(scalar.Scalar) error }); ok {
		return scalarAppender.AppendScalar(sc)
	}
	
	// Fallback: handle specific builder types for special cases
	switch b := builder.(type) {
	case *array.BinaryBuilder, *array.BinaryViewBuilder, *array.FixedSizeBinaryBuilder:
		// For binary builders, append raw bytes directly
		if bytes, ok := val.([]byte); ok {
			if bb, ok := b.(*array.BinaryBuilder); ok {
				bb.Append(bytes)
				return nil
			}
			if bvb, ok := b.(*array.BinaryViewBuilder); ok {
				bvb.Append(bytes)
				return nil
			}
			if fsbb, ok := b.(*array.FixedSizeBinaryBuilder); ok {
				fsbb.Append(bytes)
				return nil
			}
		}
		// Fall through to string conversion for non-byte values
	}
	
	// Final fallback: use string conversion
	return builder.AppendValueFromString(sc.String())
}

// convertArrowRecordToParams converts an Arrow record to SQL parameters
func (s *statementImpl) convertArrowRecordToParams(record arrow.Record) ([][]interface{}, error) {
	numRows, numCols := int(record.NumRows()), int(record.NumCols())

	if numRows == 0 {
		return nil, nil
	}

	// Create a slice of parameter rows
	params := make([][]interface{}, numRows)

	// Convert each row
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		rowParams := make([]interface{}, numCols)

		// Convert each column value for this row
		for colIdx := 0; colIdx < numCols; colIdx++ {
			arr := record.Column(colIdx)
			value, err := s.extractArrowValue(arr, rowIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to extract value at row %d, col %d: %w", rowIdx, colIdx, err)
			}
			rowParams[colIdx] = value
		}

		params[rowIdx] = rowParams
	}

	return params, nil
}

// extractArrowValue extracts a value from an Arrow array at the given index
// This now uses Arrow's built-in scalar system instead of manual type switching
func (s *statementImpl) extractArrowValue(arr arrow.Array, index int) (interface{}, error) {
	if arr.IsNull(index) {
		return nil, nil
	}
	
	// Use Arrow's built-in scalar extraction
	sc, err := scalar.GetScalar(arr, index)
	if err != nil {
		return nil, fmt.Errorf("failed to get scalar from array: %w", err)
	}
	
	// Convert the scalar to an appropriate Go value
	return s.scalarToGoValue(sc), nil
}

// scalarToGoValue converts an Arrow scalar to an appropriate Go value
// This handles the specific needs for SQL parameter binding
func (s *statementImpl) scalarToGoValue(sc scalar.Scalar) interface{} {
	if !sc.IsValid() {
		return nil
	}
	
	switch s := sc.(type) {
	// Primitive types
	case *scalar.Int8:
		return s.Value
	case *scalar.Int16:
		return s.Value
	case *scalar.Int32:
		return s.Value
	case *scalar.Int64:
		return s.Value
	case *scalar.Uint8:
		return s.Value
	case *scalar.Uint16:
		return s.Value
	case *scalar.Uint32:
		return s.Value
	case *scalar.Uint64:
		return s.Value
	case *scalar.Float32:
		return s.Value
	case *scalar.Float64:
		return s.Value
	case *scalar.Boolean:
		return s.Value
	
	// String and binary types
	case *scalar.String:
		return string(s.Value.Bytes())
	case *scalar.LargeString:
		return string(s.Value.Bytes())
	case *scalar.Binary:
		return s.Value.Bytes()
	case *scalar.LargeBinary:
		return s.Value.Bytes()
	case *scalar.FixedSizeBinary:
		return s.Value.Bytes()
	
	// Temporal types - convert to time.Time
	case *scalar.Date32:
		// Date32 stores days since epoch
		return time.Unix(int64(s.Value)*24*3600, 0).UTC()
	case *scalar.Date64:
		// Date64 stores milliseconds since epoch
		return time.Unix(0, int64(s.Value)*int64(time.Millisecond)).UTC()
	case *scalar.Time32:
		// Time32 - convert to time.Time (time of day)
		switch s.Type.(*arrow.Time32Type).Unit {
		case arrow.Second:
			return time.Unix(int64(s.Value), 0).UTC()
		case arrow.Millisecond:
			return time.Unix(0, int64(s.Value)*int64(time.Millisecond)).UTC()
		default:
			return time.Unix(0, int64(s.Value)*int64(time.Millisecond)).UTC()
		}
	case *scalar.Time64:
		// Time64 - convert to time.Time (time of day)
		switch s.Type.(*arrow.Time64Type).Unit {
		case arrow.Microsecond:
			return time.Unix(0, int64(s.Value)*int64(time.Microsecond)).UTC()
		case arrow.Nanosecond:
			return time.Unix(0, int64(s.Value)).UTC()
		default:
			return time.Unix(0, int64(s.Value)*int64(time.Microsecond)).UTC()
		}
	case *scalar.Timestamp:
		// Timestamp - convert to time.Time
		switch s.Type.(*arrow.TimestampType).Unit {
		case arrow.Second:
			return time.Unix(int64(s.Value), 0).UTC()
		case arrow.Millisecond:
			return time.Unix(0, int64(s.Value)*int64(time.Millisecond)).UTC()
		case arrow.Microsecond:
			return time.Unix(0, int64(s.Value)*int64(time.Microsecond)).UTC()
		case arrow.Nanosecond:
			return time.Unix(0, int64(s.Value)).UTC()
		default:
			return time.Unix(0, int64(s.Value)*int64(time.Microsecond)).UTC()
		}
	
	// Decimal types - use string representation
	case *scalar.Decimal128:
		return s.String()
	case *scalar.Decimal256:
		return s.String()
	
	// TODO: Add support for remaining scalar types in follow-up work:
	// - Dictionary scalars
	// - List/Map/Struct scalars  
	// - Extension type scalars
	// - Duration/Interval scalars
	//
	// Note: Arrow Go v18 only has Decimal128 and Decimal256 scalar types.
	// Decimal32 and Decimal64 arrays are handled by the fallback case since
	// their corresponding scalar types don't exist yet.
	//
	// StringView and BinaryView arrays are automatically handled by Arrow's
	// scalar.GetScalar() function. However, we need to ensure BinaryView returns []byte
	// to maintain functional equivalence with direct array.Value(index) calls.
	//
	// For now, fall back to string representation for other unsupported types
	
	default:
		// Handle BinaryView specially to return []byte (not string)
		// to maintain functional equivalence with array.BinaryView.Value(index)
		if sc.DataType().ID() == arrow.BINARY_VIEW {
			// For BinaryView, we want []byte, not string
			return []byte(sc.String())
		}
		
		// Fallback to string representation for any other unhandled scalar types
		// This correctly handles StringView and other view types
		return sc.String()
	}
}

// executeStreamBatch executes the statement with a bound stream, processing records in batches
func (s *statementImpl) executeStreamBatch(ctx context.Context) (int64, error) {
	if s.query == "" {
		return 0, s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query set")
	}

	// Prepare statement if needed
	var stmt *sql.Stmt
	var err error

	if s.stmt != nil {
		stmt = s.stmt
	} else {
		stmt, err = s.conn.PrepareContext(ctx, s.query)
		if err != nil {
			return 0, err
		}
		defer stmt.Close()
	}

	var totalAffected int64
	var accumulatedParams [][]interface{}
	recordsProcessed := 0

	// Helper function to execute accumulated parameters
	executeBatch := func() error {
		if len(accumulatedParams) == 0 {
			return nil
		}

		for _, rowParams := range accumulatedParams {
			result, err := stmt.ExecContext(ctx, rowParams...)
			if err != nil {
				return err
			}

			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}

			totalAffected += affected
		}

		// Clear accumulated parameters after execution
		accumulatedParams = nil
		return nil
	}

	// Process the stream, accumulating records up to batchSize
	for s.boundStream.Next() {
		record := s.boundStream.Record()
		if record == nil {
			continue
		}

		// Convert this record to parameters
		params, err := s.convertArrowRecordToParams(record)
		if err != nil {
			return totalAffected, err
		}

		// Accumulate parameters from this record
		accumulatedParams = append(accumulatedParams, params...)
		recordsProcessed++

		// Execute batch when we reach batchSize
		if recordsProcessed >= s.batchSize {
			if err := executeBatch(); err != nil {
				return totalAffected, err
			}
			recordsProcessed = 0
		}
	}

	// Execute any remaining accumulated parameters
	if err := executeBatch(); err != nil {
		return totalAffected, err
	}

	// Check for stream errors
	if err := s.boundStream.Err(); err != nil {
		return totalAffected, err
	}

	return totalAffected, nil
}

// executeBatch executes the statement with bound parameters as a batch
func (s *statementImpl) executeBatch(ctx context.Context) (int64, error) {
	if len(s.boundParams) == 0 {
		return 0, nil
	}

	if s.query == "" {
		return 0, s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query set")
	}

	// If we have a prepared statement, use it; otherwise prepare the query
	var stmt *sql.Stmt
	var err error

	if s.stmt != nil {
		stmt = s.stmt
	} else {
		stmt, err = s.conn.PrepareContext(ctx, s.query)
		if err != nil {
			return 0, err
		}
		defer stmt.Close()
	}

	// Execute each row of parameters
	var totalAffected int64

	for _, rowParams := range s.boundParams {
		result, err := stmt.ExecContext(ctx, rowParams...)
		if err != nil {
			return totalAffected, err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}

		totalAffected += affected
	}

	return totalAffected, nil
}
