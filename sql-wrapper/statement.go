package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Custom option keys for the sql-wrapper
const (
	// OptionKeyBatchSize controls how many Arrow records to accumulate in a record batch
	OptionKeyBatchSize = "adbc.statement.batch_size"
)

// statementImpl implements the ADBC Statement interface on top of database/sql.
type statementImpl struct {
	driverbase.StatementImplBase

	// conn is the dedicated SQL connection
	conn *sql.Conn
	// query holds the SQL to execute
	query string
	// stmt holds the prepared statement, if Prepare() was called
	stmt *sql.Stmt
	// boundStream holds the bound Arrow record stream for bulk operations
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
			return s.Base().ErrorHelper.InvalidArgument("invalid batch size: %v", err)
		}
		return s.SetBatchSize(size)
	default:
		return s.Base().ErrorHelper.NotImplemented("unsupported option: %s", key)
	}
}

// Bind uses an arrow record batch to bind parameters to the query
func (s *statementImpl) Bind(ctx context.Context, record arrow.Record) error {
	if record == nil {
		return s.Base().ErrorHelper.InvalidArgument("record cannot be nil")
	}

	// Release any previous bound stream
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Convert single record to a RecordReader using Arrow's built-in function
	s.boundStream, _ = array.NewRecordReader(record.Schema(), []arrow.Record{record})
	return nil
}

// BindStream uses a record batch stream to bind parameters for bulk operations
func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if stream == nil {
		return s.Base().ErrorHelper.InvalidArgument("stream cannot be nil")
	}

	// Release any previous bound stream
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Store the stream for lazy consumption during execution
	stream.Retain()
	s.boundStream = stream

	return nil
}

// ExecuteUpdate runs DML/DDL and returns rows affected
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	// If we have a bound stream, execute it with bulk updates
	if s.boundStream != nil {
		return s.executeBulkUpdate(ctx)
	}

	// Nothing to execute if neither prepared stmt nor raw SQL is set
	if s.stmt == nil && s.query == "" {
		return -1, s.Base().ErrorHelper.Errorf(
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
		return -1, s.Base().ErrorHelper.IO("failed to execute statement: %v", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, s.Base().ErrorHelper.IO("failed to get rows affected: %v", err)
	}
	return rowsAffected, nil
}

// ExecuteSchema returns the Arrow schema by querying zero rows
func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if s.query == "" {
		return nil, s.Base().ErrorHelper.InvalidArgument("no query set")
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

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		err := s.Base().ErrorHelper.InvalidArgument("no query set")
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
		return nil, -1, s.Base().ErrorHelper.IO("failed to execute query: %v", err)
	}

	// Get column type information for schema
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, -1, s.Base().ErrorHelper.IO("failed to get column types: %v", err)
	}

	// Build Arrow schema
	schema, err := s.buildArrowSchemaFromColumnTypes(columnTypes)
	if err != nil {
		rows.Close()
		return nil, -1, s.Base().ErrorHelper.IO("failed to build Arrow schema: %v", err)
	}

	// Create a record reader using driverbase BaseRecordReader
	reader, err := NewSQLRecordReader(ctx, memory.DefaultAllocator, rows, schema, columnTypes, int64(s.batchSize))
	if err != nil {
		rows.Close()
		return nil, -1, s.Base().ErrorHelper.IO("failed to create record reader: %v", err)
	}

	// Note: We return -1 for row count since we don't know without reading all rows
	return reader, -1, nil
}

// sqlRecordReaderImpl implements RecordReaderImpl for SQL result sets
type sqlRecordReaderImpl struct {
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	values      []interface{}
	valuePtrs   []interface{}
	schema      *arrow.Schema
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

// BeginAppending prepares for appending rows
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	return nil // No special initialization needed
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

	// Append values to the record builder
	for colIdx, val := range s.values {
		fieldBuilder := builder.Field(colIdx)
		if val == nil {
			fieldBuilder.AppendNull()
		} else {
			// Use helper function to append value
			if err := appendSQLValue(fieldBuilder, val); err != nil {
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

// appendSQLValue appends a SQL value to an Arrow builder using Arrow's built-in scalar system
func appendSQLValue(builder array.Builder, val interface{}) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	// Handle SQL nullable types via the driver.Valuer interface
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

// Close shuts down the prepared stmt (if any) and releases bound resources
func (s *statementImpl) Close() error {
	// Release bound stream if any
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Close prepared statement
	if s.stmt != nil {
		if err := s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close prepared statement: %v", err)
		}
	}
	return nil
}

// ExecutePartitions handles partitioned execution; not supported here
func (s *statementImpl) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	err := s.Base().ErrorHelper.NotImplemented("ExecutePartitions not supported")
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
		return s.Base().ErrorHelper.InvalidArgument("no query to prepare")
	}

	s.stmt, err = s.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return s.Base().ErrorHelper.IO("failed to prepare statement: %v", err)
	}
	return nil
}

// SetBatchSize configures the batch size for streaming operations
func (s *statementImpl) SetBatchSize(size int) error {
	if size <= 0 {
		return s.Base().ErrorHelper.InvalidArgument("batch size must be positive")
	}
	s.batchSize = size
	return nil
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *statementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.NotImplemented("SetSubstraitPlan not supported")
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

// executeBulkUpdate executes bulk updates by iterating through the bound stream directly
// This is simpler than trying to force BaseRecordReader into an update pattern
func (s *statementImpl) executeBulkUpdate(ctx context.Context) (int64, error) {
	if s.query == "" {
		return -1, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Prepare statement if needed
	var stmt *sql.Stmt
	var err error

	if s.stmt != nil {
		stmt = s.stmt
	} else {
		stmt, err = s.conn.PrepareContext(ctx, s.query)
		if err != nil {
			return -1, s.Base().ErrorHelper.IO("failed to prepare statement for batch execution: %v", err)
		}
		defer stmt.Close()
	}

	var totalAffected int64

	// Process the bound stream, respecting batchSize for Arrow record chunking
	for s.boundStream.Next() {
		record := s.boundStream.Record()
		if record == nil {
			continue
		}

		// Process this Arrow Record in chunks of batchSize
		// This ensures batchSize controls Arrow record structure, not SQL execution
		numRows := int(record.NumRows())
		for startRow := 0; startRow < numRows; startRow += s.batchSize {
			endRow := startRow + s.batchSize
			if endRow > numRows {
				endRow = numRows
			}
			
			// Process this chunk of rows (which represents one logical Arrow batch)
			for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
				// Extract parameters for this row
				params := make([]interface{}, record.NumCols())
				for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
					arr := record.Column(colIdx)
					value, err := s.extractArrowValue(arr, rowIdx)
					if err != nil {
						return totalAffected, s.Base().ErrorHelper.IO("failed to extract parameter value: %v", err)
					}
					params[colIdx] = value
				}

				// Execute with parameters
				result, err := stmt.ExecContext(ctx, params...)
				if err != nil {
					return totalAffected, s.Base().ErrorHelper.IO("failed to execute statement: %v", err)
				}

				affected, err := result.RowsAffected()
				if err != nil {
					return totalAffected, s.Base().ErrorHelper.IO("failed to get rows affected: %v", err)
				}
				totalAffected += affected
			}
		}
	}

	// Check for stream errors
	if err := s.boundStream.Err(); err != nil {
		return totalAffected, s.Base().ErrorHelper.IO("stream error during execution: %v", err)
	}

	return totalAffected, nil
}
