package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	// boundRecord holds the bound Arrow record for bulk operations
	boundRecord arrow.Record
	// boundParams holds converted parameters for batch execution
	boundParams [][]interface{}
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
func (s *statementImpl) GetQuery() (string, error) {
	return s.query, nil
}

// Bind uses an arrow record batch to bind parameters to the query
func (s *statementImpl) Bind(ctx context.Context, record arrow.Record) error {
	if record == nil {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "record cannot be nil")
	}

	// Release any previous bound record
	if s.boundRecord != nil {
		s.boundRecord.Release()
	}

	// Retain the record (we'll release it when statement is closed or new record is bound)
	record.Retain()
	s.boundRecord = record

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

	// Clear any previous bound parameters
	if s.boundRecord != nil {
		s.boundRecord.Release()
		s.boundRecord = nil
	}
	s.boundParams = nil

	// Process all records from the stream and accumulate parameters
	var allParams [][]interface{}
	
	for stream.Next() {
		record := stream.Record()
		if record == nil {
			continue
		}

		// Convert this record to parameters
		params, err := s.convertArrowRecordToParams(record)
		if err != nil {
			return err
		}

		// Append to our accumulated parameters
		allParams = append(allParams, params...)
	}

	// Check for stream errors
	if err := stream.Err(); err != nil {
		return err
	}

	// Store the accumulated parameters
	s.boundParams = allParams

	// Release the stream (as per ADBC contract)
	stream.Release()

	return nil
}

// ExecuteUpdate runs DML/DDL and returns rows affected
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	// If we have bound parameters, execute them as a batch
	if s.boundParams != nil {
		return s.executeBatch(ctx)
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

	if s.stmt != nil {
		// Can't use prepared statement with modified query, fall back to direct execution
		rows, err = s.conn.QueryContext(ctx, limitQuery)
	} else {
		rows, err = s.conn.QueryContext(ctx, limitQuery)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column type information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert SQL column types to Arrow fields
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable := sqlTypeToArrow(colType)
		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
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

	// Decimal types - map to string for now (Arrow decimal requires precision/scale)
	case "DECIMAL", "NUMERIC":
		return arrow.BinaryTypes.String, nullable

	// String types
	case "CHAR", "VARCHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT":
		return arrow.BinaryTypes.String, nullable

	// Binary types
	case "BINARY", "VARBINARY", "BLOB", "MEDIUMBLOB", "LONGBLOB", "TINYBLOB":
		return arrow.BinaryTypes.Binary, nullable

	// Date/time types
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nullable
	case "DATETIME", "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_us, nullable
	case "TIME":
		return arrow.FixedWidthTypes.Time64us, nullable

	// Boolean type
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean, nullable

	// JSON type (MySQL specific)
	case "JSON":
		return arrow.BinaryTypes.String, nullable

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
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, nullable := sqlTypeToArrow(colType)
		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create a record reader that will stream the results
	reader := &sqlRecordReader{
		schema:      schema,
		rows:        rows,
		columnTypes: columnTypes,
		allocator:   memory.DefaultAllocator,
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

// Prepare actually prepares the query on the connection
func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query to prepare")
	}
	ps, err := s.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return err
	}
	s.stmt = ps
	return nil
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *statementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan not supported")
}

// sqlRecordReader implements array.RecordReader for streaming SQL results as Arrow records
type sqlRecordReader struct {
	schema      *arrow.Schema
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	allocator     memory.Allocator
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

	// Read a batch of rows (we'll start with 1000 rows per batch)
	const batchSize = 1000
	var batch [][]interface{}
	
	rowCount := 0
	for rowCount < batchSize && r.rows.Next() {
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
		
		batch = append(batch, values)
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
	
	// Convert batch to Arrow record
	record, err := r.batchToArrowRecord(batch)
	if err != nil {
		r.err = err
		return false
	}
	
	r.currentRecord = record
	
	// Check if we've reached the end
	if rowCount < batchSize {
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

// batchToArrowRecord converts a batch of SQL rows to an Arrow record
func (r *sqlRecordReader) batchToArrowRecord(batch [][]interface{}) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("empty batch")
	}
	
	numCols := len(r.columnTypes)
	numRows := len(batch)
	
	// Create arrays for each column
	arrays := make([]arrow.Array, numCols)
	
	for colIdx := 0; colIdx < numCols; colIdx++ {
		arrowType, _ := sqlTypeToArrow(r.columnTypes[colIdx])
		
		// Extract column values from batch
		columnValues := make([]interface{}, numRows)
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			columnValues[rowIdx] = batch[rowIdx][colIdx]
		}
		
		// Build Arrow array based on type
		arr, err := r.buildArrowArray(arrowType, columnValues)
		if err != nil {
			return nil, fmt.Errorf("failed to build array for column %d: %w", colIdx, err)
		}
		arrays[colIdx] = arr
	}
	
	// Create and return the record
	return array.NewRecord(r.schema, arrays, int64(numRows)), nil
}

// buildArrowArray creates an Arrow array from SQL column values
func (r *sqlRecordReader) buildArrowArray(arrowType arrow.DataType, values []interface{}) (arrow.Array, error) {
	switch arrowType.ID() {
	case arrow.INT32:
		return r.buildInt32Array(values)
	case arrow.INT64:
		return r.buildInt64Array(values)
	case arrow.STRING:
		return r.buildStringArray(values)
	case arrow.FLOAT32:
		return r.buildFloat32Array(values)
	case arrow.FLOAT64:
		return r.buildFloat64Array(values)
	case arrow.BOOL:
		return r.buildBoolArray(values)
	default:
		// Default to string for unsupported types
		return r.buildStringArray(values)
	}
}

// buildInt32Array creates an Int32Array from interface{} values
func (r *sqlRecordReader) buildInt32Array(values []interface{}) (arrow.Array, error) {
	builder := array.NewInt32Builder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to int32
			switch v := val.(type) {
			case int32:
				builder.Append(v)
			case int64:
				builder.Append(int32(v))
			case int:
				builder.Append(int32(v))
			default:
				return nil, fmt.Errorf("cannot convert %T to int32", val)
			}
		}
	}
	
	return builder.NewArray(), nil
}

// convertArrowRecordToParams converts an Arrow record to SQL parameters
func (s *statementImpl) convertArrowRecordToParams(record arrow.Record) ([][]interface{}, error) {
	numRows := int(record.NumRows())
	numCols := int(record.NumCols())
	
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
func (s *statementImpl) extractArrowValue(arr arrow.Array, index int) (interface{}, error) {
	if arr.IsNull(index) {
		return nil, nil
	}
	
	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(index), nil
	case *array.Int64:
		return a.Value(index), nil
	case *array.Int16:
		return a.Value(index), nil
	case *array.Int8:
		return a.Value(index), nil
	case *array.Uint32:
		return a.Value(index), nil
	case *array.Uint64:
		return a.Value(index), nil
	case *array.Uint16:
		return a.Value(index), nil
	case *array.Uint8:
		return a.Value(index), nil
	case *array.Float32:
		return a.Value(index), nil
	case *array.Float64:
		return a.Value(index), nil
	case *array.String:
		return a.Value(index), nil
	case *array.Boolean:
		return a.Value(index), nil
	case *array.Binary:
		return a.Value(index), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow array type: %T", arr)
	}
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

// buildInt64Array creates an Int64Array from interface{} values
func (r *sqlRecordReader) buildInt64Array(values []interface{}) (arrow.Array, error) {
	builder := array.NewInt64Builder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to int64
			switch v := val.(type) {
			case int64:
				builder.Append(v)
			case int32:
				builder.Append(int64(v))
			case int:
				builder.Append(int64(v))
			default:
				return nil, fmt.Errorf("cannot convert %T to int64", val)
			}
		}
	}
	
	return builder.NewArray(), nil
}

// buildStringArray creates a StringArray from interface{} values
func (r *sqlRecordReader) buildStringArray(values []interface{}) (arrow.Array, error) {
	builder := array.NewStringBuilder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to string, handling common MySQL types
			var str string
			switch v := val.(type) {
			case string:
				str = v
			case []byte:
				str = string(v) // MySQL VARCHAR/TEXT often comes as []byte
			case nil:
				builder.AppendNull()
				continue
			default:
				str = fmt.Sprintf("%v", val)
			}
			builder.Append(str)
		}
	}
	
	return builder.NewArray(), nil
}

// buildFloat32Array creates a Float32Array from interface{} values
func (r *sqlRecordReader) buildFloat32Array(values []interface{}) (arrow.Array, error) {
	builder := array.NewFloat32Builder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to float32
			switch v := val.(type) {
			case float32:
				builder.Append(v)
			case float64:
				builder.Append(float32(v))
			default:
				return nil, fmt.Errorf("cannot convert %T to float32", val)
			}
		}
	}
	
	return builder.NewArray(), nil
}

// buildFloat64Array creates a Float64Array from interface{} values
func (r *sqlRecordReader) buildFloat64Array(values []interface{}) (arrow.Array, error) {
	builder := array.NewFloat64Builder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to float64
			switch v := val.(type) {
			case float64:
				builder.Append(v)
			case float32:
				builder.Append(float64(v))
			default:
				return nil, fmt.Errorf("cannot convert %T to float64", val)
			}
		}
	}
	
	return builder.NewArray(), nil
}

// buildBoolArray creates a BooleanArray from interface{} values
func (r *sqlRecordReader) buildBoolArray(values []interface{}) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(r.allocator)
	defer builder.Release()
	
	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			// Convert to bool
			switch v := val.(type) {
			case bool:
				builder.Append(v)
			case int64:
				builder.Append(v != 0)
			default:
				return nil, fmt.Errorf("cannot convert %T to bool", val)
			}
		}
	}
	
	return builder.NewArray(), nil
}
