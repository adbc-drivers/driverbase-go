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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BulkIngester interface allows drivers to implement database-specific bulk ingest functionality
type BulkIngester interface {
	ExecuteBulkIngest(ctx context.Context, conn *LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (int64, error)

	// QuoteIdentifier quotes a table/column identifier for SQL
	QuoteIdentifier(name string) string

	// GetPlaceholder returns the SQL placeholder for a field at the given parameter index (0-based)
	// Examples: "?" for MySQL/Trino, "$1" for PostgreSQL (index+1), "CAST(? AS REAL)" for special types
	// For databases using positional placeholders (e.g., PostgreSQL $1, $2, ...), the index indicates
	// the parameter position in the overall statement
	GetPlaceholder(field *arrow.Field, index int) string
}

// Custom option keys for the sqlwrapper
const (
	// OptionKeyBatchSize controls how many Arrow records to accumulate in a record batch
	OptionKeyBatchSize = "adbc.statement.batch_size"
)

// statementImpl implements the ADBC Statement interface on top of database/sql.
type statementImpl struct {
	driverbase.StatementImplBase

	// conn is the dedicated SQL connection
	conn *LoggingConn
	// connectionImpl is a reference to the parent connection for bulk ingest
	connectionImpl ConnectionImpl
	// query holds the SQL to execute
	query string
	// stmt holds the prepared statement, if Prepare() was called
	stmt *LoggingStmt
	// boundStream holds the bound Arrow record stream for bulk operations
	boundStream array.RecordReader
	// batchSize controls how many records to process at once during streaming execution
	batchSize int
	// typeConverter handles SQL-to-Arrow type conversion
	typeConverter TypeConverter

	// bulk ingest
	bulkIngestOptions driverbase.BulkIngestOptions
	// closed tracks if the statement has been closed
	closed bool
}

// Base returns the embedded StatementImplBase for driverbase plumbing
func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

// newStatement constructs a new StatementImpl wrapped by driverbase
func newStatement(c *ConnectionImplBase) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: base,
		conn:              c.Conn,
		connectionImpl:    c.Derived,
		batchSize:         1000, // Default batch size for streaming operations
		typeConverter:     c.TypeConverter,
		bulkIngestOptions: driverbase.NewBulkIngestOptions(),
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *statementImpl) SetSqlQuery(query string) error {
	if err := s.connectionImpl.ClearPending(); err != nil {
		return err
	}

	// if someone resets the SQL after Prepare, clean up the old stmt
	if s.stmt != nil {
		if err := s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.WrapIO(err, "failed to close prepared statement")
		}
		s.stmt = nil
	}

	// Clear any bound parameters when setting a new query
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	s.query = query
	return nil
}

// SetOption sets a string option on this statement
func (s *statementImpl) SetOption(key, val string) error {
	// Let driverbase handle standard bulk ingest options first
	if handled, err := s.bulkIngestOptions.SetOption(&s.Base().ErrorHelper, key, val); err != nil {
		return err
	} else if handled {
		return nil
	}

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
func (s *statementImpl) Bind(ctx context.Context, record arrow.RecordBatch) error {
	if record == nil {
		return s.Base().ErrorHelper.InvalidArgument("record cannot be nil")
	}

	// Release any previous bound stream
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Convert single record to a RecordReader using Arrow's built-in function
	s.boundStream, _ = array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
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
	if err := s.connectionImpl.ClearPending(); err != nil {
		return -1, err
	}

	// Check if this is a bulk ingest operation
	if s.bulkIngestOptions.IsSet() {
		return s.executeBulkIngest(ctx)
	}

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
		return -1, s.Base().ErrorHelper.WrapIO(err, "failed to execute statement")
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, s.Base().ErrorHelper.WrapIO(err, "failed to get rows affected")
	}
	return rowsAffected, nil
}

// ExecuteSchema returns the Arrow schema by querying zero rows
func (s *statementImpl) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	if s.query == "" {
		return nil, s.Base().ErrorHelper.InvalidState("no query set")
	}

	if err := s.connectionImpl.ClearPending(); err != nil {
		return nil, err
	}

	// Execute query with LIMIT 0 to get schema without data
	limitQuery := fmt.Sprintf("SELECT * FROM (%s) AS subquery LIMIT 0", s.query)

	var rows *LoggingRows

	// Can't use prepared statement with modified query, fall back to direct execution
	rows, err = s.conn.QueryContext(ctx, limitQuery)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Get column type information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// Convert SQL column types to Arrow schema
	return buildArrowSchemaFromColumnTypes(columnTypes, s.typeConverter)
}

type closer struct {
	baseRecordReader *driverbase.BaseRecordReader
}

func (c closer) Close() error {
	if c.baseRecordReader != nil {
		c.baseRecordReader.Close()
	}
	return nil
}

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (reader array.RecordReader, rowCount int64, err error) {
	if s.query == "" {
		return nil, -1, s.Base().ErrorHelper.InvalidState("no query set")
	}

	if err := s.connectionImpl.ClearPending(); err != nil {
		return nil, -1, err
	}

	// Create the record reader implementation with all the state
	impl := &sqlRecordReaderImpl{
		conn:          s.conn,
		query:         s.query,
		stmt:          s.stmt,
		typeConverter: s.typeConverter,
	}

	// Let BaseRecordReader handle parameterized vs non-parameterized logic
	baseRecordReader := &driverbase.BaseRecordReader{}
	// Must use a background context or else when the CGO wrapper code
	// cancels the context, that gets propagated down to the underlying
	// database/sql driver which in some implementations will close the
	// connection for some reason.
	// TODO(lidavidm): when given ctx is cancelled, cancel the query, but
	// not in a way that breaks the connection!
	options := driverbase.BaseRecordReaderOptions{
		BatchRowLimit: int64(s.batchSize),
	}
	if err := baseRecordReader.Init(context.Background(), memory.DefaultAllocator, s.boundStream,
		options, impl); err != nil {
		// Clear boundStream on error to prevent double-release in Close()
		s.boundStream = nil
		return nil, -1, s.Base().ErrorHelper.WrapIO(err, "failed to create record reader")
	}
	s.boundStream = nil

	if err := s.connectionImpl.OfferPending(closer{baseRecordReader: baseRecordReader}); err != nil {
		return nil, -1, err
	}

	return baseRecordReader, -1, nil
}

// Close shuts down the prepared stmt (if any) and releases bound resources
func (s *statementImpl) Close() error {
	// Check if already closed
	if s.closed {
		return s.Base().ErrorHelper.InvalidState("statement already closed")
	}
	s.closed = true

	// Release bound stream if any
	if s.boundStream != nil {
		s.boundStream.Release()
		s.boundStream = nil
	}

	// Close prepared statement
	if s.stmt != nil {
		if err := s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.WrapIO(err, "failed to close prepared statement")
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
// This is not supported because parameter syntax varies between databases and database/sql
// doesn't provide parameter introspection.
func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.Base().ErrorHelper.NotImplemented("GetParameterSchema not supported - parameter introspection varies by database")
}

func (s *statementImpl) Prepare(ctx context.Context) (err error) {
	if s.query == "" {
		return s.Base().ErrorHelper.InvalidArgument("no query to prepare")
	}

	if err := s.connectionImpl.ClearPending(); err != nil {
		return err
	}

	// Close old statement if it exists
	if s.stmt != nil {
		if err = s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.WrapIO(err, "failed to close statement")
		}
		s.stmt = nil
	}

	s.stmt, err = s.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return s.Base().ErrorHelper.WrapIO(err, "failed to prepare statement")
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

// executeBulkUpdate executes bulk updates by iterating through the bound stream directly
func (s *statementImpl) executeBulkUpdate(ctx context.Context) (totalAffected int64, err error) {
	if s.query == "" {
		return -1, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Prepare statement if needed
	var stmt *LoggingStmt
	if s.stmt != nil {
		stmt = s.stmt
	} else {
		stmt, err = s.conn.PrepareContext(ctx, s.query)
		if err != nil {
			return -1, s.Base().ErrorHelper.WrapIO(err, "failed to prepare statement for batch execution")
		}
		defer func() {
			err = errors.Join(err, stmt.Close())
		}()
	}

	params := make([]any, s.boundStream.Schema().NumFields())
	for s.boundStream.Next() {
		record := s.boundStream.RecordBatch()
		for rowIdx := range int(record.NumRows()) {
			for colIdx := range int(record.NumCols()) {
				arr := record.Column(colIdx)
				field := record.Schema().Field(colIdx)
				value, err := s.typeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return totalAffected, s.Base().ErrorHelper.WrapIO(err, "failed to extract parameter value")
				}
				params[colIdx] = value
			}

			result, err := stmt.ExecContext(ctx, params...)
			if err != nil {
				return totalAffected, s.Base().ErrorHelper.WrapIO(err, "failed to execute statement")
			}

			affected, err := result.RowsAffected()
			if err != nil {
				return totalAffected, s.Base().ErrorHelper.WrapIO(err, "failed to get rows affected")
			}
			totalAffected += affected
		}
	}

	// Check for stream errors
	if err := s.boundStream.Err(); err != nil {
		return totalAffected, s.Base().ErrorHelper.WrapIO(err, "stream error during execution")
	}

	return totalAffected, nil
}

// executeBulkIngest executes bulk ingest using the connection's ExecuteBulkIngest method
func (s *statementImpl) executeBulkIngest(ctx context.Context) (int64, error) {
	// Check for proper bulk ingest setup
	if s.boundStream == nil {
		return -1, s.Base().ErrorHelper.InvalidArgument("bulk ingest options are set but no stream is bound - call BindStream() first")
	}

	// Type-assert to the BulkIngester interface for database-specific implementations
	if ingester, ok := s.connectionImpl.(BulkIngester); ok {
		rowCount, err := ingester.ExecuteBulkIngest(ctx, s.conn, &s.bulkIngestOptions, s.boundStream)
		if err != nil {
			return -1, err
		}
		return rowCount, nil
	}

	return -1, s.Base().ErrorHelper.NotImplemented("connection does not support bulk ingest")
}

// ExecuteBatchedBulkIngest provides a generic batched INSERT implementation for SQL databases.
// This is used by connections that don't have a database-specific bulk loading mechanism.
//
// It generates multi-row INSERT statements like: INSERT INTO table VALUES (row1), (row2), ..., (rowN)
//
// Parameters:
//   - ingester: provides QuoteIdentifier and GetPlaceholder helpers
//
// Batching behavior:
//   - uses options.IngestBatchSize (defaults to 1000 if <= 0).
func ExecuteBatchedBulkIngest(
	ctx context.Context,
	conn *LoggingConn,
	options *driverbase.BulkIngestOptions,
	stream array.RecordReader,
	typeConverter TypeConverter,
	ingester BulkIngester,
	errorHelper *driverbase.ErrorHelper,
) (totalRowsInserted int64, err error) {
	if stream == nil {
		return -1, errorHelper.InvalidArgument("stream cannot be nil")
	}

	batchSize := options.IngestBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	schema := stream.Schema()
	numCols := len(schema.Fields())
	if numCols == 0 {
		return 0, errorHelper.InvalidArgument("stream has no columns")
	}

	quotedTableName := ingester.QuoteIdentifier(options.TableName)

	iterator, err := NewRowBufferIterator(stream, batchSize, typeConverter)
	if err != nil {
		return -1, errorHelper.WrapIO(err, "failed to create row buffer iterator")
	}

	insertSQL := buildMultiRowInsertSQL(quotedTableName, schema, batchSize, ingester)
	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return -1, errorHelper.WrapIO(err, "failed to prepare batch insert statement")
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	for iterator.Next() {
		buffer, rowCount := iterator.CurrentBatch()

		if rowCount == batchSize {
			// Full batch: use pre-prepared statement
			result, execErr := stmt.ExecContext(ctx, buffer...)
			if execErr != nil {
				return totalRowsInserted, errorHelper.WrapIO(execErr,
					"failed to execute batch insert (inserted so far: %d)", totalRowsInserted)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return totalRowsInserted, errorHelper.WrapIO(err, "failed to get rows affected")
			}
			totalRowsInserted += rowsAffected
		} else {
			// Partial batch at end: handle with multi-row insertion
			partialInserted, partialErr := ExecutePartialBatch(
				ctx, conn, quotedTableName, schema,
				buffer, rowCount, ingester, errorHelper)
			if partialErr != nil {
				return totalRowsInserted, partialErr
			}
			totalRowsInserted += partialInserted
		}
	}

	// Check for iteration errors
	if iterErr := iterator.Err(); iterErr != nil {
		return totalRowsInserted, errorHelper.WrapIO(iterErr, "stream error during iteration")
	}

	return totalRowsInserted, nil
}

// buildMultiRowInsertSQL constructs an INSERT statement with placeholders for a fixed number of rows.
func buildMultiRowInsertSQL(quotedTableName string, schema *arrow.Schema, batchSize int, ingester BulkIngester) string {
	var queryBuilder strings.Builder
	numCols := len(schema.Fields())

	// Estimate capacity: "INSERT INTO table VALUES " + batchSize * "(?, ?, ?), "
	singleRowLen := 1 + numCols*4 + 2 // "(" + placeholders (estimate 4 chars each) + ")"
	estimatedSize := 20 + len(quotedTableName) + 8 + (batchSize * (singleRowLen + 2))
	queryBuilder.Grow(estimatedSize)

	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(quotedTableName)
	queryBuilder.WriteString(" VALUES ")

	// Build multi-row VALUES with proper parameter indices
	for row := range batchSize {
		if row > 0 {
			queryBuilder.WriteString(",")
		}
		queryBuilder.WriteString("(")
		for col := range numCols {
			if col > 0 {
				queryBuilder.WriteString(",")
			}
			field := schema.Field(col)
			paramIndex := row*numCols + col
			placeholder := ingester.GetPlaceholder(&field, paramIndex)
			queryBuilder.WriteString(placeholder)
		}
		queryBuilder.WriteString(")")
	}

	return queryBuilder.String()
}

// ExecutePartialBatch executes a multi-row INSERT with a dynamic number of rows.
// Builds a multi-row INSERT statement for the exact batch size and executes with parameters.
func ExecutePartialBatch(
	ctx context.Context,
	conn *LoggingConn,
	quotedTableName string,
	schema *arrow.Schema,
	buffer []any,
	rowCount int,
	ingester BulkIngester,
	errorHelper *driverbase.ErrorHelper,
) (rowsInserted int64, err error) {
	insertSQL := buildMultiRowInsertSQL(quotedTableName, schema, rowCount, ingester)
	stmt, err := conn.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, errorHelper.WrapIO(err, "failed to prepare partial batch insert")
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	result, execErr := stmt.ExecContext(ctx, buffer...)
	if execErr != nil {
		return 0, errorHelper.WrapIO(execErr, "failed to insert partial batch (%d rows)", rowCount)
	}

	rowsInserted, err = result.RowsAffected()
	if err != nil {
		err = errorHelper.WrapIO(err, "failed to get rows affected")
	}
	return
}
