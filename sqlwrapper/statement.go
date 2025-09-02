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

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Custom option keys for the sqlwrapper
const (
	// OptionKeyBatchSize controls how many Arrow records to accumulate in a record batch
	OptionKeyBatchSize = "adbc.statement.batch_size"
)

// StatementImpl implements the ADBC Statement interface on top of database/sql.
type StatementImpl struct {
	driverbase.StatementImplBase

	// Conn is the dedicated SQL connection
	Conn *sql.Conn
	// ConnectionImpl is a reference to the parent connection for bulk ingest.
	// Type is 'any' to allow database-specific connections (e.g., *mysqlConnectionImpl)
	// to override ExecuteBulkIngest while maintaining compatibility with the base
	// *sqlwrapper.ConnectionImpl. This is safe because:
	// 1. Only used for bulk ingest operations with defensive type assertion
	// 2. All other statement operations use the strongly-typed individual fields (Conn, TypeConverter, etc.)
	// 3. Type assertion failure is handled gracefully with descriptive error
	ConnectionImpl any
	// Query holds the SQL to execute
	Query string
	// Stmt holds the prepared statement, if Prepare() was called
	Stmt *sql.Stmt
	// BoundStream holds the bound Arrow record stream for bulk operations
	BoundStream array.RecordReader
	// BatchSize controls how many records to process at once during streaming execution
	BatchSize int
	// TypeConverter handles SQL-to-Arrow type conversion
	TypeConverter TypeConverter

	// bulk ingest
	BulkIngestOptions driverbase.BulkIngestOptions
}

// Base returns the embedded StatementImplBase for driverbase plumbing
func (s *StatementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

// newStatement constructs a new StatementImpl wrapped by driverbase
func newStatement(c *ConnectionImpl) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	return driverbase.NewStatement(&StatementImpl{
		StatementImplBase: base,
		Conn:              c.Conn,
		ConnectionImpl:    c,
		BatchSize:         1000, // Default batch size for streaming operations
		TypeConverter:     c.TypeConverter,
		BulkIngestOptions: driverbase.NewBulkIngestOptions(),
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *StatementImpl) SetSqlQuery(query string) error {
	// if someone resets the SQL after Prepare, clean up the old stmt
	if s.Stmt != nil {
		if err := s.Stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close prepared statement: %v", err)
		}
		s.Stmt = nil
	}

	// Clear any bound parameters when setting a new query
	if s.BoundStream != nil {
		s.BoundStream.Release()
		s.BoundStream = nil
	}

	s.Query = query
	return nil
}

// SetOption sets a string option on this statement
func (s *StatementImpl) SetOption(key, val string) error {
	// Let driverbase handle standard bulk ingest options first
	if handled, err := s.BulkIngestOptions.SetOption(&s.Base().ErrorHelper, key, val); err != nil {
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
func (s *StatementImpl) Bind(ctx context.Context, record arrow.Record) error {
	if record == nil {
		return s.Base().ErrorHelper.InvalidArgument("record cannot be nil")
	}

	// Release any previous bound stream
	if s.BoundStream != nil {
		s.BoundStream.Release()
		s.BoundStream = nil
	}

	// Convert single record to a RecordReader using Arrow's built-in function
	s.BoundStream, _ = array.NewRecordReader(record.Schema(), []arrow.Record{record})
	return nil
}

// BindStream uses a record batch stream to bind parameters for bulk operations
func (s *StatementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	if stream == nil {
		return s.Base().ErrorHelper.InvalidArgument("stream cannot be nil")
	}

	// Release any previous bound stream
	if s.BoundStream != nil {
		s.BoundStream.Release()
		s.BoundStream = nil
	}

	// Store the stream for lazy consumption during execution
	stream.Retain()
	s.BoundStream = stream

	return nil
}

// ExecuteUpdate runs DML/DDL and returns rows affected
func (s *StatementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	// Check if this is a bulk ingest operation
	if s.BulkIngestOptions.IsSet() {
		return s.executeBulkIngest(ctx)
	}

	// If we have a bound stream, execute it with bulk updates
	if s.BoundStream != nil {
		return s.executeBulkUpdate(ctx)
	}

	// Nothing to execute if neither prepared stmt nor raw SQL is set
	if s.Stmt == nil && s.Query == "" {
		return -1, s.Base().ErrorHelper.Errorf(
			adbc.StatusInvalidArgument,
			"no SQL statement provided",
		)
	}

	// Regular execution without parameters
	var res sql.Result
	var err error

	if s.Stmt != nil {
		res, err = s.Stmt.ExecContext(ctx)
	} else {
		res, err = s.Conn.ExecContext(ctx, s.Query)
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
func (s *StatementImpl) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	if s.Query == "" {
		return nil, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Execute query with LIMIT 0 to get schema without data
	limitQuery := fmt.Sprintf("SELECT * FROM (%s) AS subquery LIMIT 0", s.Query)

	var rows *sql.Rows

	// Can't use prepared statement with modified query, fall back to direct execution
	rows, err = s.Conn.QueryContext(ctx, limitQuery)
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
	return buildArrowSchemaFromColumnTypes(columnTypes, s.TypeConverter)
}

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *StatementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.Query == "" {
		err := s.Base().ErrorHelper.InvalidArgument("no query set")
		return nil, -1, err
	}

	// Execute the query
	var rows *sql.Rows
	var err error

	if s.Stmt != nil {
		rows, err = s.Stmt.QueryContext(ctx)
	} else {
		rows, err = s.Conn.QueryContext(ctx, s.Query)
	}

	if err != nil {
		return nil, -1, s.Base().ErrorHelper.IO("failed to execute query: %v", err)
	}

	// Get column type information for schema
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		err = errors.Join(err, rows.Close())
		return nil, -1, s.Base().ErrorHelper.IO("failed to get column types: %v", err)
	}

	// Build Arrow schema
	schema, err := buildArrowSchemaFromColumnTypes(columnTypes, s.TypeConverter)
	if err != nil {
		err = errors.Join(err, rows.Close())
		return nil, -1, s.Base().ErrorHelper.IO("failed to build Arrow schema: %v", err)
	}

	// Create a record reader by constructing the implementation directly
	impl := &sqlRecordReaderImpl{
		rows:          rows,
		columnTypes:   columnTypes,
		schema:        schema,
		conn:          s.Conn,
		query:         s.Query,
		stmt:          s.Stmt,
		typeConverter: s.TypeConverter,
	}
	impl.ensureValueBuffers(len(columnTypes))

	reader := &driverbase.BaseRecordReader{}
	if err := reader.Init(ctx, memory.DefaultAllocator, nil, int64(s.BatchSize), impl); err != nil {
		err = errors.Join(err, rows.Close())
		return nil, -1, s.Base().ErrorHelper.IO("failed to create record reader: %v", err)
	}

	// Note: We return -1 for row count since we don't know without reading all rows
	return reader, -1, nil
}

// Close shuts down the prepared stmt (if any) and releases bound resources
func (s *StatementImpl) Close() error {
	// Release bound stream if any
	if s.BoundStream != nil {
		s.BoundStream.Release()
		s.BoundStream = nil
	}

	// Close prepared statement
	if s.Stmt != nil {
		if err := s.Stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close prepared statement: %v", err)
		}
	}
	return nil
}

// ExecutePartitions handles partitioned execution; not supported here
func (s *StatementImpl) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	err := s.Base().ErrorHelper.NotImplemented("ExecutePartitions not supported")
	return nil, adbc.Partitions{}, 0, err
}

// GetParameterSchema returns the schema for query parameters.
// This is not supported because parameter syntax varies between databases and database/sql
// doesn't provide parameter introspection.
func (s *StatementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.Base().ErrorHelper.NotImplemented("GetParameterSchema not supported - parameter introspection varies by database")
}

func (s *StatementImpl) Prepare(ctx context.Context) (err error) {
	if s.Query == "" {
		return s.Base().ErrorHelper.InvalidArgument("no query to prepare")
	}

	// Close old statement if it exists
	if s.Stmt != nil {
		if err = s.Stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close statement: %v", err)
		}
		s.Stmt = nil
	}

	s.Stmt, err = s.Conn.PrepareContext(ctx, s.Query)
	if err != nil {
		return s.Base().ErrorHelper.IO("failed to prepare statement: %v", err)
	}
	return nil
}

// SetBatchSize configures the batch size for streaming operations
func (s *StatementImpl) SetBatchSize(size int) error {
	if size <= 0 {
		return s.Base().ErrorHelper.InvalidArgument("batch size must be positive")
	}
	s.BatchSize = size
	return nil
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *StatementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.NotImplemented("SetSubstraitPlan not supported")
}

// executeBulkUpdate executes bulk updates by iterating through the bound stream directly
func (s *StatementImpl) executeBulkUpdate(ctx context.Context) (totalAffected int64, err error) {
	if s.Query == "" {
		return -1, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Prepare statement if needed
	var stmt *sql.Stmt
	if s.Stmt != nil {
		stmt = s.Stmt
	} else {
		stmt, err = s.Conn.PrepareContext(ctx, s.Query)
		if err != nil {
			return -1, s.Base().ErrorHelper.IO("failed to prepare statement for batch execution: %v", err)
		}
		defer func() {
			err = errors.Join(err, stmt.Close())
		}()
	}

	params := make([]any, s.BoundStream.Schema().NumFields())
	for s.BoundStream.Next() {
		record := s.BoundStream.Record()
		for rowIdx := range int(record.NumRows()) {
			for colIdx := range int(record.NumCols()) {
				arr := record.Column(colIdx)
				field := record.Schema().Field(colIdx)
				value, err := s.TypeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return totalAffected, s.Base().ErrorHelper.IO("failed to extract parameter value: %v", err)
				}
				params[colIdx] = value
			}

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

	// Check for stream errors
	if err := s.BoundStream.Err(); err != nil {
		return totalAffected, s.Base().ErrorHelper.IO("stream error during execution: %v", err)
	}

	return totalAffected, nil
}

// executeBulkIngest executes bulk ingest using the connection's ExecuteBulkIngest method
func (s *StatementImpl) executeBulkIngest(ctx context.Context) (int64, error) {
	// Check for proper bulk ingest setup
	if s.BoundStream == nil {
		return -1, s.Base().ErrorHelper.InvalidArgument("bulk ingest options are set but no stream is bound - call BindStream() first")
	}

	// Type-assert to get the ExecuteBulkIngest method - supports both ConnectionImpl and mysqlConnectionImpl
	type bulkIngester interface {
		ExecuteBulkIngest(ctx context.Context, options *driverbase.BulkIngestOptions, stream array.RecordReader) error
	}

	if ingester, ok := s.ConnectionImpl.(bulkIngester); ok {
		err := ingester.ExecuteBulkIngest(ctx, &s.BulkIngestOptions, s.BoundStream)
		if err != nil {
			return -1, err
		}
		// TODO: Return actual row count from bulk ingest operations instead of -1
		// Need to enhance ExecuteBulkIngest interface to return (int64, error) to support proper row count reporting.
		return -1, nil
	}

	return -1, s.Base().ErrorHelper.NotImplemented("connection does not support bulk ingest")
}
