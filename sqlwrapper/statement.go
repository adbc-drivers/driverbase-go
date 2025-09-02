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
func newStatement(c *ConnectionImpl) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: base,
		conn:              c.Conn,
		batchSize:         1000, // Default batch size for streaming operations
		typeConverter:     c.TypeConverter,
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *statementImpl) SetSqlQuery(query string) error {
	// if someone resets the SQL after Prepare, clean up the old stmt
	if s.stmt != nil {
		if err := s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close prepared statement: %v", err)
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
func (s *statementImpl) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	if s.query == "" {
		return nil, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Execute query with LIMIT 0 to get schema without data
	limitQuery := fmt.Sprintf("SELECT * FROM (%s) AS subquery LIMIT 0", s.query)

	var rows *sql.Rows

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

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (reader array.RecordReader, rowCount int64, err error) {
	if s.query == "" {
		err := s.Base().ErrorHelper.InvalidArgument("no query set")
		return nil, -1, err
	}

	// Create sqlRecordReaderImpl - it handles both parameterized and non-parameterized queries
	var rows *sql.Rows
	var columnTypes []*sql.ColumnType
	var schema *arrow.Schema

	// For non-parameterized queries, execute immediately to get schema
	if s.boundStream == nil {
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
		columnTypes, err = rows.ColumnTypes()
		if err != nil {
			err = errors.Join(err, rows.Close())
			return nil, -1, s.Base().ErrorHelper.IO("failed to get column types: %v", err)
		}

		// Build Arrow schema
		schema, err = buildArrowSchemaFromColumnTypes(columnTypes, s.typeConverter)
		if err != nil {
			err = errors.Join(err, rows.Close())
			return nil, -1, s.Base().ErrorHelper.IO("failed to build Arrow schema: %v", err)
		}
	}

	// Create record reader implementation
	impl := &sqlRecordReaderImpl{
		rows:          rows,        // nil for parameterized queries, set for non-parameterized
		columnTypes:   columnTypes, // nil for parameterized queries, set for non-parameterized
		schema:        schema,      // nil for parameterized queries, set for non-parameterized
		conn:          s.conn,
		query:         s.query,
		stmt:          s.stmt,
		typeConverter: s.typeConverter,
	}
	if columnTypes != nil {
		impl.ensureValueBuffers(len(columnTypes))
	}

	// Create BaseRecordReader with bound stream (nil for non-parameterized queries)
	baseRecordReader := &driverbase.BaseRecordReader{}
	if err := baseRecordReader.Init(ctx, memory.DefaultAllocator, s.boundStream, int64(s.batchSize), impl); err != nil {
		if rows != nil {
			err = errors.Join(err, rows.Close())
		}
		return nil, -1, s.Base().ErrorHelper.IO("failed to create record reader: %v", err)
	}

	// Note: We return -1 for row count since we don't know without reading all rows
	return baseRecordReader, -1, nil
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
// This is not supported because parameter syntax varies between databases and database/sql
// doesn't provide parameter introspection.
func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.Base().ErrorHelper.NotImplemented("GetParameterSchema not supported - parameter introspection varies by database")
}

func (s *statementImpl) Prepare(ctx context.Context) (err error) {
	if s.query == "" {
		return s.Base().ErrorHelper.InvalidArgument("no query to prepare")
	}

	// Close old statement if it exists
	if s.stmt != nil {
		if err = s.stmt.Close(); err != nil {
			return s.Base().ErrorHelper.IO("failed to close statement: %v", err)
		}
		s.stmt = nil
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

// executeBulkUpdate executes bulk updates by iterating through the bound stream directly
func (s *statementImpl) executeBulkUpdate(ctx context.Context) (totalAffected int64, err error) {
	if s.query == "" {
		return -1, s.Base().ErrorHelper.InvalidArgument("no query set")
	}

	// Prepare statement if needed
	var stmt *sql.Stmt
	if s.stmt != nil {
		stmt = s.stmt
	} else {
		stmt, err = s.conn.PrepareContext(ctx, s.query)
		if err != nil {
			return -1, s.Base().ErrorHelper.IO("failed to prepare statement for batch execution: %v", err)
		}
		defer func() {
			err = errors.Join(err, stmt.Close())
		}()
	}

	params := make([]any, s.boundStream.Schema().NumFields())
	for s.boundStream.Next() {
		record := s.boundStream.Record()
		for rowIdx := range int(record.NumRows()) {
			for colIdx := range int(record.NumCols()) {
				arr := record.Column(colIdx)
				field := record.Schema().Field(colIdx)
				value, err := s.typeConverter.ConvertArrowToGo(arr, rowIdx, &field)
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
	if err := s.boundStream.Err(); err != nil {
		return totalAffected, s.Base().ErrorHelper.IO("stream error during execution: %v", err)
	}

	return totalAffected, nil
}
