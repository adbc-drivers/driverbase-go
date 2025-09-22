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
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// sqlRecordReaderImpl implements RecordReaderImpl interface for SQL result sets.
// This is the row-wise implementation that BaseRecordReader converts to batch-wise.
type sqlRecordReaderImpl struct {
	// Current result set data
	rows        *LoggingRows
	columnTypes []*sql.ColumnType
	values      []any
	valuePtrs   []any
	schema      *arrow.Schema

	// For bind parameter support
	conn          *LoggingConn  // Database connection to execute queries
	query         string        // Original SQL query with placeholders
	stmt          *LoggingStmt  // Prepared statement (optional)
	typeConverter TypeConverter // Type converter for building schemas

	// Performance optimization: pre-computed inserters to avoid per-value type switching
	columnInserters []Inserter
}

// NextResultSet returns the Arrow schema for the current result set.
// For SQL queries with bind parameters, this method executes the query with the
// specific parameter set (rec[rowIdx]) and returns the resulting schema.
func (s *sqlRecordReaderImpl) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (schema *arrow.Schema, err error) {
	// Close any previous result set
	if s.rows != nil {
		if err := s.rows.Close(); err != nil {
			return nil, fmt.Errorf("failed to close previous result set: %w", err)
		}
		s.rows = nil
	}

	// Extract parameters if present
	var args []any
	if rec != nil {
		args = make([]any, int(rec.NumCols()))
		for i := range args {
			field := rec.Schema().Field(i)
			args[i], err = s.typeConverter.ConvertArrowToGo(rec.Column(i), rowIdx, &field)
			if err != nil {
				return nil, fmt.Errorf("failed to extract parameter %d: %w", i, err)
			}
		}
	}

	// Execute query (with or without parameters)
	if s.stmt != nil {
		s.rows, err = s.stmt.QueryContext(ctx, args...)
	} else {
		s.rows, err = s.conn.QueryContext(ctx, s.query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Common post-execution logic for both cases
	return s.setupResultSet()
}

// setupResultSet handles the common post-execution setup for both parameterized and non-parameterized queries
func (s *sqlRecordReaderImpl) setupResultSet() (schema *arrow.Schema, err error) {
	// Get column type information from s.rows (assigned in NextResultSet)
	columnTypes, err := s.rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build Arrow schema
	s.schema, err = buildArrowSchemaFromColumnTypes(columnTypes, s.typeConverter)
	if err != nil {
		return nil, fmt.Errorf("failed to build Arrow schema: %w", err)
	}

	// Update implementation state
	s.columnTypes = columnTypes
	s.ensureValueBuffers(len(columnTypes))

	return s.schema, nil
}

// ensureValueBuffers ensures the value buffers are sized correctly for the given column count
func (s *sqlRecordReaderImpl) ensureValueBuffers(numCols int) {
	// Only reallocate if size has changed
	if len(s.values) != numCols {
		s.values = make([]any, numCols)
		s.valuePtrs = make([]any, numCols)
	}

	// Always update pointers since values array might be reallocated
	for i := range s.values {
		s.valuePtrs[i] = &s.values[i]
	}
}

// BeginAppending prepares for appending rows.
// This is called once before the first AppendRow call.
// Note: BaseRecordReader may need to call this again after schema changes in NextResultSet.
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	// Create column-specific inserters bound to builders to eliminate ALL type switching
	// This optimization moves type checking from per-value to per-column (once per schema setup)
	numCols := len(s.schema.Fields())
	s.columnInserters = make([]Inserter, numCols)

	for i, field := range s.schema.Fields() {
		fieldBuilder := builder.Field(i)
		inserter, err := s.typeConverter.CreateInserter(&field, fieldBuilder)
		if err != nil {
			return fmt.Errorf("failed to create inserter for column %d: %w", i, err)
		}
		s.columnInserters[i] = inserter
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

	// Append each column value using pre-bound inserters
	// This eliminates ALL type switching - inserters are bound to builders during BeginAppending
	for i := range len(s.values) {
		if err := s.columnInserters[i].AppendValue(s.values[i]); err != nil {
			return fmt.Errorf("failed to append value to column %d: %w", i, err)
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
