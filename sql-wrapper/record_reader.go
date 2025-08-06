// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sqlwrapper

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)


// appendValue is the unified value appender that handles all Arrow builder types.
// It uses a TypeConverter to handle SQL-to-Arrow value conversion, then appends to the builder.
func appendValue(builder array.Builder, val interface{}, typeConverter TypeConverter) error {
	// Convert SQL value to Arrow value using TypeConverter
	arrowType := builder.Type()
	convertedVal, err := typeConverter.ConvertSQLToArrow(val, arrowType)
	if err != nil {
		return fmt.Errorf("failed to convert SQL value to Arrow: %w", err)
	}
	
	// Handle NULL values
	if convertedVal == nil {
		builder.AppendNull()
		return nil
	}

	// Now append the converted value using the appropriate builder method
	// The TypeConverter has already done most of the work, we just need to call the right Append method
	switch b := builder.(type) {
	// Numeric types - using generic helpers
	case *array.Int8Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Int16Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Int32Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Int64Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Uint8Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Uint16Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Uint32Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Uint64Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Float32Builder:
		return appendNumericToBuilder(b, convertedVal)
	case *array.Float64Builder:
		return appendNumericToBuilder(b, convertedVal)

	// Boolean type
	case *array.BooleanBuilder:
		if boolVal, ok := convertedVal.(bool); ok {
			b.Append(boolVal)
		} else {
			return appendBooleanToBuilder(b, convertedVal)
		}

	// String types
	case array.StringLikeBuilder:
		if strVal, ok := convertedVal.(string); ok {
			b.Append(strVal)
		} else {
			return b.AppendValueFromString(fmt.Sprintf("%v", convertedVal))
		}

	// Binary types
	case array.BinaryLikeBuilder:
		if binVal, ok := convertedVal.([]byte); ok {
			b.Append(binVal)
		} else {
			return b.AppendValueFromString(fmt.Sprintf("%v", convertedVal))
		}

	// Date types - using generic helpers
	case *array.Date32Builder:
		return appendDateToBuilder(b, convertedVal,
			func(t time.Time) any {
				// Date32 stores days since epoch
				return arrow.Date32(t.Unix() / (24 * 3600))
			},
			func(converted any) { b.Append(converted.(arrow.Date32)) })
	case *array.Date64Builder:
		return appendDateToBuilder(b, convertedVal,
			func(t time.Time) any {
				// Date64 stores milliseconds since epoch
				return arrow.Date64(t.UnixMilli())
			},
			func(converted any) { b.Append(converted.(arrow.Date64)) })

	// Time types - using generic helpers
	case *array.Time32Builder:
		return appendTimeToBuilder(b, convertedVal,
			func() arrow.TimeUnit { return b.Type().(*arrow.Time32Type).Unit },
			func(t time.Time, unit arrow.TimeUnit) any {
				// Convert to time since midnight based on unit
				switch unit {
				case arrow.Second:
					return arrow.Time32(t.Hour()*3600 + t.Minute()*60 + t.Second())
				case arrow.Millisecond:
					return arrow.Time32(t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000)
				default:
					return arrow.Time32(t.Hour()*3600000 + t.Minute()*60000 + t.Second()*1000 + t.Nanosecond()/1000000)
				}
			},
			func(converted any) { b.Append(converted.(arrow.Time32)) })
	case *array.Time64Builder:
		return appendTimeToBuilder(b, convertedVal,
			func() arrow.TimeUnit { return b.Type().(*arrow.Time64Type).Unit },
			func(t time.Time, unit arrow.TimeUnit) any {
				// Convert to time since midnight based on unit
				switch unit {
				case arrow.Microsecond:
					return arrow.Time64(t.Hour()*3600000000 + t.Minute()*60000000 + t.Second()*1000000 + t.Nanosecond()/1000)
				case arrow.Nanosecond:
					return arrow.Time64(t.Hour()*3600000000000 + t.Minute()*60000000000 + t.Second()*1000000000 + t.Nanosecond())
				default:
					return arrow.Time64(t.Hour()*3600000000 + t.Minute()*60000000 + t.Second()*1000000 + t.Nanosecond()/1000)
				}
			},
			func(converted any) { b.Append(converted.(arrow.Time64)) })

	// Timestamp types - using generic helpers
	case *array.TimestampBuilder:
		return appendTimeToBuilder(b, convertedVal,
			func() arrow.TimeUnit { return b.Type().(*arrow.TimestampType).Unit },
			func(t time.Time, unit arrow.TimeUnit) any {
				switch unit {
				case arrow.Second:
					return arrow.Timestamp(t.Unix())
				case arrow.Millisecond:
					return arrow.Timestamp(t.UnixMilli())
				case arrow.Microsecond:
					return arrow.Timestamp(t.UnixMicro())
				case arrow.Nanosecond:
					return arrow.Timestamp(t.UnixNano())
				default:
					return arrow.Timestamp(t.UnixMicro())
				}
			},
			func(converted any) { b.Append(converted.(arrow.Timestamp)) })

	// Decimal types - use generic helper since TypeConverter returns raw value
	case *array.Decimal32Builder:
		return appendDecimalToBuilder(b, convertedVal)
	case *array.Decimal64Builder:
		return appendDecimalToBuilder(b, convertedVal)
	case *array.Decimal128Builder:
		return appendDecimalToBuilder(b, convertedVal)
	case *array.Decimal256Builder:
		return appendDecimalToBuilder(b, convertedVal)

	// Fallback for any unhandled builder types
	default:
		if stringAppender, ok := builder.(interface{ AppendValueFromString(string) error }); ok {
			return stringAppender.AppendValueFromString(fmt.Sprintf("%v", convertedVal))
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
	values      []any
	valuePtrs   []any
	schema      *arrow.Schema

	// For bind parameter support
	conn          *sql.Conn     // Database connection to execute queries
	query         string        // Original SQL query with placeholders
	stmt          *sql.Stmt     // Prepared statement (optional)
	typeConverter TypeConverter // Type converter for building schemas
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
		v, err := extractArrowValue(rec.Column(i), rowIdx, s.typeConverter)
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

// BeginAppending prepares for appending rows.
// This is called once before the first AppendRow call.
// Note: BaseRecordReader may need to call this again after schema changes in NextResultSet.
func (s *sqlRecordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	// No setup needed for now - we'll work directly with the RecordBuilder

	// TODO (https://github.com/adbc-drivers/driverbase-go/issues/29): Replace appendValue calls in AppendRow with per-column closure functions.
	//       For each column, generate a func(val any) error that captures the appropriate builder
	//       and performs type-safe appending. Store these in s.appendFuncs and call them in AppendRow.
	//       This eliminates repeated type switches and enables faster row appending.
	//       See https://github.com/apache/arrow-go/blob/main/arrow/csv/reader.go#L237 for an example of this pattern.
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
	for i := 0; i < len(s.values); i++ {
		fieldBuilder := builder.Field(i)
		if s.values[i] == nil {
			// Handle SQL NULL values
			fieldBuilder.AppendNull()
		} else {
			// Use the unified appendValue function to handle type conversion
			if err := appendValue(fieldBuilder, s.values[i], s.typeConverter); err != nil {
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
