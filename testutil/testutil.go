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

package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CheckedClose validates that a deferred Close call did not fail.
// See: https://github.com/stretchr/testify/issues/1067
func CheckedClose(t *testing.T, obj io.Closer) {
	if err := obj.Close(); err != nil {
		t.Errorf("Failed to close object of type %T: %s", obj, err)
	}
}

type CloserWithContext interface {
	Close(ctx context.Context) error
}

// CheckedCloseWithContext validates that a deferred Close call did not fail.
// See: https://github.com/stretchr/testify/issues/1067
func CheckedCloseWithContext(t *testing.T, obj CloserWithContext, ctx context.Context) {
	if err := obj.Close(ctx); err != nil {
		t.Errorf("Failed to close object of type %T: %s", obj, err)
	}
}

// ArrayFromJSON is the same as array.FromJSON, but fails the test on error.
func ArrayFromJSON(t *testing.T, mem memory.Allocator, dt arrow.DataType, json string) arrow.Array {
	record, _, err := array.FromJSON(mem, dt, bytes.NewReader([]byte(json)))
	if err != nil {
		t.Fatalf("failed to create array from JSON: %v", err)
	}
	return record
}

// RecordFromJSON is the same as array.RecordFromJSON, but fails the test on error.
func RecordFromJSON(t *testing.T, mem memory.Allocator, schema *arrow.Schema, json string) arrow.RecordBatch {
	record, _, err := array.RecordFromJSON(mem, schema, bytes.NewReader([]byte(json)))
	if err != nil {
		t.Fatalf("failed to create record from JSON: %v", err)
	}
	return record
}

// Statistic represents a single statistic extracted from ADBC GetStatistics results.
type Statistic struct {
	ColumnName     *string
	StatisticKey   int16
	StatisticValue any
	IsApproximate  bool
}

// ExtractStatisticsForTable extracts statistics for a specific table from GetStatistics result.
//
// This helper navigates the nested ADBC GetStatistics schema:
//
//	catalog_name: string
//	catalog_db_schemas: list<struct{
//	    db_schema_name: string
//	    db_schema_statistics: list<struct{
//	        table_name: string
//	        column_name: string (nullable)
//	        statistic_key: int16
//	        statistic_value: dense_union<int64, uint64, float64, binary>
//	        statistic_is_approximate: bool
//	    }>
//	}>
func ExtractStatisticsForTable(rec arrow.RecordBatch, catalog, schema, table string) []Statistic {
	catArr, ok := rec.Column(0).(*array.String)
	if !ok {
		panic(fmt.Sprintf("expected column 0 (catalog_name) to be String, got %T", rec.Column(0)))
	}
	schemaList, ok := rec.Column(1).(*array.List)
	if !ok {
		panic(fmt.Sprintf("expected column 1 (catalog_db_schemas) to be List, got %T", rec.Column(1)))
	}
	schemaStruct, ok := schemaList.ListValues().(*array.Struct)
	if !ok {
		panic(fmt.Sprintf("expected catalog_db_schemas list values to be Struct, got %T", schemaList.ListValues()))
	}
	dbSchemaNameArr, ok := schemaStruct.Field(0).(*array.String)
	if !ok {
		panic(fmt.Sprintf("expected db_schema_name field to be String, got %T", schemaStruct.Field(0)))
	}
	statsListArr, ok := schemaStruct.Field(1).(*array.List)
	if !ok {
		panic(fmt.Sprintf("expected db_schema_statistics field to be List, got %T", schemaStruct.Field(1)))
	}
	statsStruct, ok := statsListArr.ListValues().(*array.Struct)
	if !ok {
		panic(fmt.Sprintf("expected db_schema_statistics list values to be Struct, got %T", statsListArr.ListValues()))
	}
	tableNameArr, ok := statsStruct.Field(0).(*array.String)
	if !ok {
		panic(fmt.Sprintf("expected table_name field to be String, got %T", statsStruct.Field(0)))
	}
	columnNameArr, ok := statsStruct.Field(1).(*array.String)
	if !ok {
		panic(fmt.Sprintf("expected column_name field to be String, got %T", statsStruct.Field(1)))
	}
	statKeyArr, ok := statsStruct.Field(2).(*array.Int16)
	if !ok {
		panic(fmt.Sprintf("expected statistic_key field to be Int16, got %T", statsStruct.Field(2)))
	}
	statValueArr, ok := statsStruct.Field(3).(*array.DenseUnion)
	if !ok {
		panic(fmt.Sprintf("expected statistic_value field to be DenseUnion, got %T", statsStruct.Field(3)))
	}
	statApproxArr, ok := statsStruct.Field(4).(*array.Boolean)
	if !ok {
		panic(fmt.Sprintf("expected statistic_is_approximate field to be Boolean, got %T", statsStruct.Field(4)))
	}

	var results []Statistic

	for i := range rec.NumRows() {
		if catArr.IsNull(int(i)) || catArr.Value(int(i)) != catalog {
			continue
		}
		sStart, sEnd := schemaList.ValueOffsets(int(i))
		for j := int(sStart); j < int(sEnd); j++ {
			if dbSchemaNameArr.IsNull(j) || dbSchemaNameArr.Value(j) != schema {
				continue
			}
			stStart, stEnd := statsListArr.ValueOffsets(j)
			for k := int(stStart); k < int(stEnd); k++ {
				if tableNameArr.IsNull(k) || tableNameArr.Value(k) != table {
					continue
				}

				stat := Statistic{
					StatisticKey:  statKeyArr.Value(k),
					IsApproximate: statApproxArr.Value(k),
				}

				// Extract column name (nullable)
				if !columnNameArr.IsNull(k) {
					colName := columnNameArr.Value(k)
					stat.ColumnName = &colName
				}

				// Extract statistic value from dense union
				typeCode := statValueArr.TypeCode(k)
				valueOffset := int(statValueArr.ValueOffset(k))

				switch typeCode {
				case 0: // int64
					child := statValueArr.Field(0).(*array.Int64)
					stat.StatisticValue = child.Value(valueOffset)
				case 1: // uint64
					child := statValueArr.Field(1).(*array.Uint64)
					stat.StatisticValue = child.Value(valueOffset)
				case 2: // float64
					child := statValueArr.Field(2).(*array.Float64)
					stat.StatisticValue = child.Value(valueOffset)
				case 3: // binary
					child := statValueArr.Field(3).(*array.Binary)
					stat.StatisticValue = child.Value(valueOffset)
				}

				results = append(results, stat)
			}
		}
	}
	return results
}

// StatisticsToLookupMap converts a slice of statistics (from ExtractStatisticsForTable)
// into a lookup map for easier test assertions.
// Returns a map from statistic key to the full Statistic struct.
func StatisticsToLookupMap(stats []Statistic) map[int16]Statistic {
	result := make(map[int16]Statistic)
	for _, stat := range stats {
		result[stat.StatisticKey] = stat
	}
	return result
}
