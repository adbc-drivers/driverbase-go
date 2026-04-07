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

// ExtractStatisticsForTable extracts statistics for a specific table from GetStatistics result.
// Returns a slice of maps, each containing statistic_key, statistic_value, statistic_is_approximate,
// and optionally column_name.
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
func ExtractStatisticsForTable(rec arrow.RecordBatch, catalog, schema, table string) []map[string]any {
	catArr, ok := rec.Column(0).(*array.String)
	if !ok {
		return nil
	}
	schemaList, ok := rec.Column(1).(*array.List)
	if !ok {
		return nil
	}
	schemaStruct, ok := schemaList.ListValues().(*array.Struct)
	if !ok {
		return nil
	}
	dbSchemaNameArr, ok := schemaStruct.Field(0).(*array.String)
	if !ok {
		return nil
	}
	statsListArr, ok := schemaStruct.Field(1).(*array.List)
	if !ok {
		return nil
	}
	statsStruct, ok := statsListArr.ListValues().(*array.Struct)
	if !ok {
		return nil
	}
	tableNameArr, ok := statsStruct.Field(0).(*array.String)
	if !ok {
		return nil
	}
	columnNameArr, ok := statsStruct.Field(1).(*array.String)
	if !ok {
		return nil
	}
	statKeyArr, ok := statsStruct.Field(2).(*array.Int16)
	if !ok {
		return nil
	}
	statValueArr, ok := statsStruct.Field(3).(*array.DenseUnion)
	if !ok {
		return nil
	}
	statApproxArr, ok := statsStruct.Field(4).(*array.Boolean)
	if !ok {
		return nil
	}

	var results []map[string]any

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

				stat := map[string]any{
					"statistic_key":            statKeyArr.Value(k),
					"statistic_is_approximate": statApproxArr.Value(k),
				}

				// Extract column name (nullable)
				if !columnNameArr.IsNull(k) {
					stat["column_name"] = columnNameArr.Value(k)
				}

				// Extract statistic value from dense union
				typeCode := statValueArr.TypeCode(k)
				valueOffset := int(statValueArr.ValueOffset(k))

				switch typeCode {
				case 0: // int64
					child := statValueArr.Field(0).(*array.Int64)
					stat["statistic_value"] = child.Value(valueOffset)
				case 1: // uint64
					child := statValueArr.Field(1).(*array.Uint64)
					stat["statistic_value"] = child.Value(valueOffset)
				case 2: // float64
					child := statValueArr.Field(2).(*array.Float64)
					stat["statistic_value"] = child.Value(valueOffset)
				case 3: // binary
					child := statValueArr.Field(3).(*array.Binary)
					stat["statistic_value"] = child.Value(valueOffset)
				}

				results = append(results, stat)
			}
		}
	}
	return results
}

// StatisticsToLookupMaps converts a slice of statistics maps (from ExtractStatisticsForTable)
// into lookup maps for easier test assertions. Each statistic in the input is expected to have:
//   - "statistic_key" (int16): The statistic identifier
//   - "statistic_value" (any): The statistic value (int64, uint64, float64, or []byte)
//   - "statistic_is_approximate" (bool): Whether the statistic is approximate
//
// Returns two maps:
//   - values: Maps statistic key to its value
//   - isApproximate: Maps statistic key to its approximate flag
func StatisticsToLookupMaps(stats []map[string]any) (
	values map[int16]any,
	isApproximate map[int16]bool,
) {
	values = make(map[int16]any)
	isApproximate = make(map[int16]bool)

	for _, stat := range stats {
		key := stat["statistic_key"].(int16)
		values[key] = stat["statistic_value"]
		isApproximate[key] = stat["statistic_is_approximate"].(bool)
	}

	return values, isApproximate
}
