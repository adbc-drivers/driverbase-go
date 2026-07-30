// Copyright (c) 2026 ADBC Drivers Contributors
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

package arrowext_test

import (
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/driverbase/arrowext"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDictDecodeSchema(t *testing.T) {
	var schema *arrow.Schema
	var newSchema *arrow.Schema
	var hadDictionaries bool

	schema = arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	newSchema, hadDictionaries = arrowext.DictDecodeSchema(schema)
	assert.False(t, hadDictionaries)
	assert.Equal(t, schema, newSchema)

	schema = arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, Nullable: true},
	}, nil)
	newSchema, hadDictionaries = arrowext.DictDecodeSchema(schema)
	assert.True(t, hadDictionaries)
	assert.NotEqual(t, schema, newSchema)
	assert.Equal(t, arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil), newSchema)
}

func TestDictDecodeRecordReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	eh := &driverbase.ErrorHelper{DriverName: "test"}

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	dict, err := array.DictArrayFromJSON(mem, dictType, `[0, 1, null]`, `["foo", "bar"]`)
	require.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: dictType, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{dict}, int64(dict.Len()))
	dict.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record, record})
	record.Release()
	require.NoError(t, err)

	decoded := arrowext.DictDecodeRecordReader(mem, eh, reader)
	reader.Release()
	defer decoded.Release()

	require.True(t, decoded.Next())
	values := decoded.RecordBatch().Column(0).(*array.String)
	assert.Equal(t, "foo", values.Value(0))
	assert.Equal(t, "bar", values.Value(1))
	assert.True(t, values.IsNull(2))

	require.True(t, decoded.Next())
	values = decoded.RecordBatch().Column(0).(*array.String)
	assert.Equal(t, "foo", values.Value(0))
	assert.Equal(t, "bar", values.Value(1))
	assert.True(t, values.IsNull(2))

	assert.False(t, decoded.Next())
	require.NoError(t, decoded.Err())
}
