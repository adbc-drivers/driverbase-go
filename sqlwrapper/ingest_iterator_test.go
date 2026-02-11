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
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowBufferIteratorInit(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	t.Run("nil reader", func(t *testing.T) {
		_, err := NewRowBufferIterator(nil, 10, DefaultTypeConverter{})
		assert.ErrorContains(t, err, "reader cannot be nil")
	})

	t.Run("invalid batch size", func(t *testing.T) {
		reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
		require.NoError(t, err)
		defer reader.Release()

		_, err = NewRowBufferIterator(reader, 0, DefaultTypeConverter{})
		assert.ErrorContains(t, err, "batchSize must be positive")

		_, err = NewRowBufferIterator(reader, -1, DefaultTypeConverter{})
		assert.ErrorContains(t, err, "batchSize must be positive")
	})

	t.Run("valid initialization", func(t *testing.T) {
		reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
		require.NoError(t, err)
		defer reader.Release()

		iter, err := NewRowBufferIterator(reader, 10, DefaultTypeConverter{})
		require.NoError(t, err)
		assert.NotNil(t, iter)
	})
}

func TestRowBufferIteratorEmpty(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
	require.NoError(t, err)
	defer reader.Release()

	iter, err := NewRowBufferIterator(reader, 10, DefaultTypeConverter{})
	require.NoError(t, err)

	// Empty reader should return false immediately
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestRowBufferIteratorSingleBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "string", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create a single Arrow batch with 5 rows
	rec := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 0, "string": "value0"},
		{"int32": 1, "string": "value1"},
		{"int32": 2, "string": "value2"},
		{"int32": 3, "string": "value3"},
		{"int32": 4, "string": "value4"}
	]`)
	defer rec.Release()

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer reader.Release()

	// SQL batch size of 10, Arrow batch has 5 rows
	iter, err := NewRowBufferIterator(reader, 10, DefaultTypeConverter{})
	require.NoError(t, err)

	// Should get one batch with 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount := iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	// Verify data
	assert.Equal(t, int32(0), buffer[0])
	assert.Equal(t, "value0", buffer[1])
	assert.Equal(t, int32(4), buffer[8])
	assert.Equal(t, "value4", buffer[9])

	// No more batches
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestRowBufferIteratorCrossBatchBoundary(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "string", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create 3 Arrow batches with 4 rows each (total 12 rows)
	batch1 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 0, "string": "str0"},
		{"int32": 1, "string": "str1"},
		{"int32": 2, "string": "str2"},
		{"int32": 3, "string": "str3"}
	]`)
	defer batch1.Release()

	batch2 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 4, "string": "str4"},
		{"int32": 5, "string": "str5"},
		{"int32": 6, "string": "str6"},
		{"int32": 7, "string": "str7"}
	]`)
	defer batch2.Release()

	batch3 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 8, "string": "str8"},
		{"int32": 9, "string": "str9"},
		{"int32": 10, "string": "str10"},
		{"int32": 11, "string": "str11"}
	]`)
	defer batch3.Release()

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch1, batch2, batch3})
	require.NoError(t, err)
	defer reader.Release()

	// SQL batch size of 5, Arrow batches have 4 rows each
	// Should produce: [5 rows], [5 rows], [2 rows]
	iter, err := NewRowBufferIterator(reader, 5, DefaultTypeConverter{})
	require.NoError(t, err)

	// First SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount := iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(0), buffer[0])
	assert.Equal(t, "str0", buffer[1])
	assert.Equal(t, int32(1), buffer[2])
	assert.Equal(t, "str1", buffer[3])
	assert.Equal(t, int32(2), buffer[4])
	assert.Equal(t, "str2", buffer[5])
	assert.Equal(t, int32(3), buffer[6])
	assert.Equal(t, "str3", buffer[7])
	assert.Equal(t, int32(4), buffer[8])
	assert.Equal(t, "str4", buffer[9])

	// Second SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(5), buffer[0])
	assert.Equal(t, "str5", buffer[1])
	assert.Equal(t, int32(6), buffer[2])
	assert.Equal(t, "str6", buffer[3])
	assert.Equal(t, int32(7), buffer[4])
	assert.Equal(t, "str7", buffer[5])
	assert.Equal(t, int32(8), buffer[6])
	assert.Equal(t, "str8", buffer[7])
	assert.Equal(t, int32(9), buffer[8])
	assert.Equal(t, "str9", buffer[9])

	// Third SQL batch: 2 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 2, rowCount)
	assert.Len(t, buffer, 4) // 2 rows * 2 columns

	assert.Equal(t, int32(10), buffer[0])
	assert.Equal(t, "str10", buffer[1])
	assert.Equal(t, int32(11), buffer[2])
	assert.Equal(t, "str11", buffer[3])

	// No more batches
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestRowBufferIteratorUnevenBatches(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "string", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create Arrow batches with UNEVEN row counts: 3, 5, 2, 4 (total 14 rows)
	batch1 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 0, "string": "row0"},
		{"int32": 1, "string": "row1"},
		{"int32": 2, "string": "row2"}
	]`)
	defer batch1.Release()

	batch2 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 3, "string": "row3"},
		{"int32": 4, "string": "row4"},
		{"int32": 5, "string": "row5"},
		{"int32": 6, "string": "row6"},
		{"int32": 7, "string": "row7"}
	]`)
	defer batch2.Release()

	batch3 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 8, "string": "row8"},
		{"int32": 9, "string": "row9"}
	]`)
	defer batch3.Release()

	batch4 := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 10, "string": "row10"},
		{"int32": 11, "string": "row11"},
		{"int32": 12, "string": "row12"},
		{"int32": 13, "string": "row13"}
	]`)
	defer batch4.Release()

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch1, batch2, batch3, batch4})
	require.NoError(t, err)
	defer reader.Release()

	// SQL batch size of 5, Arrow batches have uneven rows: 3, 5, 2, 4
	// Should produce: [5 rows], [5 rows], [4 rows]
	iter, err := NewRowBufferIterator(reader, 5, DefaultTypeConverter{})
	require.NoError(t, err)

	// First SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount := iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(0), buffer[0])
	assert.Equal(t, "row0", buffer[1])
	assert.Equal(t, int32(1), buffer[2])
	assert.Equal(t, "row1", buffer[3])
	assert.Equal(t, int32(2), buffer[4])
	assert.Equal(t, "row2", buffer[5])
	assert.Equal(t, int32(3), buffer[6])
	assert.Equal(t, "row3", buffer[7])
	assert.Equal(t, int32(4), buffer[8])
	assert.Equal(t, "row4", buffer[9])

	// Second SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(5), buffer[0])
	assert.Equal(t, "row5", buffer[1])
	assert.Equal(t, int32(6), buffer[2])
	assert.Equal(t, "row6", buffer[3])
	assert.Equal(t, int32(7), buffer[4])
	assert.Equal(t, "row7", buffer[5])
	assert.Equal(t, int32(8), buffer[6])
	assert.Equal(t, "row8", buffer[7])
	assert.Equal(t, int32(9), buffer[8])
	assert.Equal(t, "row9", buffer[9])

	// Third SQL batch: 4 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 4, rowCount)
	assert.Len(t, buffer, 8) // 4 rows * 2 columns

	assert.Equal(t, int32(10), buffer[0])
	assert.Equal(t, "row10", buffer[1])
	assert.Equal(t, int32(11), buffer[2])
	assert.Equal(t, "row11", buffer[3])
	assert.Equal(t, int32(12), buffer[4])
	assert.Equal(t, "row12", buffer[5])
	assert.Equal(t, int32(13), buffer[6])
	assert.Equal(t, "row13", buffer[7])

	// No more batches
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}

func TestRowBufferIteratorSplitLargeBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int32", Type: arrow.PrimitiveTypes.Int32},
		{Name: "string", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create a single large Arrow batch with 12 rows
	rec := testutil.RecordFromJSON(t, mem, schema, `[
		{"int32": 0, "string": "val0"},
		{"int32": 1, "string": "val1"},
		{"int32": 2, "string": "val2"},
		{"int32": 3, "string": "val3"},
		{"int32": 4, "string": "val4"},
		{"int32": 5, "string": "val5"},
		{"int32": 6, "string": "val6"},
		{"int32": 7, "string": "val7"},
		{"int32": 8, "string": "val8"},
		{"int32": 9, "string": "val9"},
		{"int32": 10, "string": "val10"},
		{"int32": 11, "string": "val11"}
	]`)
	defer rec.Release()

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer reader.Release()

	// SQL batch size of 5, Arrow batch has 12 rows
	// Should split into: [5 rows], [5 rows], [2 rows]
	iter, err := NewRowBufferIterator(reader, 5, DefaultTypeConverter{})
	require.NoError(t, err)

	// First SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount := iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(0), buffer[0])
	assert.Equal(t, "val0", buffer[1])
	assert.Equal(t, int32(1), buffer[2])
	assert.Equal(t, "val1", buffer[3])
	assert.Equal(t, int32(2), buffer[4])
	assert.Equal(t, "val2", buffer[5])
	assert.Equal(t, int32(3), buffer[6])
	assert.Equal(t, "val3", buffer[7])
	assert.Equal(t, int32(4), buffer[8])
	assert.Equal(t, "val4", buffer[9])

	// Second SQL batch: 5 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 5, rowCount)
	assert.Len(t, buffer, 10) // 5 rows * 2 columns

	assert.Equal(t, int32(5), buffer[0])
	assert.Equal(t, "val5", buffer[1])
	assert.Equal(t, int32(6), buffer[2])
	assert.Equal(t, "val6", buffer[3])
	assert.Equal(t, int32(7), buffer[4])
	assert.Equal(t, "val7", buffer[5])
	assert.Equal(t, int32(8), buffer[6])
	assert.Equal(t, "val8", buffer[7])
	assert.Equal(t, int32(9), buffer[8])
	assert.Equal(t, "val9", buffer[9])

	// Third SQL batch: 2 rows
	assert.True(t, iter.Next())
	buffer, rowCount = iter.CurrentBatch()
	assert.Equal(t, 2, rowCount)
	assert.Len(t, buffer, 4) // 2 rows * 2 columns

	assert.Equal(t, int32(10), buffer[0])
	assert.Equal(t, "val10", buffer[1])
	assert.Equal(t, int32(11), buffer[2])
	assert.Equal(t, "val11", buffer[3])

	// No more batches
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
}
