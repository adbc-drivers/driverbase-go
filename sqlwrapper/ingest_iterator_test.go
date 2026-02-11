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
	"fmt"
	"testing"

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
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	int32Builder := builder.Field(0).(*array.Int32Builder)
	stringBuilder := builder.Field(1).(*array.StringBuilder)

	for i := range 5 {
		int32Builder.Append(int32(i))
		stringBuilder.Append("value" + string(rune('0'+i)))
	}

	rec := builder.NewRecordBatch()
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
	var batches []arrow.RecordBatch
	for batchIdx := range 3 {
		builder := array.NewRecordBuilder(mem, schema)
		int32Builder := builder.Field(0).(*array.Int32Builder)
		stringBuilder := builder.Field(1).(*array.StringBuilder)

		for i := range 4 {
			val := batchIdx*4 + i
			int32Builder.Append(int32(val))
			stringBuilder.Append(fmt.Sprintf("str%d", val))
		}

		rec := builder.NewRecordBatch()
		batches = append(batches, rec)
		builder.Release()
	}
	defer func() {
		for _, rec := range batches {
			rec.Release()
		}
	}()

	reader, err := array.NewRecordReader(schema, batches)
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
	batchSizes := []int{3, 5, 2, 4}
	var batches []arrow.RecordBatch
	rowCounter := 0

	for _, size := range batchSizes {
		builder := array.NewRecordBuilder(mem, schema)
		int32Builder := builder.Field(0).(*array.Int32Builder)
		stringBuilder := builder.Field(1).(*array.StringBuilder)

		for i := range size {
			val := rowCounter + i
			int32Builder.Append(int32(val))
			stringBuilder.Append(fmt.Sprintf("row%d", val))
		}

		rec := builder.NewRecordBatch()
		batches = append(batches, rec)
		builder.Release()
		rowCounter += size
	}
	defer func() {
		for _, rec := range batches {
			rec.Release()
		}
	}()

	reader, err := array.NewRecordReader(schema, batches)
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
