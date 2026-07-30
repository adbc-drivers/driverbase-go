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

package arrowext

import (
	"context"
	"fmt"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DictDecodeSchema returns a new schema with all top-level dictionary-encoded
// fields replaced with their value types. If no fields are
// dictionary-encoded, the original schema is returned and the second return
// value is false.
func DictDecodeSchema(schema *arrow.Schema) (*arrow.Schema, bool) {
	fields := schema.Fields() // creates copy of fields
	hasDictionary := false
	for i := range fields {
		if fields[i].Type.ID() == arrow.DICTIONARY {
			fields[i].Type = fields[i].Type.(*arrow.DictionaryType).ValueType
			hasDictionary = true
		}
	}
	if !hasDictionary {
		return schema, false
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata), true
}

// DictDecodeRecordBatch decodes all top-level dictionary-encoded columns in a
// record batch. The caller MUST release the returned record batch. The input
// record batch is NOT released.
func DictDecodeRecordBatch(ctx context.Context, schema *arrow.Schema, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	columns := batch.Columns()
	decoded := make([]arrow.Array, len(columns))
	copy(decoded, columns)

	for i, column := range columns {
		if column.DataType().ID() != arrow.DICTIONARY {
			continue
		}

		var err error
		decoded[i], err = compute.CastArray(ctx, column, compute.SafeCastOptions(schema.Field(i).Type))
		if err != nil {
			for j := range i {
				if decoded[j] != columns[j] {
					decoded[j].Release()
				}
			}
			return nil, fmt.Errorf("could not decode dictionary for `%s`: %w", schema.Field(i).Name, err)
		}
	}

	metadata := arrow.Metadata{}
	if batchWithMetadata, ok := batch.(arrow.RecordBatchWithMetadata); ok {
		metadata = batchWithMetadata.Metadata()
	}
	record := array.NewRecordBatchWithMetadata(schema, decoded, batch.NumRows(), metadata)
	for i := range decoded {
		if decoded[i] != columns[i] {
			decoded[i].Release()
		}
	}
	return record, nil
}

// DictDecodeRecordReader wraps an [array.RecordReader] and decodes top-level
// dictionary-encoded columns. The caller MUST release the input reader. The
// caller MUST release the output reader.
func DictDecodeRecordReader(mem memory.Allocator, errHelper *driverbase.ErrorHelper, reader array.RecordReader) array.RecordReader {
	reader.Retain()

	schema, hasDictionary := DictDecodeSchema(reader.Schema())
	if !hasDictionary {
		return reader
	}

	ctx := compute.WithAllocator(context.Background(), mem)
	return array.ReaderFromIter(schema, func(yield func(arrow.RecordBatch, error) bool) {
		defer reader.Release()
		for reader.Next() {
			batch := reader.RecordBatch()
			decoded, err := DictDecodeRecordBatch(ctx, schema, batch)
			if err != nil {
				err = errHelper.WrapInternal(err, "decode dictionary batch")
				yield(nil, err)
				return
			}
			if !yield(decoded, nil) {
				return
			}
		}
		if err := reader.Err(); err != nil {
			yield(nil, err)
		}
	})
}
