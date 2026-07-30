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
	"sync/atomic"

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
	// Do not use array.ReaderFromIter here. It uses iter.Pull, which can panic
	// when the underlying reader calls into Go from cgo. See
	// https://github.com/golang/go/issues/67499.

	// You would see something like the following:

	// coro: got thread 0x2550e0bc5808, want 0x2550e0bc4008
	// coro: got lock internal 0, want 2
	// coro: got lock external 0, want 0
	// fatal error: coro: OS thread locking must match locking at coroutine creation
	//
	// runtime stack:
	// runtime.throw({0x7f0f8e9a84fb?, 0x7f0f8d180337?})
	// 	/opt/hostedtoolcache/go/1.26.4/x64/src/runtime/panic.go:1229 +0x4a fp=0x7f0f3effdda8 sp=0x7f0f3effdd78 pc=0x7f0f8d1b3a4a
	// runtime.coroswitch_m(0x2550e0dac3c0?)
	// 	/opt/hostedtoolcache/go/1.26.4/x64/src/runtime/coro.go:128 +0x54e fp=0x7f0f3effde38 sp=0x7f0f3effdda8 pc=0x7f0f8d147fee
	// runtime.mcall()
	// 	/opt/hostedtoolcache/go/1.26.4/x64/src/runtime/asm_amd64.s:496 +0x57 fp=0x7f0f3effde50 sp=0x7f0f3effde38 pc=0x7f0f8d1b9a17
	//
	// goroutine 2928 gp=0x2550e0dac3c0 m=4 mp=0x2550e0bc5808 [running]:
	// runtime.coroswitch(0x7f0f8d447cc2?)
	// 	/opt/hostedtoolcache/go/1.26.4/x64/src/runtime/coro.go:94 +0x47 fp=0x2550e0dd5de8 sp=0x2550e0dd5dc8 pc=0x7f0f8d1b0c87
	// iter.Pull2[...].func2()
	// 	/opt/hostedtoolcache/go/1.26.4/x64/src/iter/iter.go:434 +0x68 fp=0x2550e0dd5e28 sp=0x2550e0dd5de8 pc=0x7f0f8d476828
	// github.com/apache/arrow-go/v18/arrow/array.(*iterReader).Next(0x2550e0e162c0)
	// 	/home/runner/go/pkg/mod/github.com/apache/arrow-go/v18@v18.7.0/arrow/array/record.go:543 +0x3d fp=0x2550e0dd5e40 sp=0x2550e0dd5e28 pc=0x7f0f8d44989d

	decoded := &dictDecodeRecordReader{
		ctx:       ctx,
		schema:    schema,
		errHelper: errHelper,
		reader:    reader,
	}
	decoded.refCount.Add(1)
	return decoded
}

type dictDecodeRecordReader struct {
	refCount atomic.Int64

	ctx       context.Context
	schema    *arrow.Schema
	errHelper *driverbase.ErrorHelper
	reader    array.RecordReader
	current   arrow.RecordBatch
	err       error
}

var _ array.RecordReader = (*dictDecodeRecordReader)(nil)

func (r *dictDecodeRecordReader) Retain() {
	r.refCount.Add(1)
}

func (r *dictDecodeRecordReader) Release() {
	newCount := r.refCount.Add(-1)
	if newCount == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		r.releaseReader()
	}
	driverbase.DebugAssert(newCount >= 0, "refCount went negative in dictDecodeRecordReader")
}

func (r *dictDecodeRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *dictDecodeRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.reader == nil {
		return false
	}

	if !r.reader.Next() {
		r.err = r.reader.Err()
		r.releaseReader()
		return false
	}

	decoded, err := DictDecodeRecordBatch(r.ctx, r.schema, r.reader.RecordBatch())
	if err != nil {
		r.err = r.errHelper.WrapInternal(err, "decode dictionary batch")
		r.releaseReader()
		return false
	}
	r.current = decoded
	return true
}

func (r *dictDecodeRecordReader) RecordBatch() arrow.RecordBatch {
	return r.current
}

// Deprecated: Use [dictDecodeRecordReader.RecordBatch] instead.
func (r *dictDecodeRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *dictDecodeRecordReader) Err() error {
	return r.err
}

func (r *dictDecodeRecordReader) releaseReader() {
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}
}
