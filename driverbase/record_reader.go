// Copyright (c) 2025 Columnar Technologies, Inc.
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

package driverbase

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecordReaderImpl is a row-wise implementation of a record reader.  The
// driverbase can pivot this into an array.RecordReader.
type RecordReaderImpl interface {
	io.Closer
	NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error)
	BeginAppending(builder *array.RecordBuilder) error
	// Return io.EOF if no more rows can be appended from the current result set
	AppendRow(builder *array.RecordBuilder) error
}

// BaseRecordReader is an array.RecordReader based on a row-wise interface.
// It manages ADBC requirements like re-issuing queries multiple times for
// each row of a bind parameter set.
type BaseRecordReader struct {
	refCount int64
	ctx      context.Context
	alloc    memory.Allocator
	// bind parameters or nil
	params array.RecordReader
	// rows in a batch to target
	batchSize int64
	impl      RecordReaderImpl
	builder   *array.RecordBuilder

	// The next record to be yielded
	record arrow.Record
	// All errors encountered
	err error
	// Current record containing bind parameters (if any)
	paramRecord arrow.Record
	// Current row index into paramRecord
	paramIndex int
	done       bool
}

// Init initializes the state for the record reader.
func (rr *BaseRecordReader) Init(ctx context.Context, alloc memory.Allocator, params array.RecordReader, batchSize int64, impl RecordReaderImpl) (err error) {
	rr.refCount = 1

	if ctx == nil {
		return errors.New("driverbase: BaseRecordReader: must provide ctx")
	} else if alloc == nil {
		return errors.New("driverbase: BaseRecordReader: must provide alloc")
	} else if batchSize == 0 {
		batchSize = 65536
	} else if batchSize < 0 {
		return errors.New("driverbase: BaseRecordReader: batchSize must be non-negative")
	}

	rr.ctx = ctx
	rr.alloc = alloc
	rr.params = params
	rr.batchSize = batchSize
	rr.impl = impl

	// Initialize the builder and get the first result set
	var schema *arrow.Schema
	if rr.params != nil {
		if !rr.advanceParams() {
			rr.Close()
			if rr.err != nil {
				return rr.err
			}
			// Parameters given but stream is empty => this reader
			// is an empty stream with an empty schema.

			// TODO(lidavidm): in theory we could still infer the
			// result set schema
			schema = arrow.NewSchema([]arrow.Field{}, nil)
		}
	}
	if schema == nil {
		schema, err = rr.impl.NextResultSet(rr.ctx, rr.paramRecord, rr.paramIndex)
		if err != nil {
			rr.err = err
			rr.Close()
			return err
		}
	}

	rr.builder = array.NewRecordBuilder(rr.alloc, schema)
	return rr.impl.BeginAppending(rr.builder)
}

func (rr *BaseRecordReader) Close() {
	if rr.record != nil {
		rr.record.Release()
		rr.record = nil
	}
	if rr.builder != nil {
		rr.builder.Release()
		rr.builder = nil
	}
	if rr.impl != nil {
		if err := rr.impl.Close(); err != nil {
			rr.err = errors.Join(rr.err, err)
		}
		rr.impl = nil
	}
	if rr.paramRecord != nil {
		rr.paramRecord.Release()
		rr.paramRecord = nil
	}
	if rr.params != nil {
		if err := rr.params.Err(); err != nil {
			rr.err = errors.Join(rr.err, err)
		}
		rr.params.Release()
		rr.params = nil
	}
}

func (rr *BaseRecordReader) Next() bool {
	if rr.impl == nil || rr.err != nil {
		return false
	}
	if rr.record != nil {
		rr.record.Release()
		rr.record = nil
	}
	if rr.done {
		rr.Close()
		return false
	}

	rows := int64(0)
	for rows < rr.batchSize {
		err := rr.impl.AppendRow(rr.builder)
		if err == io.EOF {
			// No more rows in this result set, advance to the
			// next one (if possible, this only happens if we have
			// bind parameters)
			if !rr.advanceParams() {
				// We are done.  We want to clean up resources
				// eagerly even if Release() isn't called
				// until later. We can't Close() immediately
				// here since we still need the
				// resources. Instead, set this flag so that
				// the next call to Next (the first call to
				// return false) will close the reader.
				rr.done = true
				break
			}

			_, err = rr.impl.NextResultSet(rr.ctx, rr.paramRecord, rr.paramIndex)
			if err != nil {
				rr.err = err
				return false
			}
			// TODO(lidavidm): validate that the schema from
			// NextResultSet is consistent
			continue
		} else if err != nil {
			rr.err = err
			return false
		}
		rows++
	}
	rr.record = rr.builder.NewRecord()
	if rows == 0 && rr.done {
		// N.B. I believe rows == 0 implies rr.done here
		// Clean up eagerly since we will return false below
		rr.Close()
	}
	return rows > 0
}

// advanceParams gets the next row of bind parameters, or returns false if no
// more are available. If there are no bind parameters, it returns false
// immediately.
func (rr *BaseRecordReader) advanceParams() bool {
	if rr.params == nil {
		return false
	}

	rr.paramIndex++
	// Must loop in case params yields a 0-row record
	for rr.paramRecord == nil || rr.paramIndex >= int(rr.paramRecord.NumRows()) {
		if rr.params.Next() {
			rr.paramRecord = rr.params.Record()
			rr.paramIndex = 0
		} else {
			return false
		}
	}
	// Don't check rr.params.Err() here, that's checked in Close()
	return rr.paramRecord != nil && rr.paramIndex < int(rr.paramRecord.NumRows())
}

func (rr *BaseRecordReader) Release() {
	if atomic.AddInt64(&rr.refCount, -1) == 0 {
		rr.Close()
	}
}

func (rr *BaseRecordReader) Retain() {
	atomic.AddInt64(&rr.refCount, 1)
}

func (rr *BaseRecordReader) Schema() *arrow.Schema {
	return rr.builder.Schema()
}

func (rr *BaseRecordReader) Record() arrow.Record {
	return rr.record
}

func (rr *BaseRecordReader) Err() error {
	return rr.err
}
