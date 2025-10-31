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

var ErrOverflow = errors.New("driverbase: value overflow")

// RecordReaderImpl is a row-wise implementation of a record reader.  The
// driverbase can pivot this into an array.RecordReader.
type RecordReaderImpl interface {
	io.Closer
	// AppendRow adds a row of the current result set to the record
	// builder. Return io.EOF if no more rows can be appended from the
	// current result set. Return an estimate of row size. If ErrOverflow
	// is returned, end the current batch immediately. It is assumed the
	// reader can retry again on the next call.
	AppendRow(builder *array.RecordBuilder) (int64, error)
	// BeginAppending is called exactly once before the first call to
	// AppendRow. The implementation can do any necessary initialization
	// here. It will be called after the first call to NextResultSet.
	BeginAppending(builder *array.RecordBuilder) error
	// NextResultSet closes the current result set and opens the next
	// result set for the given parameters. If there are no parameters, it
	// will be called exactly once with rec == nil.
	NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error)
}

// BaseRecordReader is an array.RecordReader based on a row-wise interface.
// It manages ADBC requirements like re-issuing queries multiple times for
// each row of a bind parameter set.
type BaseRecordReader struct {
	refCount int64
	ctx      context.Context
	alloc    memory.Allocator
	// bind parameters or nil
	params  array.RecordReader
	options BaseRecordReaderOptions
	impl    RecordReaderImpl
	schema  *arrow.Schema
	builder *array.RecordBuilder

	// The next nextBatch to be yielded
	nextBatch arrow.RecordBatch
	// All errors encountered
	err error
	// Current record containing bind parameters (if any)
	paramBatch arrow.RecordBatch
	// Current row index into paramRecord
	paramIndex int
	done       bool
}

type BaseRecordReaderOptions struct {
	BatchByteLimit int64
	BatchRowLimit  int64
}

func (options *BaseRecordReaderOptions) validate() error {
	if options.BatchRowLimit < 0 {
		return errors.New("driverbase: BaseRecordReaderOptions: BatchRowLimit must be non-negative")
	} else if options.BatchRowLimit == 0 {
		options.BatchRowLimit = 65536
	}
	return nil
}

// Init initializes the state for the record reader.
func (rr *BaseRecordReader) Init(ctx context.Context, alloc memory.Allocator, params array.RecordReader, options BaseRecordReaderOptions, impl RecordReaderImpl) (err error) {
	rr.refCount = 1

	if ctx == nil {
		return errors.New("driverbase: BaseRecordReader: must provide ctx")
	} else if alloc == nil {
		return errors.New("driverbase: BaseRecordReader: must provide alloc")
	} else if impl == nil {
		return errors.New("driverbase: BaseRecordReader: must provide impl")
	} else if err := options.validate(); err != nil {
		return err
	}

	rr.ctx = ctx
	rr.alloc = alloc
	rr.params = params
	rr.options = options
	rr.impl = impl

	// Initialize the builder and get the first result set
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
			rr.schema = arrow.NewSchema([]arrow.Field{}, nil)
			return nil
		}
	}
	rr.schema, err = rr.impl.NextResultSet(rr.ctx, rr.paramBatch, rr.paramIndex)
	if err != nil {
		rr.err = err
		rr.Close()
		return err
	}

	rr.builder = array.NewRecordBuilder(rr.alloc, rr.schema)
	return rr.impl.BeginAppending(rr.builder)
}

func (rr *BaseRecordReader) Close() {
	if rr.nextBatch != nil {
		rr.nextBatch.Release()
		rr.nextBatch = nil
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
	if rr.nextBatch != nil {
		rr.nextBatch.Release()
		rr.nextBatch = nil
	}
	if rr.done {
		rr.Close()
		return false
	}

	rows := int64(0)
	batchSize := int64(0)
	for rows < rr.options.BatchRowLimit {
		size, err := rr.impl.AppendRow(rr.builder)
		if err == ErrOverflow {
			break
		} else if err == io.EOF {
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

			_, err = rr.impl.NextResultSet(rr.ctx, rr.paramBatch, rr.paramIndex)
			if err != nil {
				rr.err = err
				// TODO: close here?
				return false
			}
			// TODO(lidavidm): validate that the schema from
			// NextResultSet is consistent
			continue
		} else if err != nil {
			rr.err = err
			// TODO: close here?
			return false
		}
		rows++
		batchSize += size

		if rr.options.BatchByteLimit > 0 && batchSize >= rr.options.BatchByteLimit {
			break
		}
	}
	rr.nextBatch = rr.builder.NewRecordBatch()
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
	for rr.paramBatch == nil || rr.paramIndex >= int(rr.paramBatch.NumRows()) {
		if rr.params.Next() {
			rr.paramBatch = rr.params.RecordBatch()
			rr.paramIndex = 0
		} else {
			return false
		}
	}
	// Don't check rr.params.Err() here, that's checked in Close()
	return rr.paramBatch != nil && rr.paramIndex < int(rr.paramBatch.NumRows())
}

func (rr *BaseRecordReader) Release() {
	newCount := atomic.AddInt64(&rr.refCount, -1)
	if newCount == 0 {
		rr.Close()
	}
	DebugAssert(newCount >= 0, "refCount went negative in BaseRecordReader")
}

func (rr *BaseRecordReader) Retain() {
	atomic.AddInt64(&rr.refCount, 1)
}

func (rr *BaseRecordReader) Schema() *arrow.Schema {
	return rr.schema
}

func (rr *BaseRecordReader) Record() arrow.RecordBatch {
	return rr.nextBatch
}

func (rr *BaseRecordReader) RecordBatch() arrow.RecordBatch {
	return rr.nextBatch
}

func (rr *BaseRecordReader) Err() error {
	return rr.err
}

var _ array.RecordReader = &BaseRecordReader{}
