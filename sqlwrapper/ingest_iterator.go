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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// RowBufferIterator accumulates rows from an Arrow RecordReader into fixed-size
// batches of Go values, crossing Arrow batch boundaries transparently.
type RowBufferIterator struct {
	reader          array.RecordReader
	typeConverter   TypeConverter
	batchSize       int                 // Target SQL batch size (from options.IngestBatchSize)
	numCols         int                 // Number of columns in schema
	buffer          []any               // Pre-allocated buffer: [batchSize * numCols]
	currentSize     int                 // Number of rows currently in buffer (0 to batchSize)
	currentBatch    arrow.RecordBatch   // Current Arrow batch being processed
	currentRow      int                 // Next row to read from currentBatch
	retainedBatches []arrow.RecordBatch // Batches retained for current SQL batch
	done            bool                // True when stream is exhausted
	err             error               // First error encountered
}

// NewRowBufferIterator creates a new iterator that accumulates rows into fixed-size batches.
//
// Parameters:
//   - reader: Arrow RecordReader to consume
//   - batchSize: Target number of rows per SQL batch
//   - typeConverter: Converts Arrow values to Go values
//
// Returns error if batchSize <= 0 or reader is nil.
func NewRowBufferIterator(
	reader array.RecordReader,
	batchSize int,
	typeConverter TypeConverter,
) (*RowBufferIterator, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("batchSize must be positive, got %d", batchSize)
	}

	numCols := len(reader.Schema().Fields())

	return &RowBufferIterator{
		reader:          reader,
		typeConverter:   typeConverter,
		batchSize:       batchSize,
		numCols:         numCols,
		buffer:          make([]any, batchSize*numCols),
		currentSize:     0,
		currentBatch:    nil,
		currentRow:      0,
		retainedBatches: make([]arrow.RecordBatch, 0, 4),
		done:            false,
		err:             nil,
	}, nil
}

// Next fills the buffer with up to batchSize rows, crossing Arrow batch boundaries.
// Returns true if a batch is available (could be full or partial), false if stream is exhausted.
func (it *RowBufferIterator) Next() bool {
	if it.err != nil || it.done {
		return false
	}

	// Reset buffer position for reuse
	it.currentSize = 0
	schema := it.reader.Schema()

	// Build new list of batches we'll use in this iteration
	newRetainedBatches := make([]arrow.RecordBatch, 0, cap(it.retainedBatches))
	newRetainedSet := make(map[arrow.RecordBatch]bool)
	alreadyRetained := make(map[arrow.RecordBatch]bool)

	// Mark batches from previous iteration as "already retained"
	for _, batch := range it.retainedBatches {
		alreadyRetained[batch] = true
	}

	// Accumulate rows until buffer is full
	for it.currentSize < it.batchSize {
		if it.currentBatch == nil || it.currentRow >= int(it.currentBatch.NumRows()) {
			if !it.advanceArrowBatch() {
				// Stream exhausted
				break
			}
		}

		// Copy rows from current Arrow batch to buffer
		rowsAvailable := int(it.currentBatch.NumRows()) - it.currentRow
		rowsNeeded := it.batchSize - it.currentSize
		rowsToCopy := min(rowsAvailable, rowsNeeded)

		// Track this batch for the new retained list
		if !newRetainedSet[it.currentBatch] {
			if !alreadyRetained[it.currentBatch] {
				it.currentBatch.Retain()
			}
			newRetainedBatches = append(newRetainedBatches, it.currentBatch)
			newRetainedSet[it.currentBatch] = true
		}

		for colIdx := range it.numCols {
			arr := it.currentBatch.Column(colIdx)
			field := schema.Field(colIdx)

			for i := range rowsToCopy {
				arrowRowIdx := it.currentRow + i
				bufferRowIdx := it.currentSize + i

				value, err := it.typeConverter.ConvertArrowToGo(arr, arrowRowIdx, &field)
				if err != nil {
					it.err = fmt.Errorf("failed to convert row %d, col %d (%s): %w",
						arrowRowIdx, colIdx, field.Name, err)
					return false
				}

				// Store in buffer (row-major layout)
				it.buffer[bufferRowIdx*it.numCols+colIdx] = value
			}
		}

		it.currentSize += rowsToCopy
		it.currentRow += rowsToCopy
	}

	// Release batches from previous iteration that we're no longer using
	for _, batch := range it.retainedBatches {
		if !newRetainedSet[batch] {
			batch.Release()
		}
	}

	// Update retained batches list for next iteration
	it.retainedBatches = newRetainedBatches

	// Return true if we have any data (even partial batch at end)
	return it.currentSize > 0
}

// advanceArrowBatch moves to the next Arrow batch from the reader.
// Returns false if no more batches available.
func (it *RowBufferIterator) advanceArrowBatch() bool {
	if !it.reader.Next() {
		// Check for stream errors
		if err := it.reader.Err(); err != nil {
			it.err = err
		}
		it.done = true
		return false
	}

	it.currentBatch = it.reader.RecordBatch()
	it.currentRow = 0
	return true
}

// CurrentBatch returns the current batch of Go values and its size.
// The returned slice is a view into the internal buffer and will be
// overwritten on the next call to Next().
//
// Returns:
//   - buffer: []any slice containing rowCount * numCols values
//   - rowCount: Number of complete rows in the batch (may be < batchSize at end)
//
// The buffer uses row-major layout: [row0_col0, row0_col1, ..., rowN_colM]
func (it *RowBufferIterator) CurrentBatch() (buffer []any, rowCount int) {
	// Return view of buffer containing only filled rows
	return it.buffer[:it.currentSize*it.numCols], it.currentSize
}

// Err returns the first error encountered during iteration.
// Should be called after Next() returns false.
func (it *RowBufferIterator) Err() error {
	return it.err
}

// Close releases any retained Arrow batches. This should be called if iteration
// is aborted before completion, or to explicitly release resources.
// Note: Next() automatically releases batches from the previous iteration,
// so Close() is only needed for cleanup in error cases.
func (it *RowBufferIterator) Close() {
	for _, batch := range it.retainedBatches {
		batch.Release()
	}
	it.retainedBatches = nil
}
