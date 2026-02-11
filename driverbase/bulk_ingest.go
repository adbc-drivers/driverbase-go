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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"golang.org/x/sync/errgroup"
)

const (
	// OptionKeyIngestBatchSize controls rows per INSERT during batched bulk ingest
	// Value of 0 means driver default applies.
	OptionKeyIngestBatchSize = "adbc.statement.ingest.batch_size"

	// OptionKeyIngestMaxQuerySizeBytes controls maximum SQL query size in bytes for batched bulk ingest
	// When set, the batch size is calculated to keep the generated INSERT query under this limit.
	// Value of 0 means driver default applies.
	OptionKeyIngestMaxQuerySizeBytes = "adbc.statement.ingest.max_query_size_bytes"
)

// WriterProps holds properties for writing data files to be ingested.
type WriterProps struct {
	// A target file size in bytes.
	MaxBytes           int64
	ParquetWriterProps *parquet.WriterProperties
	ArrowWriterProps   pqarrow.ArrowWriterProperties
	ArrowIpcProps      []ipc.Option
}

// Common options for bulk ingestion.
type BulkIngestOptions struct {
	// The table to ingest data into.
	TableName   string
	SchemaName  string
	CatalogName string
	// If true, use a temporary table.  The catalog/schema, if specified, will likely
	// be ignored (as temporary tables generally get implemented via a special
	// catalog/schema).
	Temporary bool
	// The ingest mode.
	Mode string
	// How far to read ahead on the data source
	ReadDepth int
	// How many parallel writers to use
	WriterParallelism int
	// How many parallel uploaders to use
	UploaderParallelism int
	// How many buffers to queue at once
	MaxPendingBuffers int
	// Format-specific options.
	WriterProps WriterProps
	// IngestBatchSize controls rows per INSERT during batched ingestion (0 means driver default)
	IngestBatchSize int
	// MaxQuerySizeBytes controls maximum SQL query size in bytes (0 means driver default)
	MaxQuerySizeBytes int
}

func NewBulkIngestOptions() BulkIngestOptions {
	return BulkIngestOptions{
		TableName:           "",
		Mode:                adbc.OptionValueIngestModeCreate,
		ReadDepth:           5,
		WriterParallelism:   2,
		MaxPendingBuffers:   2,
		UploaderParallelism: 2,
		WriterProps: WriterProps{
			MaxBytes:           10 * 1024 * 1024, // 10MiB
			ParquetWriterProps: parquet.NewWriterProperties(),
			ArrowWriterProps:   pqarrow.NewArrowWriterProperties(),
			ArrowIpcProps:      []ipc.Option{ipc.WithZstd()},
		},
	}
}

func (options *BulkIngestOptions) SetOption(eh *ErrorHelper, key, val string) (bool, error) {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		options.TableName = val
	case adbc.OptionValueIngestTargetCatalog:
		options.CatalogName = val
	case adbc.OptionValueIngestTargetDBSchema:
		options.SchemaName = val
	case adbc.OptionKeyIngestMode:
		switch val {
		case adbc.OptionValueIngestModeAppend:
			fallthrough
		case adbc.OptionValueIngestModeCreate:
			fallthrough
		case adbc.OptionValueIngestModeReplace:
			fallthrough
		case adbc.OptionValueIngestModeCreateAppend:
			options.Mode = val
		default:
			return true, eh.Errorf(adbc.StatusInvalidArgument, "invalid statement option %s=%s", key, val)
		}
	case adbc.OptionValueIngestTemporary:
		switch val {
		case adbc.OptionValueEnabled:
			options.Temporary = true
			options.SchemaName = ""
			options.CatalogName = ""
		case adbc.OptionValueDisabled:
			options.Temporary = false
		default:
			return true, eh.Errorf(adbc.StatusInvalidArgument, "invalid statement option %s=%s", key, val)
		}
	case OptionKeyIngestBatchSize:
		size, err := strconv.Atoi(val)
		if err != nil {
			return true, eh.Errorf(adbc.StatusInvalidArgument, "invalid ingest batch size: %v", err)
		}
		if size < 0 {
			return true, eh.Errorf(adbc.StatusInvalidArgument, "ingest batch size must be non-negative, got %d", size)
		}
		// These options are mutually exclusive - zero out the other
		options.MaxQuerySizeBytes = 0
		options.IngestBatchSize = size
	case OptionKeyIngestMaxQuerySizeBytes:
		size, err := strconv.Atoi(val)
		if err != nil {
			return true, eh.Errorf(adbc.StatusInvalidArgument, "invalid max query size: %v", err)
		}
		if size < 0 {
			return true, eh.Errorf(adbc.StatusInvalidArgument, "max query size must be non-negative, got %d", size)
		}
		// These options are mutually exclusive - zero out the other
		options.IngestBatchSize = 0
		options.MaxQuerySizeBytes = size
	default:
		return false, nil
	}
	return true, nil
}

// IsSet returns true if the user has set a table name to ingest into.
func (options *BulkIngestOptions) IsSet() bool {
	return options.TableName != ""
}

// Clear resets the destination options.
func (options *BulkIngestOptions) Clear() {
	options.TableName = ""
	options.SchemaName = ""
	options.CatalogName = ""
	options.Temporary = false
	options.Mode = adbc.OptionValueIngestModeCreate
}

// What a driver should do when creating the target table, if the table exists.
type BulkIngestTableExistsBehavior int

const (
	BulkIngestTableExistsError BulkIngestTableExistsBehavior = iota
	BulkIngestTableExistsIgnore
	BulkIngestTableExistsDrop
)

// What a driver should do when creating the target table, if the table does not exist.
type BulkIngestTableMissingBehavior int

const (
	BulkIngestTableMissingError BulkIngestTableMissingBehavior = iota
	BulkIngestTableMissingCreate
)

// BulkIngestSink is a buffer, ready for Parquet data to be written to it.  It
// can be an in-memory buffer, or it could be an open file handle.
type BulkIngestSink interface {
	io.Closer
	Sink() io.Writer
}

// BulkIngestPendingUpload is a set of serialized rows, ready to be uploaded or written to
// the staging area.
type BulkIngestPendingUpload struct {
	Data BulkIngestSink
	Rows int64
}

func (bpu *BulkIngestPendingUpload) String() string {
	return fmt.Sprintf("<in-memory buffer (%d rows)>", bpu.Rows)
}

func (bpu *BulkIngestPendingUpload) NumRows() int64 {
	return bpu.Rows
}

// BulkIngestPendingCopy is a file that was uploaded to the staging area and
// is ready to be copied into the target table.
type BulkIngestPendingCopy interface {
	fmt.Stringer
	NumRows() int64
}

var _ BulkIngestPendingCopy = (*BulkIngestPendingUpload)(nil)

// BulkIngestImpl is driver-specific behavior for bulk ingest, under the assumption that
// the target database accepts a stream of serialized data (which may be Arrow, Parquet,
// or something else).
type BulkIngestImpl interface {
	// CreateTable is the first step, and should create the table to ingest into.
	CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists BulkIngestTableExistsBehavior, ifTableMissing BulkIngestTableMissingBehavior) error
	// CreateSink is called repeatedly to allocate a sink to write serialized data
	// into, if needed. Return BufferBulkIngestSink to use an in-memory staging area.
	CreateSink(ctx context.Context, options *BulkIngestOptions) (BulkIngestSink, error)
	// Serialize writes Arrow data into the sink, pulling from the stream of
	// batches. Returns (rows, bytes) written.
	Serialize(ctx context.Context, writerProps *WriterProps, schema *arrow.Schema, batches chan arrow.RecordBatch, sink BulkIngestSink) (int64, int64, error)
	// Copy actually uploads the given serialized data into the target table.
	Copy(ctx context.Context, chunk BulkIngestPendingCopy) error
}

// BulkIngestInitFinalizeImpl is an optional interface to implement in addition to
// BulkIngestImpl for drivers to perform setup/teardown.
type BulkIngestInitFinalizeImpl interface {
	// Init is called prior to any other methods.
	Init(ctx context.Context) error
	// Finalize ends the bulk ingest. It is always called.
	Finalize(ctx context.Context, success bool) error
}

// BulkIngestFileImpl is an optional interface to implement in addition to BulkIngestImpl
// for drivers to add an additional upload-to-staging-area step.  For example, the target
// database may only be able to ingest data from Parquet files on S3; this interface gives
// the driver a chance to upload the serialized files, and then delete them.
type BulkIngestFileImpl interface {
	// Upload serialized data to the staging area, if needed.
	Upload(ctx context.Context, chunk BulkIngestPendingUpload) (BulkIngestPendingCopy, error)
	// Delete serialized data from the staging area, if needed.
	Delete(ctx context.Context, chunk BulkIngestPendingCopy) error
}

// Optionally implement this if the data has to be transformed (e.g. cast view types to
// 'normal' types).  The benefit of using this interface is that the transformation can be
// parallelized (vs just transforming the input stream).
type BulkIngestTransformImpl interface {
	TransformedSchema() *arrow.Schema
	TransformBatch(ctx context.Context, batch arrow.RecordBatch) (arrow.RecordBatch, error)
}

// ParquetIngestImpl is a base for BulkIngestImpl that writes Parquet data.
type ParquetIngestImpl struct{}

func (p *ParquetIngestImpl) Serialize(ctx context.Context, writerProps *WriterProps, schema *arrow.Schema, batches chan arrow.RecordBatch, sink BulkIngestSink) (int64, int64, error) {
	return writeParquetForIngestion(writerProps, schema, batches, sink.Sink())
}

// BulkIngestManager actually implements bulk ingest given an implementation of
// BulkIngestImpl and other interfaces.
type BulkIngestManager struct {
	Impl        BulkIngestImpl
	ErrorHelper *ErrorHelper
	Logger      *slog.Logger
	Alloc       memory.Allocator
	Ctx         context.Context
	Options     BulkIngestOptions
	Data        array.RecordReader

	// Internal state
	batches            chan arrow.RecordBatch
	transformedBatches chan arrow.RecordBatch
}

func (bi *BulkIngestManager) Close() {
	if bi.Data != nil {
		bi.Data.Release()
		bi.Data = nil
	}
	if bi.batches != nil {
		for record := range bi.batches {
			record.Release()
		}
		bi.batches = nil
	}
	if bi.transformedBatches != nil {
		for record := range bi.transformedBatches {
			record.Release()
		}
		bi.transformedBatches = nil
	}
}

// Init checks preconditions.  This must be called before ExecuteIngest.
func (bi *BulkIngestManager) Init() error {
	bi.Ctx = compute.WithAllocator(bi.Ctx, bi.Alloc)
	bi.Options.WriterProps.ArrowIpcProps = append(bi.Options.WriterProps.ArrowIpcProps, ipc.WithAllocator(bi.Alloc))
	if bi.Options.TableName == "" {
		return bi.ErrorHelper.InvalidState("must set %s to ingest data", adbc.OptionKeyIngestTargetTable)
	} else if bi.Data == nil {
		return bi.ErrorHelper.InvalidState("must bind data to ingest")
	} else if bi.Options.Mode == "" {
		bi.Options.Mode = adbc.OptionValueIngestModeCreate
	} else if bi.Options.Temporary && (bi.Options.CatalogName != "" || bi.Options.SchemaName != "") {
		return bi.ErrorHelper.InvalidState("cannot specify catalog/schema name and temporary table")
	} else if bi.Options.CatalogName != "" && bi.Options.SchemaName == "" {
		return bi.ErrorHelper.InvalidState("cannot specify catalog name without schema name")
	}

	return nil
}

// ExecuteIngest actually uploads data.
func (bi *BulkIngestManager) ExecuteIngest() (int64, error) {
	schema := bi.Data.Schema()

	initFinal, needsInit := bi.Impl.(BulkIngestInitFinalizeImpl)

	if needsInit {
		if err := initFinal.Init(bi.Ctx); err != nil {
			return -1, errors.Join(err, initFinal.Finalize(bi.Ctx, false))
		}
	}

	t, needsTransform := bi.Impl.(BulkIngestTransformImpl)
	if needsTransform {
		schema = t.TransformedSchema()
	}

	fileImpl, needsUpload := bi.Impl.(BulkIngestFileImpl)

	// Create the table if needed.
	var ifTableExists BulkIngestTableExistsBehavior
	var ifTableMissing BulkIngestTableMissingBehavior
	switch bi.Options.Mode {
	case adbc.OptionValueIngestModeCreate:
		ifTableExists = BulkIngestTableExistsError
		ifTableMissing = BulkIngestTableMissingCreate
	case adbc.OptionValueIngestModeAppend:
		ifTableExists = BulkIngestTableExistsIgnore
		ifTableMissing = BulkIngestTableMissingError
	case adbc.OptionValueIngestModeReplace:
		ifTableExists = BulkIngestTableExistsDrop
		ifTableMissing = BulkIngestTableMissingCreate
	case adbc.OptionValueIngestModeCreateAppend:
		ifTableExists = BulkIngestTableExistsIgnore
		ifTableMissing = BulkIngestTableMissingCreate
	}
	err := bi.Impl.CreateTable(bi.Ctx, schema, ifTableExists, ifTableMissing)
	if err != nil {
		if needsInit {
			return -1, errors.Join(err, initFinal.Finalize(bi.Ctx, false))
		}
		return -1, err
	}

	// From this point on: no early returns; at least spawn all goroutines.

	// Set up the ingest pipeline.
	g, cancelCtx := errgroup.WithContext(bi.Ctx)

	// Drain the bind parameters into a channel, chunking data.  (The final data size
	// will vary based on compression/encoding ratios.)
	bi.batches = make(chan arrow.RecordBatch, bi.Options.ReadDepth)
	g.Go(func() error {
		defer close(bi.batches)
		for bi.Data.Next() {
			select {
			case <-cancelCtx.Done():
				// We're not going to drain the reader, assuming that
				// Release will properly cancel if applicable
				return bi.Data.Err()
			default:
			}

			// TODO(lidavidm): rechunk data
			rec := bi.Data.RecordBatch()
			rec.Retain()
			bi.batches <- rec
		}

		err := bi.Data.Err()
		bi.Logger.Debug("drained source", "err", err)
		return err
	})

	if needsTransform {
		bi.transformedBatches = make(chan arrow.RecordBatch, bi.Options.ReadDepth+1)
		g.Go(func() error {
			defer close(bi.transformedBatches)
		loop:
			for {
				select {
				case <-cancelCtx.Done():
					return nil
				case batch := <-bi.batches:
					if batch == nil {
						break loop
					}
					transformed, err := t.TransformBatch(cancelCtx, batch)
					batch.Release()
					if err != nil {
						return err
					}
					bi.transformedBatches <- transformed
				}
			}

			bi.Logger.Debug("drained source", "err", err)
			return nil
		})
	}

	// Take the records from the channel and write serialized data to files/in-memory buffers.
	pendingBuffers := make(chan BulkIngestPendingUpload, bi.Options.MaxPendingBuffers)
	pendingFiles := make(chan BulkIngestPendingCopy, bi.Options.MaxPendingBuffers)
	g.Go(func() error {
		defer close(pendingBuffers)
		if !needsUpload {
			defer close(pendingFiles)
		}

		batches := bi.batches
		if needsTransform {
			batches = bi.transformedBatches
		}

		writers, innerCtx := errgroup.WithContext(cancelCtx)
		for range bi.Options.WriterParallelism {
			writers.Go(func() error {
				for {
					select {
					case <-innerCtx.Done():
						return nil
					default:
					}

					sink, err := bi.Impl.CreateSink(innerCtx, &bi.Options)
					if err != nil {
						return err
					}

					rows, bytes, err := bi.Impl.Serialize(innerCtx, &bi.Options.WriterProps, schema, batches, sink)
					// TODO(lidavidm): in these cases, don't we still need to delete the file?
					if err != nil {
						return errors.Join(err, sink.Close())
					} else if rows == 0 {
						_ = sink.Close()
						break
					}

					bi.Logger.Debug("created buffer", "table", bi.Options.TableName, "rows", rows, "bytes", bytes)
					if needsUpload {
						pendingBuffers <- BulkIngestPendingUpload{
							Data: sink,
							Rows: rows,
						}
					} else {
						pendingFiles <- &BulkIngestPendingUpload{
							Data: sink,
							Rows: rows,
						}
					}
				}
				return nil
			})
		}
		err := writers.Wait()
		bi.Logger.Debug("wrote all buffers", "err", err)
		return err
	})

	// Take the buffers and upload them (if necessary)
	if needsUpload {
		g.Go(func() error {
			defer close(pendingFiles)

			uploaders, innerCtx := errgroup.WithContext(cancelCtx)
			for range bi.Options.UploaderParallelism {
				uploaders.Go(func() (err error) {
					for pendingBuffer := range pendingBuffers {
						defer func() {
							err = errors.Join(err, pendingBuffer.Data.Close())
						}()

						select {
						case <-innerCtx.Done():
							bi.Logger.Debug("operation canceled", "table", bi.Options.TableName)
							return nil
						default:
						}

						uploaded, err := fileImpl.Upload(bi.Ctx, pendingBuffer)
						if err != nil {
							return err
						}

						pendingFiles <- uploaded
						bi.Logger.Debug("uploaded file", "table", bi.Options.TableName, "dest", uploaded.String(), "rows", pendingBuffer.Rows)
					}
					return nil
				})
			}
			err := uploaders.Wait()
			bi.Logger.Debug("uploaded all files", "err", err)
			return err
		})
	}

	// Take uploaded files and copy them into the remote system.
	var rowsWritten atomic.Int64
	recycleBin := make(chan BulkIngestPendingCopy, bi.Options.MaxPendingBuffers*2)
	g.Go(func() error {
		defer close(recycleBin)
		defer func() {
			if needsUpload {
				for pendingFile := range pendingFiles {
					recycleBin <- pendingFile
				}
			}
		}()
		for pendingFile := range pendingFiles {
			select {
			case <-cancelCtx.Done():
				if needsUpload {
					recycleBin <- pendingFile
				}
				return nil
			default:
			}

			err := bi.Impl.Copy(bi.Ctx, pendingFile)
			if err != nil {
				if needsUpload {
					recycleBin <- pendingFile
				}
				bi.Logger.Debug("failed to ingest file", "uri", pendingFile, "err", err)
				return err
			}

			rowsWritten.Add(pendingFile.NumRows())
			bi.Logger.Debug("ingested file", "table", bi.Options.TableName, "uri", pendingFile, "rows", pendingFile.NumRows())
			if needsUpload {
				recycleBin <- pendingFile
			}
		}
		bi.Logger.Debug("ingested all files")
		return nil
	})

	// Take uploaded files and delete them.
	if needsUpload {
		g.Go(func() error {
			var res error
			for pendingFile := range recycleBin {
				bi.Logger.Debug("cleaning up file", "table", bi.Options.TableName, "uri", pendingFile)

				err := fileImpl.Delete(bi.Ctx, pendingFile)
				if err != nil {
					// Only save the first error, but log all errors
					if res == nil {
						res = err
					}
					bi.Logger.Warn("failed to clean up file", "table", bi.Options.TableName, "uri", pendingFile, "err", err)
				} else {
					bi.Logger.Debug("cleaned up file", "table", bi.Options.TableName, "uri", pendingFile, "err", err)
				}
			}
			bi.Logger.Debug("deleted files", "err", res)
			return res
		})
	}

	err = g.Wait()

	if needsInit {
		// N.B. we always call finalize, even on error!
		if finalErr := initFinal.Finalize(bi.Ctx, err == nil); err != nil {
			bi.Logger.Error("failed to finalize bulk ingest", "err", finalErr)
			err = errors.Join(err, finalErr)
		}
	}

	bi.Logger.Debug("completed ingest", "err", err)
	return rowsWritten.Load(), err
}

// writeParquetForIngestion pulls records from the channel and appends them to a Parquet
// file until a certain number of rows is reached or the channel is closed.
func writeParquetForIngestion(writerProps *WriterProps, schema *arrow.Schema, batches chan arrow.RecordBatch, sink io.Writer) (int64, int64, error) {
	w, err := pqarrow.NewFileWriter(schema, sink, writerProps.ParquetWriterProps, writerProps.ArrowWriterProps)
	if err != nil {
		return 0, 0, err
	}

	rows := int64(0)
	for record := range batches {
		err = func(record arrow.RecordBatch) error {
			defer record.Release()
			if record.NumRows() == 0 {
				return nil
			}

			if err := w.Write(record); err != nil {
				return err
			}
			rows += int64(record.NumRows())
			return nil
		}(record)

		if err != nil {
			return 0, 0, err
		} else if w.RowGroupTotalBytesWritten() >= writerProps.MaxBytes {
			break
		}
	}

	if w.RowGroupTotalBytesWritten() == 0 {
		return 0, 0, nil
	}

	if err := w.Close(); err != nil {
		return 0, 0, err
	}

	return rows, w.RowGroupTotalCompressedBytes(), nil
}

// BufferBulkIngestSink is an in-memory BulkIngestSink.
type BufferBulkIngestSink struct {
	bytes.Buffer
}

func (sink *BufferBulkIngestSink) Sink() io.Writer {
	return &sink.Buffer
}

func (*BufferBulkIngestSink) Close() error {
	return nil
}
