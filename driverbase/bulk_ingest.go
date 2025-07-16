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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"golang.org/x/sync/errgroup"
)

// WriterProps holds properties for writing data files to be ingested.
type WriterProps struct {
	// A target file size in bytes.
	MaxBytes           int64
	ParquetWriterProps *parquet.WriterProperties
	ArrowWriterProps   pqarrow.ArrowWriterProperties
}

type BulkIngestOptions struct {
	// The table to ingest data into.
	TableName   string
	SchemaName  string
	CatalogName string
	// If true, use a temporary table.  The catalog/schema, if specified,
	// will be ignored (as temporary tables generally get implemented via
	// a special catalog/schema).
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
	WriterProps       WriterProps
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
		},
	}
}

type BulkIngestTableExistsBehavior int

const (
	BulkIngestTableExistsError BulkIngestTableExistsBehavior = iota
	BulkIngestTableExistsIgnore
	BulkIngestTableExistsDrop
)

type BulkIngestTableMissingBehavior int

const (
	BulkIngestTableMissingError BulkIngestTableMissingBehavior = iota
	BulkIngestTableMissingCreate
)

type BulkIngestSink interface {
	io.Closer
	Sink() io.Writer
}

// BulkIngestPendingUpload is a set of rows serialized to Parquet, ready to be uploaded to
// the remote system.
type BulkIngestPendingUpload struct {
	Data BulkIngestSink
	Rows int64
}

// BulkIngestPendingCopy is a file that was uploaded and is ready to be COPY-d into the
// remote system.
type BulkIngestPendingCopy interface {
	fmt.Stringer
	Rows() int64
}

type BulkIngestImpl interface {
	Copy(ctx context.Context, chunk BulkIngestPendingCopy) error
	CreateSink(ctx context.Context, options *BulkIngestOptions) (BulkIngestSink, error)
	CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists BulkIngestTableExistsBehavior, ifTableMissing BulkIngestTableMissingBehavior) error
	Delete(ctx context.Context, chunk BulkIngestPendingCopy) error
	Upload(ctx context.Context, chunk BulkIngestPendingUpload) (BulkIngestPendingCopy, error)
}

type BulkIngestManager struct {
	Impl       BulkIngestImpl
	DriverName string
	Logger     *slog.Logger
	Alloc      memory.Allocator
	Ctx        context.Context
	Options    BulkIngestOptions
	Data       array.RecordReader

	// Internal state
	records chan arrow.Record
}

func (bi *BulkIngestManager) Close() {
	if bi.Data != nil {
		bi.Data.Release()
		bi.Data = nil
	}
	if bi.records != nil {
		for record := range bi.records {
			record.Release()
		}
		bi.records = nil
	}
}

func (bi *BulkIngestManager) Init() error {
	if bi.Options.TableName == "" {
		return adbc.Error{
			Msg:  fmt.Sprintf("[%s] Must set %s to ingest data", bi.DriverName, adbc.OptionKeyIngestTargetTable),
			Code: adbc.StatusInvalidState,
		}
	} else if bi.Data == nil {
		return adbc.Error{
			Msg:  fmt.Sprintf("[%s] Must bind data to ingest", bi.DriverName),
			Code: adbc.StatusInvalidState,
		}
	} else if bi.Options.Mode == "" {
		bi.Options.Mode = adbc.OptionValueIngestModeCreate
	} else if bi.Options.Temporary && (bi.Options.CatalogName != "" || bi.Options.SchemaName != "") {
		return adbc.Error{
			Msg:  fmt.Sprintf("[%s] Cannot specify catalog/schema name and temporary table", bi.DriverName),
			Code: adbc.StatusInvalidState,
		}
	} else if bi.Options.CatalogName != "" && bi.Options.SchemaName == "" {
		return adbc.Error{
			Msg:  fmt.Sprintf("[%s] Cannot specify catalog name without schema name", bi.DriverName),
			Code: adbc.StatusInvalidState,
		}
	}

	return nil
}

func (bi *BulkIngestManager) ExecuteIngest() (int64, error) {
	schema := bi.Data.Schema()

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
		return -1, err
	}

	// Set up the ingest pipeline.
	g, cancelCtx := errgroup.WithContext(bi.Ctx)

	// Drain the bind parameters into a channel, chunking data appropriately.  (The
	// final data size after being serialized to Parquet will vary based on
	// compression/encoding ratios.)
	bi.records = make(chan arrow.Record, bi.Options.ReadDepth)
	g.Go(func() error {
		defer close(bi.records)
		for bi.Data.Next() {
			select {
			case <-cancelCtx.Done():
				// We're not going to drain the reader, assuming that
				// Release will properly cancel if applicable
				return bi.Data.Err()
			default:
			}

			// TODO(lidavidm): rechunk data
			rec := bi.Data.Record()
			rec.Retain()
			bi.records <- rec
		}

		err := bi.Data.Err()
		bi.Logger.Debug("drained source", "err", err)
		return err
	})

	// Take the records from the channel and write Parquet to files/in-memory buffers.
	pendingBuffers := make(chan BulkIngestPendingUpload, bi.Options.MaxPendingBuffers)
	g.Go(func() error {
		defer close(pendingBuffers)
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

					rows, bytes, err := writeParquetForIngestion(&bi.Options.WriterProps, schema, bi.records, sink.Sink())
					if err != nil {
						return errors.Join(err, sink.Close())
					} else if rows == 0 {
						_ = sink.Close()
						break
					}

					bi.Logger.Debug("created buffer", "table", bi.Options.TableName, "rows", rows, "bytes", bytes)
					pendingBuffers <- BulkIngestPendingUpload{
						Data: sink,
						Rows: rows,
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
	pendingFiles := make(chan BulkIngestPendingCopy, bi.Options.MaxPendingBuffers)
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

					uploaded, err := bi.Impl.Upload(bi.Ctx, pendingBuffer)
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

	// Take uploaded files and copy them into the remote system.
	var rowsWritten atomic.Int64
	recycleBin := make(chan BulkIngestPendingCopy, bi.Options.MaxPendingBuffers*2)
	g.Go(func() error {
		defer close(recycleBin)
		defer func() {
			for pendingFile := range pendingFiles {
				recycleBin <- pendingFile
			}
		}()
		for pendingFile := range pendingFiles {
			select {
			case <-cancelCtx.Done():
				recycleBin <- pendingFile
				return nil
			default:
			}

			err := bi.Impl.Copy(bi.Ctx, pendingFile)
			if err != nil {
				recycleBin <- pendingFile
				bi.Logger.Debug("failed to ingest file", "uri", pendingFile, "err", err)
				return err
			}

			rowsWritten.Add(pendingFile.Rows())
			bi.Logger.Debug("ingested file", "table", bi.Options.TableName, "uri", pendingFile, "rows", pendingFile.Rows())
			recycleBin <- pendingFile
		}
		bi.Logger.Debug("ingested all files")
		return nil
	})

	// Take uploaded files and delete them.
	g.Go(func() error {
		var res error
		for pendingFile := range recycleBin {
			bi.Logger.Debug("cleaning up file", "table", bi.Options.TableName, "uri", pendingFile)

			err := bi.Impl.Delete(bi.Ctx, pendingFile)
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

	err = g.Wait()
	bi.Logger.Debug("completed ingest", "err", err)
	return rowsWritten.Load(), err
}

// writeParquetForIngestion pulls records from the channel and appends them to a Parquet
// file until a certain number of rows is reached or the channel is closed.
func writeParquetForIngestion(writerProps *WriterProps, schema *arrow.Schema, records chan arrow.Record, sink io.Writer) (int64, int64, error) {
	w, err := pqarrow.NewFileWriter(schema, sink, writerProps.ParquetWriterProps, writerProps.ArrowWriterProps)
	if err != nil {
		return 0, 0, err
	}

	rows := int64(0)
	for record := range records {
		err = func(record arrow.Record) error {
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
