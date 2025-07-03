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
	"bytes"
	"context"
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/suite"
)

func TestWriteParquet(t *testing.T) {
	suite.Run(t, &WriteParquetTestSuite{})
}

type WriteParquetTestSuite struct {
	suite.Suite
	mem *memory.CheckedAllocator
}

func (s *WriteParquetTestSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *WriteParquetTestSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *WriteParquetTestSuite) readTable(buf *bytes.Buffer) arrow.Table {
	file, err := pqfile.NewParquetReader(bytes.NewReader(buf.Bytes()))
	s.NoError(err)
	defer testutil.CheckedClose(s.T(), file)
	reader, err := pqarrow.NewFileReader(file, pqarrow.ArrowReadProperties{}, s.mem)
	s.NoError(err)

	table, err := reader.ReadTable(context.TODO())
	s.NoError(err)
	defer table.Release()

	// strip metadata for easy comparison
	fields := make([]arrow.Field, len(table.Schema().Fields()))
	for i, field := range table.Schema().Fields() {
		fields[i] = arrow.Field{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// XXX: no way to strip metadata from the table schema, or to replace
	// a table schema with a new schema, or to get all columns as a slice,
	// or to compare tables (schemas) sans metadata
	columns := make([]arrow.Column, table.NumCols())
	for i := range table.NumCols() {
		columns[i] = *arrow.NewColumn(fields[i], table.Column(int(i)).Data())
	}
	// XXX: why does this want []Column when most APIs use *Column?
	newTable := array.NewTable(schema, columns, table.NumRows())
	// NewColumn and NewTable both retain, which is "one too many"
	for i := range newTable.NumCols() {
		newTable.Column(int(i)).Release()
	}
	return newTable
}

func (s *WriteParquetTestSuite) TestByteLimit() {
	props := &WriterProps{
		ParquetWriterProps: parquet.NewWriterProperties(),
		ArrowWriterProps:   pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(s.mem)),
		MaxBytes:           16,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "strs",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)
	records := make(chan arrow.Record, 5)
	record1 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"strs": "foobar"}, {"strs": "spam and eggs"}, {"strs": "eggs and spam"}]`)
	record1.Retain() // used below
	defer record1.Release()
	records <- record1
	record2 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"strs": "ruby"}, {"strs": "python"}, {"strs": "java"}]`)
	defer record2.Release()
	records <- record2
	close(records)

	sink := &bytes.Buffer{}
	rows, _, err := writeParquetForIngestion(props, schema, records, sink)
	s.NoError(err)
	s.Equal(int64(3), rows)

	table := s.readTable(sink)
	defer table.Release()

	expected := array.NewTableFromRecords(schema, []arrow.Record{record1})
	defer expected.Release()
	s.Truef(array.TableEqual(expected, table), "Expected: %s\nActual: %s", expected, table)

	leftover := <-records
	s.Truef(array.RecordEqual(record2, leftover), "Expected: %s\nActual: %s", record2, leftover)
}

func (s *WriteParquetTestSuite) TestByteLimitMultiBatch() {
	props := &WriterProps{
		ParquetWriterProps: parquet.NewWriterProperties(),
		ArrowWriterProps:   pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(s.mem)),
		MaxBytes:           128,
	}
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "strs",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)
	records := make(chan arrow.Record, 5)
	record1 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"strs": "foobar"}, {"strs": "spam and eggs"}]`)
	record1.Retain() // used below
	defer record1.Release()
	records <- record1

	record2 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"strs": "this is a test string"}, {"strs": "try to prevent compression"}, {"strs": "short string"}, {"strs": "abcde"}]`)
	record2.Retain() // used below
	defer record2.Release()
	records <- record2

	record3 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"strs": "ruby"}, {"strs": "python"}, {"strs": "java"}]`)
	defer record3.Release()
	records <- record3

	close(records)

	sink := &bytes.Buffer{}
	rows, _, err := writeParquetForIngestion(props, schema, records, sink)
	s.NoError(err)
	s.Equal(int64(6), rows)

	table := s.readTable(sink)
	defer table.Release()

	expected := array.NewTableFromRecords(schema, []arrow.Record{record1, record2})
	defer expected.Release()
	s.Truef(array.TableEqual(expected, table), "Expected: %s\nActual: %s", expected, table)

	leftover := <-records
	s.Truef(array.RecordEqual(record3, leftover), "Expected: %s\nActual: %s", record3, leftover)
}
