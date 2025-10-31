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
	"fmt"
	"io"
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestBaseRecordReader(t *testing.T) {
	suite.Run(t, &BaseRecordReaderSuite{})
}

type BaseRecordReaderSuite struct {
	suite.Suite
	ctx context.Context
	mem *memory.CheckedAllocator
}

func (s *BaseRecordReaderSuite) SetupTest() {
	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *BaseRecordReaderSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *BaseRecordReaderSuite) TestInitErrors() {
	rr := &BaseRecordReader{}
	defer rr.Release()
	// staticcheck tries to make the nil context here non-nil, but we're intentionally
	// testing a nil context.
	// nolint:staticcheck
	s.Error(rr.Init(nil, s.mem, nil, BaseRecordReaderOptions{}, nil))
	s.Error(rr.Init(s.ctx, nil, nil, BaseRecordReaderOptions{}, nil))
	s.Error(rr.Init(s.ctx, s.mem, nil, BaseRecordReaderOptions{}, nil))
}

type implNoInitialResultSet struct {
	beganAppending bool
}

func (s *implNoInitialResultSet) AppendRow(builder *array.RecordBuilder) (int64, error) {
	return 0, io.EOF
}
func (s *implNoInitialResultSet) BeginAppending(builder *array.RecordBuilder) error {
	s.beganAppending = true
	return nil
}
func (s *implNoInitialResultSet) Close() error {
	return nil
}
func (s *implNoInitialResultSet) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	return nil, fmt.Errorf("no result set")
}

// When there are no parameters and NextResultSet fails, we fail immediately in Init.
func (s *BaseRecordReaderSuite) TestInitNoInitialResultSet() {
	rr := &BaseRecordReader{}
	defer rr.Release()
	s.ErrorContains(rr.Init(s.ctx, s.mem, nil, BaseRecordReaderOptions{}, &implNoInitialResultSet{}), "no result set")
}

// When there are parameters but the parameters are empty, we get back an empty reader
// with an empty schema.
func (s *BaseRecordReaderSuite) TestInitNoParams() {
	impl := &implNoInitialResultSet{}
	rr := &BaseRecordReader{}
	defer rr.Release()
	schema := arrow.NewSchema([]arrow.Field{}, nil)
	params, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
	s.NoError(err)
	s.NoError(rr.Init(s.ctx, s.mem, params, BaseRecordReaderOptions{}, impl))
	s.False(rr.Next())
	s.True(schema.Equal(rr.Schema()))
	s.NoError(rr.Err())
	s.False(impl.beganAppending)
}

type implBeginAppending struct {
	t              *testing.T
	schema         *arrow.Schema
	beganAppending int
}

func (s *implBeginAppending) AppendRow(builder *array.RecordBuilder) (int64, error) {
	return 0, io.EOF
}
func (s *implBeginAppending) BeginAppending(builder *array.RecordBuilder) error {
	s.beganAppending++
	assert.NotNil(s.t, builder)
	assert.True(s.t, s.schema.Equal(builder.Schema()))
	return nil
}
func (s *implBeginAppending) Close() error {
	return nil
}
func (s *implBeginAppending) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	if s.beganAppending == 0 {
		return s.schema, nil
	}
	return nil, fmt.Errorf("no result set")
}

// BeginAppending should be called once with an initialized builder.  Also tests an empty
// result set.
func (s *BaseRecordReaderSuite) TestInitBeginAppending() {
	impl := &implBeginAppending{
		t: s.T(),
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}, nil),
	}
	rr := &BaseRecordReader{}
	defer rr.Release()
	s.NoError(rr.Init(s.ctx, s.mem, nil, BaseRecordReaderOptions{}, impl))
	s.False(rr.Next())
	s.True(impl.schema.Equal(rr.Schema()))
	s.NoError(rr.Err())
	s.Equal(1, impl.beganAppending)
}

func (s *BaseRecordReaderSuite) TestNextAfterClose() {
	rr := &BaseRecordReader{}
	s.False(rr.Next())
}

// There's no good way to validate this invariant.
// func (s *BaseRecordReaderSuite) TestNextClosesRecord() {
// }

type implNextCallsClose struct {
	appended int
}

func (s *implNextCallsClose) AppendRow(builder *array.RecordBuilder) (int64, error) {
	if s.appended < 2 {
		builder.Field(0).(*array.Int32Builder).Append(1)
		s.appended++
		return 1, nil
	}
	return 0, io.EOF
}
func (s *implNextCallsClose) BeginAppending(builder *array.RecordBuilder) error {
	return nil
}
func (s *implNextCallsClose) Close() error {
	return nil
}
func (s *implNextCallsClose) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	if s.appended == 0 {
		return arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}, nil), nil
	}
	return nil, fmt.Errorf("no result set")
}

func (s *BaseRecordReaderSuite) TestNextCallsClose() {
	{
		// Case 1: if we exactly fill up a batch, then Next() will return
		// true. The next call to Next() will encounter the EOF and set done, but
		// since no rows were appended, it will close the reader and return false.
		impl := &implNextCallsClose{}
		rr := &BaseRecordReader{}
		defer rr.Release()
		options := BaseRecordReaderOptions{BatchRowLimit: 2}
		s.NoError(rr.Init(s.ctx, s.mem, nil, options, impl))
		s.True(rr.Next())
		s.Equal(int64(2), rr.Record().NumRows())
		s.False(rr.done)
		s.NotNil(rr.impl)
		s.False(rr.Next())
		s.True(rr.done)
		s.Nil(rr.impl)
		s.NoError(rr.Err())
	}
	{
		// Case 2: if we append less than a batch, then Next() will return true
		// and set done.  The next call to Next() will encounter the done flag and
		// return false, closing the reader.
		impl := &implNextCallsClose{}
		rr := &BaseRecordReader{}
		defer rr.Release()
		options := BaseRecordReaderOptions{BatchRowLimit: 4}
		s.NoError(rr.Init(s.ctx, s.mem, nil, options, impl))
		s.True(rr.Next())
		s.Equal(int64(2), rr.Record().NumRows())
		s.True(rr.done)
		s.NotNil(rr.impl)
		s.False(rr.Next())
		s.True(rr.done)
		s.Nil(rr.impl)
		s.NoError(rr.Err())
	}
}

type implNextResultSetAcrossParams struct {
	resultSet int
	appended  int
}

func (s *implNextResultSetAcrossParams) AppendRow(builder *array.RecordBuilder) (int64, error) {
	if s.resultSet >= 2 && s.resultSet <= 4 {
		// These result sets are empty
		return 0, io.EOF
	}
	if s.appended < 2 {
		builder.Field(0).(*array.Int32Builder).Append(1)
		s.appended++
		return 1, nil
	}
	return 0, io.EOF
}
func (s *implNextResultSetAcrossParams) BeginAppending(builder *array.RecordBuilder) error {
	return nil
}
func (s *implNextResultSetAcrossParams) Close() error {
	return nil
}
func (s *implNextResultSetAcrossParams) NextResultSet(ctx context.Context, rec arrow.RecordBatch, rowIdx int) (*arrow.Schema, error) {
	s.resultSet++
	s.appended = 0
	return arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil), nil
}

// When there are parameters, Next() should call NextResultSet() as necessary to get
// enough rows (or until exhausting the reader), ignoring empty result sets
func (s *BaseRecordReaderSuite) TestNextResultSetAcrossParams() {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	rec1 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"ints": 1}, {"ints": 2}, {"ints": 3}, {"ints": 4}]`)
	rec2 := testutil.RecordFromJSON(s.T(), s.mem, schema, `[{"ints": 1}, {"ints": 2}, {"ints": 3}, {"ints": 4}]`)
	params, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec1, rec2})
	s.NoError(err)
	defer rec1.Release()
	defer rec2.Release()

	impl := &implNextResultSetAcrossParams{}
	rr := &BaseRecordReader{}
	defer rr.Release()
	options := BaseRecordReaderOptions{BatchRowLimit: 4}
	s.NoError(rr.Init(s.ctx, s.mem, params, options, impl))

	s.True(rr.Next())
	s.Equal(int64(4), rr.Record().NumRows())

	s.True(rr.Next())
	s.Equal(int64(4), rr.Record().NumRows())

	s.True(rr.Next())
	s.Equal(int64(2), rr.Record().NumRows())

	s.False(rr.Next())

	s.NoError(rr.Err())
}

type implNextByteLimit struct {
	implNextCallsClose
}

func (s *implNextByteLimit) AppendRow(builder *array.RecordBuilder) (int64, error) {
	if s.appended < 2 {
		builder.Field(0).(*array.Int32Builder).Append(1)
		s.appended++
		return 6, nil
	}
	if s.appended < 4 {
		builder.Field(0).(*array.Int32Builder).Append(1)
		s.appended++
		return 20, nil
	}
	return 0, io.EOF
}

func (s *BaseRecordReaderSuite) TestByteLimit() {
	impl := &implNextByteLimit{}
	rr := &BaseRecordReader{}
	defer rr.Release()
	options := BaseRecordReaderOptions{BatchByteLimit: 10}
	s.NoError(rr.Init(s.ctx, s.mem, nil, options, impl))

	s.True(rr.Next())
	s.Equal(int64(2), rr.Record().NumRows())
	s.False(rr.done)
	s.NotNil(rr.impl)

	s.True(rr.Next())
	s.Equal(int64(1), rr.Record().NumRows())
	s.False(rr.done)
	s.NotNil(rr.impl)

	s.True(rr.Next())
	s.Equal(int64(1), rr.Record().NumRows())
	s.False(rr.done)
	s.NotNil(rr.impl)

	s.False(rr.Next())
	s.True(rr.done)
	s.Nil(rr.impl)
	s.NoError(rr.Err())
}

type implOverflow struct {
	implNextCallsClose
}

func (s *implOverflow) AppendRow(builder *array.RecordBuilder) (int64, error) {
	s.appended++
	if s.appended == 2 {
		return 0, ErrOverflow
	}
	if s.appended < 5 {
		builder.Field(0).(*array.Int32Builder).Append(1)
		return 20, nil
	}
	return 0, io.EOF
}

func (s *BaseRecordReaderSuite) TestOverflow() {
	impl := &implOverflow{}
	rr := &BaseRecordReader{}
	defer rr.Release()
	options := BaseRecordReaderOptions{}
	s.NoError(rr.Init(s.ctx, s.mem, nil, options, impl))

	s.True(rr.Next())
	s.Equal(int64(1), rr.Record().NumRows())
	s.False(rr.done)
	s.NotNil(rr.impl)

	s.True(rr.Next())
	s.Equal(int64(2), rr.Record().NumRows())
	s.True(rr.done)
	s.NotNil(rr.impl)

	s.False(rr.Next())
	s.Nil(rr.impl)
	s.NoError(rr.Err())
}
