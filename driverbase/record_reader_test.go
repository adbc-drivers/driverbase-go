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

package driverbase_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
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
	rr := &driverbase.BaseRecordReader{}
	defer rr.Release()
	// staticcheck tries to make the nil context here non-nil, but we're intentionally
	// testing a nil context.
	// nolint:staticcheck
	s.Error(rr.Init(nil, s.mem, nil, 0, nil))
	s.Error(rr.Init(s.ctx, nil, nil, 0, nil))
	s.Error(rr.Init(s.ctx, s.mem, nil, 0, nil))
}

type implNoInitialResultSet struct {
	beganAppending bool
}

func (s *implNoInitialResultSet) AppendRow(builder *array.RecordBuilder) error {
	return io.EOF
}
func (s *implNoInitialResultSet) BeginAppending(builder *array.RecordBuilder) error {
	s.beganAppending = true
	return nil
}
func (s *implNoInitialResultSet) Close() error {
	return nil
}
func (s *implNoInitialResultSet) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	return nil, fmt.Errorf("no result set")
}

// If we have no parameters and NextResultSet fails, we fail immediately in Init.
func (s *BaseRecordReaderSuite) TestInitNoInitialResultSet() {
	rr := &driverbase.BaseRecordReader{}
	defer rr.Release()
	s.ErrorContains(rr.Init(s.ctx, s.mem, nil, 0, &implNoInitialResultSet{}), "no result set")
}

// If we have parameters but the parameters are empty, we get back an empty reader with an
// empty schema.
func (s *BaseRecordReaderSuite) TestInitNoParams() {
	impl := &implNoInitialResultSet{}
	rr := &driverbase.BaseRecordReader{}
	defer rr.Release()
	schema := arrow.NewSchema([]arrow.Field{}, nil)
	params, err := array.NewRecordReader(schema, []arrow.Record{})
	s.NoError(err)
	s.NoError(rr.Init(s.ctx, s.mem, params, 0, impl))
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

func (s *implBeginAppending) AppendRow(builder *array.RecordBuilder) error {
	return io.EOF
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
func (s *implBeginAppending) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	if s.beganAppending == 0 {
		return s.schema, nil
	}
	return nil, fmt.Errorf("no result set")
}

// BeginAppending should be called once with an initialized builder.
func (s *BaseRecordReaderSuite) TestInitBeginAppending() {
	impl := &implBeginAppending{
		t: s.T(),
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}, nil),
	}
	rr := &driverbase.BaseRecordReader{}
	defer rr.Release()
	s.NoError(rr.Init(s.ctx, s.mem, nil, 0, impl))
	s.False(rr.Next())
	s.True(impl.schema.Equal(rr.Schema()))
	s.NoError(rr.Err())
	s.Equal(1, impl.beganAppending)
}

func (s *BaseRecordReaderSuite) TestCloseErrors() {
}

func (s *BaseRecordReaderSuite) TestCloseIdempotent() {
}

func (s *BaseRecordReaderSuite) TestCloseReleaseRetain() {
}

func (s *BaseRecordReaderSuite) TestEmptyResultSet() {
}

func (s *BaseRecordReaderSuite) TestNextAfterClose() {
}

func (s *BaseRecordReaderSuite) TestNextClosesRecord() {
}

func (s *BaseRecordReaderSuite) TestNextNoParams() {
}

func (s *BaseRecordReaderSuite) TestNextCallsClose() {
	// there are two cases here
}

func (s *BaseRecordReaderSuite) TestNextAppendRowError() {
}

func (s *BaseRecordReaderSuite) TestNextResultSet() {
}

func (s *BaseRecordReaderSuite) TestNextResultSetAcrossParams() {
	// pull rows as needed to get to the batch size
}

func (s *BaseRecordReaderSuite) TestNextResultSetEmpty() {
	// keep going even if the result set has 0 rows
}

func (s *BaseRecordReaderSuite) TestNextResultSetError() {
}
