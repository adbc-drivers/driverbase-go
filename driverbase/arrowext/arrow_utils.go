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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// EmptyReader is a no-op RecordReader.  It is easier to use than
// [array.NewRecordReader] which may return an error.
type EmptyReader struct{}

func (EmptyReader) Retain()                        {}
func (EmptyReader) Release()                       {}
func (EmptyReader) Schema() *arrow.Schema          { return arrow.NewSchema([]arrow.Field{}, nil) }
func (EmptyReader) Next() bool                     { return false }
func (EmptyReader) Record() arrow.RecordBatch      { return nil }
func (EmptyReader) RecordBatch() arrow.RecordBatch { return nil }
func (EmptyReader) Err() error                     { return nil }

var _ array.RecordReader = &EmptyReader{}
