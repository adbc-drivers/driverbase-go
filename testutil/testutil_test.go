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

package testutil_test

import (
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrayFromJSON(t *testing.T) {
	mem := memory.DefaultAllocator
	dt := arrow.FixedWidthTypes.Duration_s
	arr := testutil.ArrayFromJSON(t, mem, dt, `[-9223372036854775808, 9223372036854775807]`).(*array.Duration)
	assert.Equal(t, arrow.Duration(-9223372036854775808), arr.Value(0))
	assert.Equal(t, arrow.Duration(9223372036854775807), arr.Value(1))
}

func TestRecordFromJSON(t *testing.T) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Duration_s},
	}, nil)
	json := `[{"a": -9223372036854775808}, {"a": 9223372036854775807}]`
	record := testutil.RecordFromJSON(t, mem, schema, json)
	arr := record.Column(0).(*array.Duration)
	t.Logf("%v", arr)
	assert.Equal(t, arrow.Duration(-9223372036854775808), arr.Value(0))
	assert.Equal(t, arrow.Duration(9223372036854775807), arr.Value(1))
}
