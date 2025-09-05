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

package testutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CheckedClose validates that a deferred Close call did not fail.
// See: https://github.com/stretchr/testify/issues/1067
func CheckedClose(t *testing.T, obj io.Closer) {
	if err := obj.Close(); err != nil {
		t.Errorf("Failed to close object of type %T: %s", obj, err)
	}
}

// RecordFromJSON is the same as array.RecordFromJSON, but fails the test on error.
func RecordFromJSON(t *testing.T, mem memory.Allocator, schema *arrow.Schema, json string) arrow.RecordBatch {
	record, _, err := array.RecordFromJSON(mem, schema, bytes.NewReader([]byte(json)))
	if err != nil {
		t.Fatalf("failed to create record from JSON: %v", err)
	}
	return record
}
