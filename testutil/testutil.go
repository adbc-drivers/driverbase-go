// Copyright (c) 2025 Columnar Technologies.  All rights reserved.

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
func RecordFromJSON(t *testing.T, mem memory.Allocator, schema *arrow.Schema, json string) arrow.Record {
	record, _, err := array.RecordFromJSON(mem, schema, bytes.NewReader([]byte(json)))
	if err != nil {
		t.Fatalf("failed to create record from JSON: %v", err)
	}
	return record
}
