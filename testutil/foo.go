package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.FixedWidthTypes.Duration_s},
	}, nil)
	json := `[{"a": -9223372036854775808}, {"a": 9223372036854775807}]`
	record, _, err := array.RecordFromJSON(mem, schema, bytes.NewReader([]byte(json)), array.WithUseNumber())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(record)
}
