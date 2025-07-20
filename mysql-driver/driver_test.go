package mysql

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// testRecordReader is a simple RecordReader for testing BindStream
type testRecordReader struct {
	schema        *arrow.Schema
	batches       [][]string
	currentBatch  int
	currentRecord arrow.Record
	allocator     memory.Allocator
	done          bool
}

func (t *testRecordReader) Schema() *arrow.Schema {
	return t.schema
}

func (t *testRecordReader) Next() bool {
	if t.done || t.currentBatch >= len(t.batches) {
		return false
	}

	// Release previous record
	if t.currentRecord != nil {
		t.currentRecord.Release()
		t.currentRecord = nil
	}

	// Create record for current batch
	batch := t.batches[t.currentBatch]
	
	// Build string array
	builder := array.NewStringBuilder(t.allocator)
	defer builder.Release()
	
	for _, value := range batch {
		builder.Append(value)
	}
	stringArray := builder.NewArray()
	defer stringArray.Release()
	
	// Create record
	t.currentRecord = array.NewRecord(t.schema, []arrow.Array{stringArray}, int64(len(batch)))
	
	t.currentBatch++
	
	// Check if we're done
	if t.currentBatch >= len(t.batches) {
		t.done = true
	}
	
	return true
}

func (t *testRecordReader) Record() arrow.Record {
	return t.currentRecord
}

func (t *testRecordReader) Err() error {
	return nil
}

func (t *testRecordReader) Release() {
	if t.currentRecord != nil {
		t.currentRecord.Release()
		t.currentRecord = nil
	}
}

func (t *testRecordReader) Retain() {
	// No-op for this test implementation
}

func TestDriver(t *testing.T) {
	dsn := "root:password@tcp(localhost:3306)/mysql"

	mysqlDriver := NewDriver()

	db, err := mysqlDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: dsn,
	})
	require.NoError(t, err)
	defer db.Close()

	cn, err := db.Open(context.Background())
	require.NoError(t, err)
	defer cn.Close()

	stmt, err := cn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// create table
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_driver (
			id INT PRIMARY KEY AUTO_INCREMENT,
			val VARCHAR(20)
		)
	`)
	require.NoError(t, err)

	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// insert dummy data
	err = stmt.SetSqlQuery(`
	INSERT INTO adbc_test_driver (val) VALUES ('apple'), ('banana')
`)
	require.NoError(t, err)

	count, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	// Test that Bind works with empty record
	emptySchema := arrow.NewSchema(nil, nil)
	emptyRec := array.NewRecord(emptySchema, []arrow.Array{}, 0)
	defer emptyRec.Release()

	// Bind should work with empty record (no-op)
	err = stmt.Bind(context.Background(), emptyRec)
	require.NoError(t, err)

	// Test GetParameterSchema with no parameters
	err = stmt.SetSqlQuery("SELECT COUNT(*) FROM adbc_test_driver")
	require.NoError(t, err)

	paramSchema, err := stmt.GetParameterSchema()
	require.NoError(t, err)
	require.Equal(t, 0, len(paramSchema.Fields()), "Query without parameters should have empty schema")

	// Test GetParameterSchema with parameters
	err = stmt.SetSqlQuery("SELECT * FROM adbc_test_driver WHERE id = ? AND val = ?")
	require.NoError(t, err)

	paramSchema, err = stmt.GetParameterSchema()
	require.NoError(t, err)
	require.Equal(t, 2, len(paramSchema.Fields()), "Query with 2 parameters should have 2 fields")
	require.Equal(t, "param_0", paramSchema.Field(0).Name)
	require.Equal(t, "param_1", paramSchema.Field(1).Name)

	// Test ExecuteSchema
	err = stmt.SetSqlQuery("SELECT id, val FROM adbc_test_driver")
	require.NoError(t, err)

	// Cast to StatementExecuteSchema interface to access ExecuteSchema
	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")
	
	resultSchema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, len(resultSchema.Fields()), "Should have 2 columns")
	require.Equal(t, "id", resultSchema.Field(0).Name)
	require.Equal(t, "val", resultSchema.Field(1).Name)

	// Verify types are mapped correctly
	require.Equal(t, "int32", resultSchema.Field(0).Type.String()) // INT -> int32
	require.Equal(t, "utf8", resultSchema.Field(1).Type.String())  // VARCHAR -> utf8

	// Test ExecuteQuery
	err = stmt.SetSqlQuery("SELECT id, val FROM adbc_test_driver ORDER BY id")
	require.NoError(t, err)
	
	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount) // Row count unknown until read
	defer reader.Release()
	
	// Verify we can read the data
	hasNext := reader.Next()
	require.True(t, hasNext, "Should have at least one batch")
	
	record := reader.Record()
	require.NotNil(t, record)
	require.Equal(t, int64(2), record.NumRows()) // Should have 2 rows
	require.Equal(t, int64(2), record.NumCols()) // Should have 2 columns
	
	// Verify data values
	idCol := record.Column(0).(*array.Int32)
	valCol := record.Column(1).(*array.String)
	
	require.Equal(t, int32(1), idCol.Value(0)) // First row id
	require.Equal(t, int32(2), idCol.Value(1)) // Second row id (AUTO_INCREMENT)
	require.Equal(t, "apple", valCol.Value(0))  // First row val
	require.Equal(t, "banana", valCol.Value(1)) // Second row val
	
	record.Release()
	
	// Should be no more batches for this small result
	hasNext = reader.Next()
	require.False(t, hasNext, "Should not have more batches")

	// Test Bind parameters for bulk insert
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_driver (val) VALUES (?)")
	require.NoError(t, err)
	
	err = stmt.Prepare(context.Background())
	require.NoError(t, err)
	
	// Create Arrow record with bulk data
	allocator := memory.DefaultAllocator
	
	// Build string array with fruit names
	stringBuilder := array.NewStringBuilder(allocator)
	defer stringBuilder.Release()
	
	fruits := []string{"orange", "grape", "kiwi"}
	for _, fruit := range fruits {
		stringBuilder.Append(fruit)
	}
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()
	
	// Create schema and record for bind parameters
	bindSchema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	
	bindRecord := array.NewRecord(bindSchema, []arrow.Array{stringArray}, 3)
	defer bindRecord.Release()
	
	// Bind the record
	err = stmt.Bind(context.Background(), bindRecord)
	require.NoError(t, err)
	
	// Execute the bulk insert
	insertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(3), insertedCount) // Should insert 3 rows
	
	// Verify the data was inserted
	err = stmt.SetSqlQuery("SELECT COUNT(*) FROM adbc_test_driver")
	require.NoError(t, err)
	
	reader2, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer reader2.Release()
	
	hasNext = reader2.Next()
	require.True(t, hasNext)
	
	countRecord := reader2.Record()
	require.NotNil(t, countRecord)
	
	countCol := countRecord.Column(0).(*array.Int64)
	totalRows := countCol.Value(0)
	require.Equal(t, int64(5), totalRows) // 2 original + 3 new = 5 total
	
	countRecord.Release()

	// Test BindStream for streaming bulk insert
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_driver (val) VALUES (?)")
	require.NoError(t, err)
	
	// Create a simple RecordReader that produces multiple batches
	streamReader := &testRecordReader{
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "val", Type: arrow.BinaryTypes.String, Nullable: false},
		}, nil),
		batches: [][]string{
			{"cherry", "peach"},     // First batch: 2 rows
			{"mango", "pineapple"},  // Second batch: 2 rows
		},
		currentBatch: 0,
		allocator:    allocator,
	}
	
	// Use BindStream
	err = stmt.BindStream(context.Background(), streamReader)
	require.NoError(t, err)
	
	// Execute the streaming bulk insert
	streamInsertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(4), streamInsertedCount) // Should insert 4 rows (2 batches × 2 rows)
	
	// Verify final count
	err = stmt.SetSqlQuery("SELECT COUNT(*) FROM adbc_test_driver")
	require.NoError(t, err)
	
	reader3, _, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	defer reader3.Release()
	
	hasNext = reader3.Next()
	require.True(t, hasNext)
	
	finalCountRecord := reader3.Record()
	require.NotNil(t, finalCountRecord)
	
	finalCountCol := finalCountRecord.Column(0).(*array.Int64)
	finalTotalRows := finalCountCol.Value(0)
	require.Equal(t, int64(9), finalTotalRows) // 5 previous + 4 new = 9 total
	
	finalCountRecord.Release()

	t.Log("✅ TestDriver passed successfully")

}
