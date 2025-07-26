package mysql

import (
	"context"
	"fmt"
	"strconv"
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

	require.Equal(t, int32(1), idCol.Value(0))  // First row id
	require.Equal(t, int32(2), idCol.Value(1))  // Second row id (AUTO_INCREMENT)
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
			{"cherry", "peach"},    // First batch: 2 rows
			{"mango", "pineapple"}, // Second batch: 2 rows
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

// TestSchemaMetadata tests that SQL type metadata is included in Arrow schema
func TestSchemaMetadata(t *testing.T) {
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

	// Create test table with various column types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_metadata (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(100) NOT NULL,
			price DECIMAL(10,2),
			created_at TIMESTAMP
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get metadata
	err = stmt.SetSqlQuery("SELECT id, name, price, created_at FROM adbc_test_metadata")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, len(schema.Fields()), "Should have 4 columns")

	// Check that each field has SQL metadata
	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())
		
		// Verify metadata exists
		require.NotNil(t, field.Metadata, "Field should have metadata")
		
		// Check for SQL database type name
		dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name")
		require.True(t, ok, "Should have sql.database_type_name metadata")
		require.NotEmpty(t, dbTypeName, "Database type name should not be empty")
		
		t.Logf("  SQL database type: %s", dbTypeName)
		
		// Check for column name
		colName, ok := field.Metadata.GetValue("sql.column_name")
		require.True(t, ok, "Should have sql.column_name metadata")
		require.Equal(t, field.Name, colName, "Column name in metadata should match field name")
		
		// Check type-specific metadata
		switch field.Name {
		case "name":
			// VARCHAR should have length
			if length, ok := field.Metadata.GetValue("sql.length"); ok {
				t.Logf("  VARCHAR length: %s", length)
			}
		case "price":
			// DECIMAL should have precision and scale
			if precision, ok := field.Metadata.GetValue("sql.precision"); ok {
				t.Logf("  DECIMAL precision: %s", precision)
			}
			if scale, ok := field.Metadata.GetValue("sql.scale"); ok {
				t.Logf("  DECIMAL scale: %s", scale)
			}
		}
	}
	
	t.Log("✅ Schema metadata test passed successfully")
}

// TestMySQLTypeConverter tests MySQL-specific type converter enhancements
func TestMySQLTypeConverter(t *testing.T) {
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

	// Create test table with MySQL-specific types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_mysql_types (
			id INT PRIMARY KEY AUTO_INCREMENT,
			data JSON,
			status ENUM('active', 'inactive') DEFAULT 'active'
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get MySQL-specific metadata
	err = stmt.SetSqlQuery("SELECT id, data, status FROM adbc_test_mysql_types")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(schema.Fields()), "Should have 3 columns")

	// Check MySQL-specific type enhancements
	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())
		
		// Verify metadata exists
		require.NotNil(t, field.Metadata, "Field should have metadata")
		
		// Check for SQL database type name
		dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name")
		require.True(t, ok, "Should have sql.database_type_name metadata")
		t.Logf("  SQL database type: %s", dbTypeName)
		
		// Check MySQL-specific enhancements
		switch field.Name {
		case "data":
			require.Equal(t, "JSON", dbTypeName, "Should be JSON type")
			
			// Check for MySQL JSON metadata
			isJSON, ok := field.Metadata.GetValue("mysql.is_json")
			require.True(t, ok, "Should have mysql.is_json metadata")
			require.Equal(t, "true", isJSON, "Should be marked as JSON")
			t.Logf("  MySQL JSON detected: %s", isJSON)
			
		case "status":
			require.Equal(t, "ENUM", dbTypeName, "Should be ENUM type")
			
			// Check for MySQL ENUM metadata
			isEnumSet, ok := field.Metadata.GetValue("mysql.is_enum_set")
			require.True(t, ok, "Should have mysql.is_enum_set metadata")
			require.Equal(t, "true", isEnumSet, "Should be marked as ENUM/SET")
			t.Logf("  MySQL ENUM detected: %s", isEnumSet)
		}
	}
	
	t.Log("✅ MySQL type converter test passed successfully")
}

// TestDecimalTypeHandling tests that DECIMAL types are properly converted to Arrow Decimal128
func TestDecimalTypeHandling(t *testing.T) {
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

	// Create test table with various DECIMAL types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_decimals (
			id INT PRIMARY KEY AUTO_INCREMENT,
			price DECIMAL(10,2),
			rate NUMERIC(5,4),
			percentage DECIMAL(3,1)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get decimal type information
	err = stmt.SetSqlQuery("SELECT id, price, rate, percentage FROM adbc_test_decimals")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 4, len(schema.Fields()), "Should have 4 columns")

	// Check that DECIMAL fields are properly converted
	expectedDecimals := map[string]struct {
		precision int32
		scale     int32
	}{
		"price":      {precision: 10, scale: 2},
		"rate":       {precision: 5, scale: 4},
		"percentage": {precision: 3, scale: 1},
	}

	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())
		
		if expected, isDecimal := expectedDecimals[field.Name]; isDecimal {
			// Verify it's a decimal type
			decimalType, ok := field.Type.(*arrow.Decimal128Type)
			require.True(t, ok, "Field %s should be Decimal128Type, got %T", field.Name, field.Type)
			
			// Verify precision and scale
			require.Equal(t, expected.precision, decimalType.Precision, "Precision mismatch for %s", field.Name)
			require.Equal(t, expected.scale, decimalType.Scale, "Scale mismatch for %s", field.Name)
			
			t.Logf("  Decimal(%d,%d) ✓", decimalType.Precision, decimalType.Scale)
			
			// Verify metadata includes precision and scale
			precision, ok := field.Metadata.GetValue("sql.precision")
			require.True(t, ok, "Should have sql.precision metadata")
			require.Equal(t, fmt.Sprintf("%d", expected.precision), precision)
			
			scale, ok := field.Metadata.GetValue("sql.scale")
			require.True(t, ok, "Should have sql.scale metadata")
			require.Equal(t, fmt.Sprintf("%d", expected.scale), scale)
		}
	}
	
	t.Log("✅ Decimal type handling test passed successfully")
}

// TestTimestampPrecisionHandling tests that TIMESTAMP/DATETIME types use correct Arrow timestamp units
func TestTimestampPrecisionHandling(t *testing.T) {
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

	// Create test table with various timestamp precisions
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_timestamps (
			id INT PRIMARY KEY AUTO_INCREMENT,
			ts_default TIMESTAMP,
			ts_seconds TIMESTAMP(0),
			ts_millis TIMESTAMP(3),
			ts_micros TIMESTAMP(6),
			dt_default DATETIME,
			dt_millis DATETIME(3)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Test ExecuteSchema to get timestamp type information
	err = stmt.SetSqlQuery("SELECT id, ts_default, ts_seconds, ts_millis, ts_micros, dt_default, dt_millis FROM adbc_test_timestamps")
	require.NoError(t, err)

	schemaStmt, ok := stmt.(adbc.StatementExecuteSchema)
	require.True(t, ok, "Statement should implement StatementExecuteSchema")

	schema, err := schemaStmt.ExecuteSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, 7, len(schema.Fields()), "Should have 7 columns")

	// Expected timestamp units based on precision (MySQL reports actual precision)
	expectedTimestamps := map[string]string{
		"ts_default": "timestamp[s, tz=UTC]",  // MySQL reports precision=0 for default TIMESTAMP
		"ts_seconds": "timestamp[s, tz=UTC]",  // 0 fractional seconds = seconds
		"ts_millis":  "timestamp[ms, tz=UTC]", // 3 fractional seconds = milliseconds
		"ts_micros":  "timestamp[us, tz=UTC]", // 6 fractional seconds = microseconds
		"dt_default": "timestamp[s, tz=UTC]",  // MySQL reports precision=0 for default DATETIME
		"dt_millis":  "timestamp[ms, tz=UTC]", // 3 fractional seconds = milliseconds
	}

	for i, field := range schema.Fields() {
		t.Logf("Field %d: %s (type: %s)", i, field.Name, field.Type.String())
		
		if expectedType, isTimestamp := expectedTimestamps[field.Name]; isTimestamp {
			// Verify the timestamp type matches expected precision
			require.Equal(t, expectedType, field.Type.String(), "Timestamp unit mismatch for %s", field.Name)
			
			// Verify metadata includes fractional seconds precision (if available)
			if precision, ok := field.Metadata.GetValue("sql.fractional_seconds_precision"); ok {
				t.Logf("  Fractional seconds precision: %s", precision)
			}
			
			t.Logf("  Expected: %s ✓", expectedType)
		}
	}
	
	t.Log("✅ Timestamp precision handling test passed successfully")
}

// TestQueryBatchSizeConfiguration tests that the batch size configuration affects query streaming
func TestQueryBatchSizeConfiguration(t *testing.T) {
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

	// Create test table with many rows
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_batch_size (
			id INT PRIMARY KEY AUTO_INCREMENT,
			value VARCHAR(50)
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert 100 rows for testing
	err = stmt.SetSqlQuery("INSERT INTO adbc_test_batch_size (value) VALUES (?)")
	require.NoError(t, err)
	err = stmt.Prepare(context.Background())
	require.NoError(t, err)

	// Create Arrow record with 100 values
	allocator := memory.DefaultAllocator
	stringBuilder := array.NewStringBuilder(allocator)
	defer stringBuilder.Release()

	for i := 0; i < 100; i++ {
		stringBuilder.Append(fmt.Sprintf("test_value_%d", i))
	}
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()

	bindSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	bindRecord := array.NewRecord(bindSchema, []arrow.Array{stringArray}, 100)
	defer bindRecord.Release()

	err = stmt.Bind(context.Background(), bindRecord)
	require.NoError(t, err)

	insertedCount, err := stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(100), insertedCount)

	// Test with different batch sizes
	testCases := []struct {
		batchSize    string
		expectedName string
	}{
		{"5", "small batch size"},
		{"25", "medium batch size"},
		{"50", "large batch size"},
		{"200", "batch size larger than total rows"},
	}

	for _, tc := range testCases {
		t.Run(tc.expectedName, func(t *testing.T) {
			// Create new statement for each test
			testStmt, err := cn.NewStatement()
			require.NoError(t, err)
			defer testStmt.Close()

			// Set the batch size
			err = testStmt.SetOption("adbc.statement.batch_size", tc.batchSize)
			require.NoError(t, err)

			// Query all rows
			err = testStmt.SetSqlQuery("SELECT id, value FROM adbc_test_batch_size ORDER BY id")
			require.NoError(t, err)

			reader, rowCount, err := testStmt.ExecuteQuery(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(-1), rowCount) // Row count unknown until read
			defer reader.Release()

			totalRows := int64(0)
			batchCount := 0

			// Read all batches and count them
			for reader.Next() {
				record := reader.Record()
				require.NotNil(t, record)
				
				batchRows := record.NumRows()
				totalRows += batchRows
				batchCount++

				t.Logf("Batch %d: %d rows (batch size setting: %s)", batchCount, batchRows, tc.batchSize)

				// Verify batch doesn't exceed expected size (except for last batch)
				expectedBatchSize, _ := strconv.Atoi(tc.batchSize)
				require.LessOrEqual(t, int(batchRows), expectedBatchSize, "Batch size should not exceed configured limit")

				record.Release()
			}

			require.NoError(t, reader.Err())
			require.Equal(t, int64(100), totalRows, "Should read all 100 rows")

			// Verify that smaller batch sizes result in more batches
			expectedBatchSize, _ := strconv.Atoi(tc.batchSize)
			if expectedBatchSize < 100 {
				require.Greater(t, batchCount, 1, "Small batch sizes should result in multiple batches")
				expectedBatches := (100 + expectedBatchSize - 1) / expectedBatchSize // Ceiling division
				require.Equal(t, expectedBatches, batchCount, "Number of batches should match expected based on batch size")
			} else {
				require.Equal(t, 1, batchCount, "Large batch sizes should result in single batch")
			}

			t.Logf("✅ Batch size %s: %d batches, %d total rows", tc.batchSize, batchCount, totalRows)
		})
	}

	t.Log("✅ Query batch size configuration test passed successfully")
}

// TestTypedBuilderHandling tests that the sqlRecordReader properly handles different data types with typed builders
func TestTypedBuilderHandling(t *testing.T) {
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

	// Create test table with various data types
	err = stmt.SetSqlQuery(`
		CREATE TEMPORARY TABLE adbc_test_typed_builders (
			id INT PRIMARY KEY AUTO_INCREMENT,
			tiny_int TINYINT,
			small_int SMALLINT, 
			medium_int MEDIUMINT,
			big_int BIGINT,
			float_val FLOAT,
			double_val DOUBLE,
			varchar_val VARCHAR(100),
			text_val TEXT,
			bool_val BOOLEAN,
			decimal_val DECIMAL(10,2),
			timestamp_val TIMESTAMP,
			binary_val VARBINARY(50),
			json_val JSON,
			enum_val ENUM('red', 'green', 'blue')
		)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Insert test data with various types
	err = stmt.SetSqlQuery(`
		INSERT INTO adbc_test_typed_builders (
			tiny_int, small_int, medium_int, big_int, 
			float_val, double_val, varchar_val, text_val, bool_val,
			decimal_val, timestamp_val, binary_val, json_val, enum_val
		) VALUES 
		(127, 32767, 8388607, 9223372036854775807, 
		 3.14159, 2.718281828, 'test string', 'longer text content', true,
		 123.45, '2023-12-25 10:30:00', X'48656C6C6F', '{"key": "value"}', 'red'),
		(-128, -32768, -8388608, -9223372036854775808,
		 -1.5, -999.999, 'another string', 'more text', false,
		 -67.89, '2024-01-01 00:00:00', X'576F726C64', '{"number": 42}', 'green'),
		(NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL)
	`)
	require.NoError(t, err)
	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)

	// Query the data to test typed builders
	err = stmt.SetSqlQuery(`
		SELECT id, tiny_int, small_int, medium_int, big_int,
		       float_val, double_val, varchar_val, text_val, bool_val,
		       decimal_val, timestamp_val, binary_val, json_val, enum_val
		FROM adbc_test_typed_builders 
		ORDER BY id
	`)
	require.NoError(t, err)

	// Set a small batch size to ensure we test the builder logic properly
	err = stmt.SetOption("adbc.statement.batch_size", "2")
	require.NoError(t, err)

	reader, rowCount, err := stmt.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(-1), rowCount)
	defer reader.Release()

	schema := reader.Schema()
	require.Equal(t, 15, len(schema.Fields()), "Should have 15 columns")

	// Verify schema field types
	expectedTypes := map[string]string{
		"id":            "int32",
		"tiny_int":      "int8", 
		"small_int":     "int16",
		"medium_int":    "int32",
		"big_int":       "int64",
		"float_val":     "float32",
		"double_val":    "float64", 
		"varchar_val":   "utf8",
		"text_val":      "utf8",
		"bool_val":      "int8", // MySQL BOOLEAN is actually TINYINT
		"decimal_val":   "decimal(10, 2)",
		"timestamp_val": "timestamp[s, tz=UTC]", // Default precision is 0 = seconds
		"binary_val":    "binary",
		"json_val":      "utf8", // JSON handled by MySQL type converter
		"enum_val":      "utf8", // ENUM handled by MySQL type converter
	}

	for _, field := range schema.Fields() {
		expectedType, exists := expectedTypes[field.Name]
		require.True(t, exists, "Unexpected field: %s", field.Name)
		require.Equal(t, expectedType, field.Type.String(), "Type mismatch for field %s", field.Name)
		t.Logf("Field %s: %s ✓", field.Name, field.Type.String())
	}

	totalRows := int64(0)
	batchCount := 0

	// Read all batches and verify data
	for reader.Next() {
		record := reader.Record()
		require.NotNil(t, record)
		
		batchRows := record.NumRows()
		totalRows += batchRows
		batchCount++
		
		t.Logf("Batch %d: %d rows", batchCount, batchRows)

		// Verify data types and values for each column
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			field := schema.Field(colIdx)
			column := record.Column(colIdx)
			
			t.Logf("  Column %s (%s): %d values", field.Name, field.Type.String(), column.Len())
			
			// Test a few specific values to ensure typed builders worked correctly
			if batchCount == 1 && batchRows >= 2 { // First batch with at least 2 rows
				switch field.Name {
				case "id":
					idCol := column.(*array.Int32)
					require.Equal(t, int32(1), idCol.Value(0), "First ID should be 1")
					require.Equal(t, int32(2), idCol.Value(1), "Second ID should be 2")
					
				case "tiny_int":
					tinyCol := column.(*array.Int8)
					require.Equal(t, int8(127), tinyCol.Value(0), "First tiny_int should be 127")
					require.Equal(t, int8(-128), tinyCol.Value(1), "Second tiny_int should be -128")
					
				case "varchar_val":
					strCol := column.(*array.String)
					require.Equal(t, "test string", strCol.Value(0), "First varchar should match")
					require.Equal(t, "another string", strCol.Value(1), "Second varchar should match")
					
				case "bool_val":
					// MySQL BOOLEAN is actually TINYINT (int8)
					boolCol := column.(*array.Int8)
					require.Equal(t, int8(1), boolCol.Value(0), "First bool should be 1 (true)")
					require.Equal(t, int8(0), boolCol.Value(1), "Second bool should be 0 (false)")
					
				case "decimal_val":
					// Decimal fields should be properly typed
					_, ok := column.(*array.Decimal128)
					require.True(t, ok, "Decimal column should be Decimal128 type")
				}
			}
			
			// Test NULL handling for third row (if present)
			if batchCount == 2 && batchRows >= 1 { // Second batch with NULL row
				nullRowIdx := 0 // First row in second batch is the NULL row
				if nullRowIdx < int(batchRows) {
					switch field.Name {
					case "id":
						// ID is auto-increment, should not be NULL
						idCol := column.(*array.Int32)
						require.False(t, idCol.IsNull(nullRowIdx), "ID should not be NULL")
					case "tiny_int", "varchar_val", "bool_val":
						// These should be NULL in the third row
						require.True(t, column.IsNull(nullRowIdx), "Column %s should be NULL in row %d", field.Name, nullRowIdx)
					}
				}
			}
		}

		record.Release()
	}

	require.NoError(t, reader.Err())
	require.Equal(t, int64(3), totalRows, "Should read all 3 rows")
	require.Equal(t, 2, batchCount, "Should have 2 batches with batch size 2")

	t.Log("✅ Typed builder handling test passed successfully")
}
