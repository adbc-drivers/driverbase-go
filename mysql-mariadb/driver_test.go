// Copyright (c) 2025 Columnar Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func TestDriver(t *testing.T) {
	dsn := "root:password@tcp(localhost:3306)/mysql"

	driver := NewDriver(memory.DefaultAllocator)

	db, err := driver.NewDatabase(map[string]string{
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

	// Make sure bind returns status not implemented
	// 2) Build a minimal, empty Arrow record for binding
	schema := arrow.NewSchema(nil, nil)
	rec := array.NewRecord(schema, []arrow.Array{}, 0)
	defer rec.Release()

	// 3) Call Bind and expect a NotImplemented error
	err = stmt.Bind(context.Background(), rec)
	require.Error(t, err, "Bind should not be implemented yet")

}
