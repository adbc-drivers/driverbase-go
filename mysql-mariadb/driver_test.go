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
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDriver(t *testing.T) {
	// TODO: Change to a non-localhost DSN for CI
	dsn := "root:NewPassword1234@tcp(127.0.0.1:3306)/mydatabase"

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

	_, err = stmt.ExecuteUpdate(context.Background())
	require.NoError(t, err)
}
