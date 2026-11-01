// Copyright (c) 2026 ADBC Drivers Contributors
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

package sqlwrapper

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteSchemaAdditionalExecParams(t *testing.T) {
	ctx := context.Background()
	d := &fakeDriver{}
	stmt := newFakeStatement(t, d)

	require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
	schema, err := stmt.(adbc.StatementExecuteSchema).ExecuteSchema(ctx)
	require.NoError(t, err)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(0).Type)

	calls := d.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "SELECT * FROM (SELECT 1) AS subquery LIMIT 0", calls[0].query)
	require.Len(t, calls[0].args, 1)
	assert.Equal(t, "param", calls[0].args[0].Name)
	assert.Equal(t, "value", calls[0].args[0].Value)
}
