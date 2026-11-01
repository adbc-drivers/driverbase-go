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
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// fakeCall is a query or statement received by fakeDriver.
type fakeCall struct {
	ctx   context.Context
	query string
	args  []driver.NamedValue
}

// fakeDriver is a database/sql driver that records the queries it receives
// and answers every query with a single BIGINT column and one row.
type fakeDriver struct {
	mu    sync.Mutex
	calls []fakeCall
}

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{d: d}, nil }

func (d *fakeDriver) Connect(context.Context) (driver.Conn, error) { return &fakeConn{d: d}, nil }

func (d *fakeDriver) Driver() driver.Driver { return d }

func (d *fakeDriver) CreateDB(context.Context, string, map[string]string, *slog.Logger) (*sql.DB, error) {
	return sql.OpenDB(d), nil
}

func (d *fakeDriver) record(ctx context.Context, query string, args []driver.NamedValue) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, fakeCall{ctx: ctx, query: query, args: args})
}

func (d *fakeDriver) Calls() []fakeCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]fakeCall(nil), d.calls...)
}

type fakeConn struct {
	d *fakeDriver
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not supported")
}

// CheckNamedValue accepts arguments of any type, as drivers that take
// driver-specific parameters do.
func (c *fakeConn) CheckNamedValue(*driver.NamedValue) error { return nil }

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.d.record(ctx, query, args)
	return &fakeRows{}, nil
}

func (c *fakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.d.record(ctx, query, args)
	return driver.RowsAffected(0), nil
}

type fakeRows struct {
	done bool
}

func (r *fakeRows) Columns() []string { return []string{"x"} }

func (r *fakeRows) Close() error { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(1)
	return nil
}

func (r *fakeRows) ColumnTypeDatabaseTypeName(int) string { return "BIGINT" }

func (r *fakeRows) ColumnTypeScanType(int) reflect.Type { return reflect.TypeFor[int64]() }

func (r *fakeRows) ColumnTypeNullable(int) (bool, bool) { return false, true }

// paramStatement is a statement that adds a driver-specific parameter to
// every execution.
type paramStatement struct {
	*StatementImplBase
}

func (s *paramStatement) GetAdditionalExecParams() []any {
	return []any{sql.Named("param", "value")}
}

func (s *paramStatement) CreateStatement(stmt *StatementImplBase) (StatementImpl, error) {
	return &paramStatement{StatementImplBase: stmt}, nil
}

// newFakeStatement opens a statement on a fakeDriver, using paramStatement.
func newFakeStatement(t *testing.T, d *fakeDriver) adbc.StatementWithContext {
	ctx := context.Background()
	drv := NewDriver(memory.DefaultAllocator, "fake", "Fake", d).WithStatementFactory(&paramStatement{})
	db, err := drv.NewDatabaseWithContext(ctx, map[string]string{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })
	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cnxn.Close(ctx)) })
	stmt, err := cnxn.NewStatement(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stmt.Close(ctx)) })
	return stmt
}
