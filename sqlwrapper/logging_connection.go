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

// Wrappers around database/sql types to add logging for debug purposes.  This
// is only enabled when compiling with the assert build tag; otherwise
// everything is a no-op (to avoid logging overhead and potentially logging
// something sensitive).

//go:build !assert

package sqlwrapper

import (
	"context"
	"database/sql"
	"log/slog"
)

type LoggingConn struct {
	Conn   *sql.Conn
	Logger *slog.Logger
}

func (tc *LoggingConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tc.Conn.ExecContext(ctx, query, args...)
}

func (tc *LoggingConn) QueryContext(ctx context.Context, query string, args ...any) (*LoggingRows, error) {
	rows, err := tc.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &LoggingRows{Rows: rows, Logger: tc.Logger}, err
}

func (tc *LoggingConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tc.Conn.QueryRowContext(ctx, query, args...)
}

func (tc *LoggingConn) PingContext(ctx context.Context) error {
	return tc.Conn.PingContext(ctx)
}

func (tc *LoggingConn) PrepareContext(ctx context.Context, query string) (*LoggingStmt, error) {
	stmt, err := tc.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &LoggingStmt{
		Stmt:   stmt,
		Logger: tc.Logger,
	}, nil
}

func (tc *LoggingConn) Close() error {
	return tc.Conn.Close()
}

type LoggingRows struct {
	Rows   *sql.Rows
	Logger *slog.Logger
}

func (lr *LoggingRows) Close() error {
	return lr.Rows.Close()
}

func (lr *LoggingRows) Columns() ([]string, error) {
	return lr.Rows.Columns()
}

func (lr *LoggingRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return lr.Rows.ColumnTypes()
}

func (lr *LoggingRows) Err() error {
	return lr.Rows.Err()
}

func (lr *LoggingRows) Next() bool {
	return lr.Rows.Next()
}

func (lr *LoggingRows) NextResultSet() bool {
	return lr.Rows.NextResultSet()
}

func (lr *LoggingRows) Scan(dest ...any) error {
	return lr.Rows.Scan(dest...)
}

type LoggingStmt struct {
	Stmt   *sql.Stmt
	Logger *slog.Logger
}

func (ls *LoggingStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return ls.Stmt.ExecContext(ctx, args...)
}

func (ls *LoggingStmt) QueryContext(ctx context.Context, args ...any) (*LoggingRows, error) {
	rows, err := ls.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &LoggingRows{Rows: rows, Logger: ls.Logger}, err
}

func (ls *LoggingStmt) Close() error {
	return ls.Stmt.Close()
}
