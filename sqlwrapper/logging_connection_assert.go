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

//go:build assert

package sqlwrapper

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/apache/arrow-adbc/go/adbc"
)

type LoggingConn struct {
	Conn   *sql.Conn
	Tx     *sql.Tx
	Logger *slog.Logger
}

func (tc *LoggingConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tc.Tx != nil {
		rs, err := tc.Tx.ExecContext(ctx, query, args...)
		tc.Logger.DebugContext(ctx, "LoggingConn.Tx.ExecContext", slog.String("query", query), slog.Any("args", args), slog.Any("err", err))
		return rs, err
	}
	if tc.Conn == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingConn.ExecContext: nil connection"}
	}
	rs, err := tc.Conn.ExecContext(ctx, query, args...)
	tc.Logger.DebugContext(ctx, "LoggingConn.ExecContext", slog.String("query", query), slog.Any("args", args), slog.Any("err", err))
	return rs, err
}

func (tc *LoggingConn) QueryContext(ctx context.Context, query string, args ...any) (*LoggingRows, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if tc.Tx != nil {
		rows, err = tc.Tx.QueryContext(ctx, query, args...)
		tc.Logger.DebugContext(ctx, "LoggingConn.Tx.QueryContext", slog.String("query", query), slog.Any("args", args), slog.Any("err", err))
	} else {
		if tc.Conn == nil {
			return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingConn.QueryContext: nil connection"}
		}
		rows, err = tc.Conn.QueryContext(ctx, query, args...)
		tc.Logger.DebugContext(ctx, "LoggingConn.QueryContext", slog.String("query", query), slog.Any("args", args), slog.Any("err", err))
	}
	if err != nil {
		return nil, err
	}
	return &LoggingRows{Rows: rows, Logger: tc.Logger}, err
}

func (tc *LoggingConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if tc.Tx != nil {
		tc.Logger.DebugContext(ctx, "LoggingConn.Tx.QueryRowContext", slog.String("query", query), slog.Any("args", args))
		return tc.Tx.QueryRowContext(ctx, query, args...)
	}
	if tc.Conn == nil {
		// We can't construct a sql.Row ourselves
		panic("LoggingConn.QueryRowContext: nil connection")
	}
	tc.Logger.DebugContext(ctx, "LoggingConn.QueryRowContext", slog.String("query", query), slog.Any("args", args))
	return tc.Conn.QueryRowContext(ctx, query, args...)
}

func (tc *LoggingConn) PingContext(ctx context.Context) error {
	if tc.Conn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingConn.PingContext: nil connection"}
	}
	err := tc.Conn.PingContext(ctx)
	tc.Logger.DebugContext(ctx, "LoggingConn.PingContext", slog.Any("err", err))
	return err
}

func (tc *LoggingConn) PrepareContext(ctx context.Context, query string) (*LoggingStmt, error) {
	if tc.Conn == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingConn.PrepareContext: nil connection"}
	}
	stmt, err := tc.Conn.PrepareContext(ctx, query)
	tc.Logger.DebugContext(ctx, "LoggingConn.PrepareContext", slog.String("query", query), slog.Any("err", err))
	if err != nil {
		return nil, err
	}
	return &LoggingStmt{
		Stmt:   stmt,
		conn:   tc,
		Logger: tc.Logger,
	}, nil
}

func (tc *LoggingConn) Close() error {
	if tc.Conn != nil {
		defer func() {
			tc.Conn = nil
		}()
		err := tc.Conn.Close()
		tc.Logger.Debug("LoggingConn.Close", slog.Any("err", err))
		return err
	}
	return nil
}

type LoggingRows struct {
	Rows   *sql.Rows
	Logger *slog.Logger
}

func (lr *LoggingRows) Close() error {
	if lr.Rows != nil {
		defer func() {
			lr.Rows = nil
		}()
		err := lr.Rows.Close()
		lr.Logger.Debug("LoggingRows.Close", slog.Any("err", err))
		return err
	}
	return nil
}

func (lr *LoggingRows) Columns() ([]string, error) {
	if lr.Rows == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingRows.Columns: nil rows"}
	}
	cols, err := lr.Rows.Columns()
	lr.Logger.Debug("LoggingRows.Columns", slog.Any("cols", cols), slog.Any("err", err))
	return cols, err
}

func (lr *LoggingRows) ColumnTypes() ([]*sql.ColumnType, error) {
	if lr.Rows == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingRows.ColumnTypes: nil rows"}
	}
	cols, err := lr.Rows.ColumnTypes()
	lr.Logger.Debug("LoggingRows.ColumnTypes", slog.Any("cols", cols), slog.Any("err", err))
	return cols, err
}

func (lr *LoggingRows) Err() error {
	if lr.Rows == nil {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingRows.Err: nil rows"}
	}
	err := lr.Rows.Err()
	lr.Logger.Debug("LoggingRows.Err", slog.Any("err", err))
	return err
}

func (lr *LoggingRows) Next() bool {
	if lr.Rows == nil {
		return false
	}
	ok := lr.Rows.Next()
	if ok {
		lr.Logger.Debug("LoggingRows.Next", slog.Bool("ok", ok))
	} else {
		lr.Logger.Info("LoggingRows.Next", slog.Bool("ok", ok), slog.Any("err", lr.Rows.Err()))
	}
	return ok
}

func (lr *LoggingRows) NextResultSet() bool {
	if lr.Rows == nil {
		return false
	}
	ok := lr.Rows.NextResultSet()
	if ok {
		lr.Logger.Debug("LoggingRows.NextResultSet", slog.Bool("ok", ok))
	} else {
		lr.Logger.Info("LoggingRows.NextResultSet", slog.Bool("ok", ok), slog.Any("err", lr.Rows.Err()))
	}
	return ok
}

func (lr *LoggingRows) Scan(dest ...any) error {
	if lr.Rows == nil {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingRows.Scan: nil rows"}
	}
	err := lr.Rows.Scan(dest...)
	lr.Logger.Debug("LoggingRows.Scan", slog.Any("err", err))
	return err
}

type LoggingStmt struct {
	Stmt   *sql.Stmt
	txStmt *sql.Stmt    // cached tx-scoped wrapper, lazily created via tx.StmtContext
	txRef  *sql.Tx      // which *sql.Tx txStmt is associated with (nil == no wrapper)
	conn   *LoggingConn // back-ref to check the current tx at execute time
	Logger *slog.Logger
}

// getTxStmt returns the *sql.Stmt to use for execution. When a transaction is
// active, the conn-scoped Stmt is wrapped via tx.StmtContext so execution
// participates in the transaction. The wrapper is cached across calls within
// the same transaction and invalidated (closed + recreated) when the
// transaction changes (Commit/Rollback/Isolation swap via chained-tx
// semantics). When no transaction is active, the conn-scoped Stmt is used
// directly.
func (ls *LoggingStmt) getTxStmt(ctx context.Context) (*sql.Stmt, error) {
	if ls.conn == nil || ls.conn.Tx == nil {
		ls.Logger.DebugContext(ctx, "LoggingStmt.getTxStmt: no active tx, using conn-scoped stmt")
		return ls.Stmt, nil
	}
	if ls.txRef == ls.conn.Tx && ls.txStmt != nil {
		ls.Logger.DebugContext(ctx, "LoggingStmt.getTxStmt: cache hit (same tx)")
		return ls.txStmt, nil
	}
	if ls.txStmt != nil {
		ls.Logger.DebugContext(ctx, "LoggingStmt.getTxStmt: cache miss (tx changed), closing old wrapper")
		_ = ls.txStmt.Close()
		ls.txStmt = nil
		ls.txRef = nil
	}
	ls.Logger.DebugContext(ctx, "LoggingStmt.getTxStmt: creating tx.StmtContext wrapper")
	txStmt := ls.conn.Tx.StmtContext(ctx, ls.Stmt)
	ls.txStmt = txStmt
	ls.txRef = ls.conn.Tx
	return ls.txStmt, nil
}

func (ls *LoggingStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	if ls.Stmt == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingStmt.ExecContext: nil statement"}
	}
	stmt, err := ls.getTxStmt(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := stmt.ExecContext(ctx, args...)
	ls.Logger.DebugContext(ctx, "LoggingStmt.ExecContext", slog.Any("args", args), slog.Any("err", err))
	return rs, err
}

func (ls *LoggingStmt) QueryContext(ctx context.Context, args ...any) (*LoggingRows, error) {
	if ls.Stmt == nil {
		return nil, adbc.Error{Code: adbc.StatusInvalidState, Msg: "LoggingStmt.QueryContext: nil statement"}
	}
	stmt, err := ls.getTxStmt(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	ls.Logger.DebugContext(ctx, "LoggingStmt.QueryContext", slog.Any("args", args), slog.Any("err", err))
	return &LoggingRows{Rows: rows, Logger: ls.Logger}, err
}

func (ls *LoggingStmt) Close() error {
	var err error
	if ls.txStmt != nil {
		err = errors.Join(err, ls.txStmt.Close())
		ls.Logger.Debug("LoggingStmt.Close txStmt", slog.Any("err", err))
		ls.txStmt = nil
		ls.txRef = nil
	}
	if ls.Stmt != nil {
		err = errors.Join(err, ls.Stmt.Close())
		ls.Logger.Debug("LoggingStmt.Close", slog.Any("err", err))
		ls.Stmt = nil
	}
	return err
}
