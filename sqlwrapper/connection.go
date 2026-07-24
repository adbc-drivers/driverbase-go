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

package sqlwrapper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
)

type ConnectionImpl interface {
	driverbase.ConnectionImpl

	// Track a pending operation that blocks other pending operations
	// (generally, a query with a result set, which needs to be cancelled
	// before running other queries on this connection).
	OfferPending(io.Closer) error
	// Cancel any other running queries.
	ClearPending() error
}

// ConnectionImplBase implements the ADBC Connection interface on top of database/sql.
type ConnectionImplBase struct {
	driverbase.ConnectionImplBase
	Derived  ConnectionImpl
	Database *DatabaseImplBase

	// Conn is the dedicated SQL connection for this ADBC session
	Conn *LoggingConn
	// db is the underlying database for metadata operations
	Db *sql.DB

	// tx is the active transaction, or nil when autocommit is on.
	tx *sql.Tx
	// isolationLevel is the last-set ADBC isolation level, applied to
	// each BeginTx. LevelDefault means "use the server's default".
	isolationLevel adbc.OptionIsolationLevel

	Pending io.Closer
}

// adbcToSqlIsolationLevel maps ADBC OptionIsolationLevel values to the
// standard library's sql.IsolationLevel. All known ADBC levels are mapped;
// only unknown strings produce an error. The underlying database/sql driver
// decides at BeginTx time whether a given level is actually supported.
func adbcToSqlIsolationLevel(level adbc.OptionIsolationLevel) (sql.IsolationLevel, error) {
	switch level {
	case adbc.LevelDefault:
		return sql.LevelDefault, nil
	case adbc.LevelReadUncommitted:
		return sql.LevelReadUncommitted, nil
	case adbc.LevelReadCommitted:
		return sql.LevelReadCommitted, nil
	case adbc.LevelRepeatableRead:
		return sql.LevelRepeatableRead, nil
	case adbc.LevelSnapshot:
		return sql.LevelSnapshot, nil
	case adbc.LevelSerializable:
		return sql.LevelSerializable, nil
	case adbc.LevelLinearizable:
		return sql.LevelLinearizable, nil
	default:
		return sql.LevelDefault, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown isolation level %q", string(level)),
		}
	}
}

// beginNewTx starts a new transaction on the dedicated connection, applying
// the cached isolation level (if any). The resulting *sql.Tx is stored on
// both the ConnectionImplBase and the LoggingConn so that all subsequent
// statement execution routes through the transaction.
//
// The transaction is started with a context derived from the caller's context
// via context.WithoutCancel so that it survives across CGO driver-manager
// calls, each of which creates and cancels its own context. The tx lives
// until explicitly committed or rolled back via Commit/Rollback.
func (c *ConnectionImplBase) beginNewTx(ctx context.Context) error {
	opts := &sql.TxOptions{}
	if c.isolationLevel != "" && c.isolationLevel != adbc.LevelDefault {
		level, err := adbcToSqlIsolationLevel(c.isolationLevel)
		if err != nil {
			return err
		}
		opts.Isolation = level
	}
	tx, err := c.Conn.Conn.BeginTx(context.WithoutCancel(ctx), opts)
	if err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "BEGIN failed: %v", err)
	}
	c.tx = tx
	c.Conn.Tx = tx
	return nil
}

// newConnection creates a new ADBC Connection by acquiring a *sql.Conn from the pool.
func newConnection(ctx context.Context, db *DatabaseImplBase) (adbc.ConnectionWithContext, error) {
	// Acquire a dedicated session
	sqlConn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, db.ErrorHelper.WrapIO(err, "failed to acquire database connection")
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)

	// Create the base sqlwrapper connection first
	sqlwrapperConn := &ConnectionImplBase{
		ConnectionImplBase: base,
		Database:           db,
		Conn:               &LoggingConn{Conn: sqlConn, Logger: base.Logger},
		Db:                 db.db,
	}

	var impl ConnectionImpl

	// Use custom factory if provided, otherwise use default implementation
	if db.connectionFactory != nil {
		impl, err = db.connectionFactory.CreateConnection(ctx, sqlwrapperConn)
		if err != nil {
			err = errors.Join(err, sqlConn.Close())
			return nil, err
		}
	} else {
		// Default sqlwrapper connection implementation
		impl = sqlwrapperConn
	}
	sqlwrapperConn.Derived = impl

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)

	// If the implementation supports any of these extras, register them
	if trait, ok := any(impl).(driverbase.AutocommitSetter); ok {
		builder.WithAutocommitSetter(trait)
	}
	if trait, ok := any(impl).(driverbase.CurrentNamespacer); ok {
		builder.WithCurrentNamespacer(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumerator); ok {
		builder.WithDbObjectsEnumerator(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumeratorFactory); ok {
		builder.WithDbObjectsEnumeratorFactory(trait)
	}
	if trait, ok := any(impl).(driverbase.DriverInfoPreparer); ok {
		builder.WithDriverInfoPreparer(trait)
	}
	if trait, ok := any(impl).(driverbase.TableTypeLister); ok {
		builder.WithTableTypeLister(trait)
	}

	return builder.Connection(), nil
}

// NewStatement satisfies adbc.Connection
func (c *ConnectionImplBase) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	return newStatement(c)
}

// SetOption sets a string option on this connection.
// Handles OptionKeyIsolationLevel by caching the level for the next BeginTx;
// if a transaction is already active (autocommit was disabled before the
// isolation level was set), it restarts the transaction with the new level
// so that subsequent statements use the correct isolation.
// All other keys are delegated to the embedded driverbase base.
func (c *ConnectionImplBase) SetOption(ctx context.Context, key, value string) error {
	switch key {
	case adbc.OptionKeyIsolationLevel:
		level := adbc.OptionIsolationLevel(value)
		if _, err := adbcToSqlIsolationLevel(level); err != nil {
			return err
		}
		c.isolationLevel = level
		// If a transaction is already active (e.g. SetAutocommit(false) was
		// called before SetOption(isolation=...), as sqldriver.BeginTx does),
		// restart it so the new isolation level takes effect. This is safe
		// because no user statements have been executed yet when called via
		// sqldriver.BeginTx (which sets autocommit first, then isolation,
		// before returning the tx handle to the caller).
		if c.tx != nil {
			if err := c.tx.Rollback(); err != nil {
				return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "ROLLBACK for isolation restart failed: %v", err)
			}
			c.tx = nil
			c.Conn.Tx = nil
			return c.beginNewTx(ctx)
		}
		return nil
	default:
		return c.ConnectionImplBase.SetOption(ctx, key, value)
	}
}

// GetOption returns a string option value.
// Handles OptionKeyIsolationLevel by returning the cached level.
func (c *ConnectionImplBase) GetOption(ctx context.Context, key string) (string, error) {
	switch key {
	case adbc.OptionKeyIsolationLevel:
		return string(c.isolationLevel), nil
	default:
		return c.ConnectionImplBase.GetOption(ctx, key)
	}
}

// SetAutocommit implements driverbase.AutocommitSetter.
// Disabling autocommit begins a transaction on the dedicated session
// connection; enabling it commits any in-flight transaction and lets
// subsequent statements auto-commit (the database/sql default).
// The driverbase wrapper updates Base().Autocommit after a successful return.
func (c *ConnectionImplBase) SetAutocommit(ctx context.Context, enabled bool) error {
	if enabled {
		if c.tx != nil {
			if err := c.tx.Commit(); err != nil {
				return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "COMMIT failed: %v", err)
			}
			c.tx = nil
			c.Conn.Tx = nil
		}
		return nil
	}
	if c.tx == nil {
		return c.beginNewTx(ctx)
	}
	return nil
}

// Commit commits the current transaction. After COMMIT a new transaction is
// immediately begun (chained-transaction semantics) so the next statement
// continues to run inside a transaction until the caller re-enables autocommit.
// The driverbase wrapper short-circuits to StatusInvalidState when autocommit
// is enabled, so this method is only reached when autocommit is disabled.
func (c *ConnectionImplBase) Commit(ctx context.Context) error {
	if c.tx == nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "no active transaction")
	}
	if err := c.tx.Commit(); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "COMMIT failed: %v", err)
	}
	return c.beginNewTx(ctx)
}

// Rollback rolls back the current transaction. Like Commit, it begins a new
// transaction immediately afterwards (chained-transaction semantics).
// The driverbase wrapper short-circuits to StatusInvalidState when autocommit
// is enabled, so this method is only reached when autocommit is disabled.
func (c *ConnectionImplBase) Rollback(ctx context.Context) error {
	if c.tx == nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "no active transaction")
	}
	if err := c.tx.Rollback(); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "ROLLBACK failed: %v", err)
	}
	return c.beginNewTx(ctx)
}

// Close closes the underlying SQL connection. If a transaction is active,
// it is rolled back first to avoid leaving the connection in an inconsistent
// state.
func (c *ConnectionImplBase) Close(ctx context.Context) error {
	if c.tx != nil {
		_ = c.tx.Rollback()
		c.tx = nil
		c.Conn.Tx = nil
	}
	if err := c.ClearPending(); err != nil {
		return errors.Join(err, c.Conn.Close())
	}
	return c.Conn.Close()
}

func (c *ConnectionImplBase) OfferPending(pending io.Closer) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	c.Pending = pending
	return nil
}

func (c *ConnectionImplBase) ClearPending() error {
	if c.Pending == nil {
		return nil
	}
	defer func() {
		c.Pending = nil
	}()
	if err := c.Pending.Close(); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "failed to clear pending operation: %v", err)
	}
	return nil
}

var _ ConnectionImpl = (*ConnectionImplBase)(nil)
