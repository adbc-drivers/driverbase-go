package sql

import (
	"context"
	"database/sql"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// statementImpl implements the ADBC Statement interface on top of database/sql.
type statementImpl struct {
	driverbase.StatementImplBase

	// conn is the dedicated SQL connection
	conn *sql.Conn
	// query holds the SQL to execute
	query string
	// stmt holds the prepared statement, if Prepare() was called
	stmt *sql.Stmt
}

// Base returns the embedded StatementImplBase for driverbase plumbing
func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

// newStatement constructs a new statementImpl wrapped by driverbase
func newStatement(c *connectionImpl) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ConnectionImplBase.ErrorHelper)
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: base,
		conn:              c.conn,
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *statementImpl) SetSqlQuery(query string) error {
	// if someone resets the SQL after Prepare, clean up the old stmt
	if s.stmt != nil {
		s.stmt.Close()
		s.stmt = nil
	}
	s.query = query
	return nil
}

// GetQuery returns the stored SQL text
func (s *statementImpl) GetQuery() (string, error) {
	return s.query, nil
}

// Bind is not supported in this driver
func (s *statementImpl) Bind(ctx context.Context, record arrow.Record) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "Bind not supported")
}

// BindStream is not supported
func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "BindStream not supported")
}

// ExecuteUpdate runs DML/DDL and returns rows affected
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	var res sql.Result
	var err error

	if s.stmt != nil {
		res, err = s.stmt.ExecContext(ctx)
	} else {
		res, err = s.conn.ExecContext(ctx, s.query)
	}
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ExecuteSchema returns the Arrow schema by querying zero rows
func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	err := s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema not supported")
	return arrow.NewSchema([]arrow.Field{}, nil), err
}

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	err := s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteQuery not supported")
	return nil, -1, err
}

// Close shuts down the prepared stmt (if any)
func (s *statementImpl) Close() error {
	if s.stmt != nil {
		return s.stmt.Close()
	}
	return nil
}

// ExecutePartitions handles partitioned execution; not supported here
func (s *statementImpl) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	err := s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions not supported")
	return nil, adbc.Partitions{}, 0, err
}

// GetParameterSchema returns the schema for query parameters; not supported here.
func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema not supported")
}

// Prepare actually prepares the query on the connection
func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return s.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "no query to prepare")
	}
	ps, err := s.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return err
	}
	s.stmt = ps
	return nil
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *statementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan not supported")
}
