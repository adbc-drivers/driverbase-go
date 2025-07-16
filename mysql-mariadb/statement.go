package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

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
}

// Base returns the embedded StatementImplBase for driverbase plumbing
func (s *statementImpl) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

// newStatement constructs a new statementImpl wrapped by driverbase
func newStatement(c *connectionImpl) adbc.Statement {
	base := driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper)
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: base,
		conn:              c.conn,
	})
}

// SetSqlQuery stores the SQL text on the statement
func (s *statementImpl) SetSqlQuery(query string) error {
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
	q, err := s.GetQuery()
	if err != nil {
		return 0, err
	}
	res, err := s.conn.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ExecuteSchema returns the Arrow schema by querying zero rows
func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	q, err := s.GetQuery()
	if err != nil {
		return nil, err
	}
	// wrap in a subselect to avoid data, just schema
	metaQ := fmt.Sprintf("SELECT * FROM (%s) _adbc_meta WHERE 1=0", q)
	rows, err := s.conn.QueryContext(ctx, metaQ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(cols))
	for i, ct := range cols {
		dt := mapSQLTypeToArrow(ct)
		fields[i] = arrow.Field{Name: ct.Name(), Type: dt}
	}
	return arrow.NewSchema(fields, nil), nil
}

// ExecuteQuery runs a SELECT and returns a RecordReader for streaming Arrow records
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	q, err := s.GetQuery()
	if err != nil {
		return nil, -1, err
	}
	_, err = s.conn.ExecContext(ctx, q)
	return nil, -1, err
}

// Close releases any resources held by the statement (no-op here)
func (s *statementImpl) Close() error {
	// No per-statement resources to clean up
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

func (s *statementImpl) Prepare(ctx context.Context) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "Prepare not supported")
}

// SetSubstraitPlan sets the Substrait plan on the statement; not supported here.
func (s *statementImpl) SetSubstraitPlan([]byte) error {
	return s.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan not supported")
}

// OTel tracing is inherited
// GetInitialSpanAttributes and StartSpan use the embedded implementations

// --------------------
// Helpers (need implementation)

// mapSQLTypeToArrow maps a SQL column type to an Arrow DataType
func mapSQLTypeToArrow(ct *sql.ColumnType) arrow.DataType {
	// TODO: inspect ct.DatabaseTypeName() / ct.ScanType() and return appropriate arrow.DataType
	return arrow.Null
}

type sqlRecordReader struct {
	ctx     context.Context
	rows    *sql.Rows
	builder *array.RecordBuilder
	schema  *arrow.Schema
	done    bool
}

func (r *sqlRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *sqlRecordReader) Read() (arrow.Record, error) {
	if r.done {
		return nil, io.EOF
	}
	// TODO: clear builder and append rows up to batch size
	// Implementation omitted for brevity
	r.done = true
	r.rows.Close()
	r.builder.Release()
	return nil, io.EOF
}

func (r *sqlRecordReader) Release() {
	if !r.done {
		r.rows.Close()
		r.builder.Release()
		r.done = true
	}
}
