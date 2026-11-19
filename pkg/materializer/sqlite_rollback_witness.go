package materializer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

// openRollbackWitnessSQLite wraps only the single writer connection. Its
// transaction boundary suppresses a redundant ROLLBACK after SQLite has
// already ended the transaction through an error.
func openRollbackWitnessSQLite(dsn string, init func(*sqlite3.Conn) error) (*sql.DB, error) {
	connector, err := (&sqlite3driver.SQLite{}).OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(&rollbackWitnessConnector{Connector: connector, init: init}), nil
}

type rollbackWitnessConnector struct {
	driver.Connector
	init func(*sqlite3.Conn) error
}

func (c *rollbackWitnessConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := conn.(sqlite3driver.Conn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected SQLite driver connection %T", conn)
	}
	if err := c.init(sqliteConn.Raw()); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &rollbackWitnessConn{Conn: sqliteConn}, nil
}

type rollbackWitnessConn struct {
	sqlite3driver.Conn
	mu          sync.Mutex
	tx          *rollbackWitnessTx
	retire      bool
	stmtErr     error
	stmtFailed  bool
	autoSkipped bool
}

type rollbackWitnessTx struct {
	driver.Tx
	owner           *rollbackWitnessConn
	hasReset        bool
	armed           bool
	commitAttempted bool
	done            bool
}

func (c *rollbackWitnessConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return c.track(tx, false), nil
}

func (c *rollbackWitnessConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return c.track(tx, opts.ReadOnly), nil
}

func (c *rollbackWitnessConn) track(tx driver.Tx, hasReset bool) driver.Tx {
	wrapped := &rollbackWitnessTx{Tx: tx, owner: c, hasReset: hasReset}
	c.mu.Lock()
	c.tx = wrapped
	c.mu.Unlock()
	return wrapped
}

// armAutoRollback records the exact owned transaction only after an apply
// failure and before Go cleanup. A missing or active native transaction never
// authorizes skipping ROLLBACK.
func (c *rollbackWitnessConn) armAutoRollback() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	tx := c.tx
	if tx == nil || tx.done || tx.commitAttempted || tx.hasReset {
		return false
	}
	tx.armed = c.Conn.Raw().GetAutocommit()
	return tx.armed
}

func (c *rollbackWitnessConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	exec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return exec.ExecContext(ctx, query, args)
}

func (c *rollbackWitnessConn) CheckNamedValue(value *driver.NamedValue) error {
	checker, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(value)
}

func (c *rollbackWitnessConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &rollbackWitnessStmt{Stmt: stmt, owner: c}, nil
}

func (c *rollbackWitnessConn) takeRetire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	retire := c.retire
	c.retire = false
	return retire
}

func (c *rollbackWitnessConn) takeStatementCloseError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.stmtErr
	c.stmtErr = nil
	if err != nil {
		c.stmtFailed = true
	}
	return err
}

func (c *rollbackWitnessConn) statementCloseFailed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stmtFailed
}

func (c *rollbackWitnessConn) autoRollbackSkipped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.autoSkipped
}

func (c *rollbackWitnessConn) setStatementCloseError(err error) {
	c.mu.Lock()
	c.stmtErr = err
	c.mu.Unlock()
}

type rollbackWitnessStmt struct {
	driver.Stmt
	owner *rollbackWitnessConn
}

func (s *rollbackWitnessStmt) Close() error {
	return errors.Join(s.Stmt.Close(), s.owner.takeStatementCloseError())
}

func (s *rollbackWitnessStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	exec, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return exec.ExecContext(ctx, args)
}

func (s *rollbackWitnessStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return query.QueryContext(ctx, args)
}

func (s *rollbackWitnessStmt) CheckNamedValue(value *driver.NamedValue) error {
	checker, ok := s.Stmt.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(value)
}

func (tx *rollbackWitnessTx) finish() {
	tx.done = true
	if tx.owner.tx == tx {
		tx.owner.tx = nil
	}
}

func (tx *rollbackWitnessTx) Commit() error {
	tx.owner.mu.Lock()
	tx.commitAttempted = true
	tx.finish()
	tx.owner.mu.Unlock()
	return tx.Tx.Commit()
}

func (tx *rollbackWitnessTx) Rollback() error {
	tx.owner.mu.Lock()
	if tx.owner.tx == tx && tx.armed && !tx.done && !tx.commitAttempted && !tx.hasReset && tx.owner.Conn.Raw().GetAutocommit() {
		tx.owner.retire = true
		tx.owner.autoSkipped = true
		tx.finish()
		tx.owner.mu.Unlock()
		return nil
	}
	tx.finish()
	tx.owner.mu.Unlock()
	return tx.Tx.Rollback()
}
