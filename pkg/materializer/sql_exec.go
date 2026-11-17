package materializer

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func ValidateSQLCommand(command types.SQLCommand) error {
	if len(command.RequestID) > types.MaxRequestIDBytes {
		return fmt.Errorf("request_id must not exceed %d bytes", types.MaxRequestIDBytes)
	}
	statements := command.Statements
	if command.RequireOne && (!command.WantRows || len(statements) != 0) {
		return fmt.Errorf("require_one is only valid for one row-returning statement")
	}
	if command.Migration != nil {
		if command.SQL != "" || len(command.Args) != 0 || command.WantRows || command.Migration.Version <= 0 || command.Migration.Name == "" || len(command.Migration.Checksum) != 64 {
			return fmt.Errorf("invalid migration command")
		}
		if _, err := hex.DecodeString(command.Migration.Checksum); err != nil {
			return fmt.Errorf("invalid migration checksum")
		}
	}
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	if len(statements) == 0 || len(statements) > MaxSQLStatements {
		return fmt.Errorf("statement count must be between 1 and %d", MaxSQLStatements)
	}
	for statementIndex, statement := range statements {
		if strings.TrimSpace(statement.SQL) == "" {
			return fmt.Errorf("SQL is required")
		}
		if strings.IndexByte(statement.SQL, 0) >= 0 {
			return fmt.Errorf("SQL contains a null byte")
		}
		if len(statement.SQL) > MaxSQLBytes {
			return fmt.Errorf("SQL exceeds %d bytes", MaxSQLBytes)
		}
		argCount := len(statement.Args)
		if len(statement.OutputRefs) > MaxSQLArgs {
			return fmt.Errorf("SQL has more than %d output references", MaxSQLArgs)
		}
		if command.Migration != nil && (len(statement.Args) != 0 || statement.WantRows || statement.ExpectedRowsAffected != nil || statement.ExpectedReturnedRows != nil || len(statement.OutputRefs) != 0) {
			return fmt.Errorf("migration statements do not support arguments, returned rows, preconditions, or output references")
		}
		if statement.ExpectedRowsAffected != nil {
			if statement.WantRows {
				return fmt.Errorf("expected_rows_affected is not valid for a row-returning statement")
			}
			if *statement.ExpectedRowsAffected < 0 {
				return fmt.Errorf("expected_rows_affected must not be negative")
			}
		}
		if statement.ExpectedReturnedRows != nil {
			if !statement.WantRows {
				return fmt.Errorf("expected_returned_rows requires want_rows")
			}
			if *statement.ExpectedReturnedRows < 0 || *statement.ExpectedReturnedRows > MaxReturningRows {
				return fmt.Errorf("expected_returned_rows must be between 0 and %d", MaxReturningRows)
			}
		}
		if len(statement.OutputRefs) != 0 {
			parameterCount, err := countPlainParameters(statement.SQL)
			if err != nil {
				return err
			}
			if parameterCount != len(statement.Args) {
				return fmt.Errorf("output references require one argument for every positional parameter")
			}
		}
		seenArgs := make(map[int]struct{}, len(statement.OutputRefs))
		for _, ref := range statement.OutputRefs {
			if ref.ArgIndex < 0 || ref.ArgIndex >= len(statement.Args) {
				return fmt.Errorf("output reference argument index is out of bounds")
			}
			if statement.Args[ref.ArgIndex] != nil {
				return fmt.Errorf("output reference argument %d must be null", ref.ArgIndex)
			}
			if _, exists := seenArgs[ref.ArgIndex]; exists {
				return fmt.Errorf("duplicate output reference argument index %d", ref.ArgIndex)
			}
			seenArgs[ref.ArgIndex] = struct{}{}
			if ref.StatementIndex < 0 || ref.StatementIndex >= statementIndex || !statements[ref.StatementIndex].WantRows {
				return fmt.Errorf("output reference must target an earlier row-returning statement")
			}
			if (ref.ColumnName == "") == (ref.ColumnIndex == nil) {
				return fmt.Errorf("output reference requires exactly one column name or index")
			}
			if ref.ColumnIndex != nil && *ref.ColumnIndex < 0 {
				return fmt.Errorf("output reference column index is out of bounds")
			}
		}
		if argCount > MaxSQLArgs {
			return fmt.Errorf("SQL has more than %d arguments", MaxSQLArgs)
		}
		if err := validateReplicatedTempSQL(statement.SQL); err != nil {
			return err
		}
		if err := validatePublicSQL(statement.SQL); err != nil {
			return err
		}
		for _, arg := range statement.Args {
			if _, err := sqlArg(arg); err != nil {
				return err
			}
		}
		keyword := firstSQLKeyword(statement.SQL)
		if keyword == "" {
			return fmt.Errorf("SQL statement has no valid leading keyword")
		}
		switch keyword {
		case "PRAGMA", "ATTACH", "DETACH", "VACUUM", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE":
			return fmt.Errorf("%s is not allowed on the replicated write API", keyword)
		}
	}
	return nil
}

func countPlainParameters(query string) (int, error) {
	count := 0
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'', '"', '`':
			quote := query[i]
			i++
			for i < len(query) {
				if query[i] == quote {
					if i+1 < len(query) && query[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '[':
			i++
			for i < len(query) && query[i] != ']' {
				i++
			}
			if i < len(query) {
				i++
			}
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i += 2
				for i < len(query) && query[i] != '\n' {
					i++
				}
			} else {
				i++
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				i += 2
				for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
					i++
				}
				if i+1 < len(query) {
					i += 2
				}
			} else {
				i++
			}
		case '?':
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				return 0, fmt.Errorf("output references require unnumbered ? parameters")
			}
			count++
			i++
		case ':', '@', '$':
			if i+1 < len(query) && (query[i+1] == '_' || query[i+1] >= 'A' && query[i+1] <= 'Z' || query[i+1] >= 'a' && query[i+1] <= 'z') {
				return 0, fmt.Errorf("output references do not support named parameters")
			}
			i++
		default:
			i++
		}
	}
	return count, nil
}

func firstSQLKeyword(query string) string {
	for {
		query = strings.TrimSpace(query)
		switch {
		case strings.HasPrefix(query, "--"):
			if end := strings.IndexByte(query, '\n'); end >= 0 {
				query = query[end+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "/*"):
			if end := strings.Index(query[2:], "*/"); end >= 0 {
				query = query[end+4:]
				continue
			}
			return ""
		}
		break
	}
	end := 0
	for end < len(query) && (query[end] >= 'A' && query[end] <= 'Z' || query[end] >= 'a' && query[end] <= 'z' || query[end] == '_') {
		end++
	}
	return strings.ToUpper(query[:end])
}

func validatePublicSQL(query string) error {
	if strings.Contains(strings.ToLower(query), "_rhiza_") {
		return fmt.Errorf("the _rhiza_ namespace is reserved")
	}
	return nil
}

func executeSQLCommand(ctx context.Context, conn *sql.Conn, tx *sql.Tx, prepared map[string]*sql.Stmt, command types.SQLCommand) (types.SQLCommandResult, error) {
	statements := command.Statements
	if len(statements) == 0 {
		statements = []types.SQLStatement{{SQL: command.SQL, Args: command.Args, WantRows: command.WantRows}}
	}
	result := types.SQLCommandResult{Statements: make([]types.SQLStatementResult, 0, len(statements))}
	if command.Migration != nil {
		var name, checksum string
		err := tx.QueryRowContext(ctx, `SELECT name, checksum FROM _rhiza_migrations WHERE version = ?`, command.Migration.Version).Scan(&name, &checksum)
		if err == nil {
			if name == command.Migration.Name && checksum == command.Migration.Checksum {
				return result, nil
			}
			return result, fmt.Errorf("migration %d conflicts with the applied definition", command.Migration.Version)
		}
		if err != sql.ErrNoRows {
			return result, err
		}
		var maxVersion sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM _rhiza_migrations`).Scan(&maxVersion); err != nil {
			return result, err
		}
		expectedVersion := int64(1)
		if maxVersion.Valid {
			expectedVersion = maxVersion.Int64 + 1
		}
		if command.Migration.Version != expectedVersion {
			return result, fmt.Errorf("migration version %d does not follow %d", command.Migration.Version, expectedVersion-1)
		}
		if _, err := execPrepared(ctx, tx, prepared, `INSERT INTO _rhiza_migrations(version, name, checksum) VALUES (?, ?, ?)`, command.Migration.Version, command.Migration.Name, command.Migration.Checksum); err != nil {
			return result, err
		}
	}
	budget := resultBudget{rows: MaxReturningRows, bytes: MaxResultBytes, limit: MaxResultBytes}
	resolvedBytes := 0
	wantsRows := command.WantRows
	for statementIndex, statement := range statements {
		budget.bytes -= maxSQLStatementCharge
		if budget.bytes < 0 {
			return result, fmt.Errorf("SQL result exceeds %d bytes of working memory", budget.limit)
		}
		args := make([]any, len(statement.Args))
		for i, arg := range statement.Args {
			value, err := sqlArg(arg)
			if err != nil {
				return result, err
			}
			args[i] = value
		}
		for _, ref := range statement.OutputRefs {
			prior := result.Statements[ref.StatementIndex]
			if len(prior.Rows) != 1 {
				return result, fmt.Errorf("statement %d must return exactly one row", ref.StatementIndex)
			}
			column := -1
			if ref.ColumnIndex != nil {
				column = *ref.ColumnIndex
			} else {
				matches := 0
				for i, name := range prior.Columns {
					if name == ref.ColumnName {
						column = i
						matches++
					}
				}
				if matches != 1 {
					return result, fmt.Errorf("statement %d output column %q must be unique", ref.StatementIndex, ref.ColumnName)
				}
			}
			if column < 0 || column >= len(prior.Rows[0]) {
				return result, fmt.Errorf("statement %d output column is out of bounds", ref.StatementIndex)
			}
			value := prior.Rows[0][column]
			resolvedBytes += sqlValueSize(value)
			if resolvedBytes > MaxMutationResultBytes {
				return result, fmt.Errorf("resolved output references exceed %d bytes", MaxMutationResultBytes)
			}
			args[ref.ArgIndex] = value
		}
		statementResult, err := func() (out types.SQLStatementResult, resultErr error) {
			restore, err := beginUserSQL(conn, singleSchemaMaintenanceSQL(statement.SQL))
			if err != nil {
				return out, err
			}
			var finishRowID func() error
			defer func() {
				if err := restore(); err != nil {
					out, resultErr = types.SQLStatementResult{}, errors.Join(resultErr, err)
					return
				}
				if finishRowID != nil {
					if err := finishRowID(); err != nil {
						out, resultErr = types.SQLStatementResult{}, err
					}
				}
			}()
			query, err := preparedStatement(ctx, tx, prepared, statement.SQL)
			if err != nil {
				return types.SQLStatementResult{}, err
			}
			// SQLite preserves Changes across non-DML statements. A zero-row
			// internal delete clears it without changing durable state.
			reset, err := preparedStatement(ctx, tx, prepared, `DELETE FROM _rhiza_meta WHERE 0`)
			if err != nil {
				return types.SQLStatementResult{}, err
			}
			if _, err := reset.ExecContext(ctx); err != nil {
				return types.SQLStatementResult{}, err
			}
			// The driver's LastInsertId reads native connection state even for an
			// UPDATE. Reset it for each user statement so receipts cannot inherit
			// an earlier insert (or differ after reconnecting the writer).
			if err := conn.Raw(func(driverConn any) error {
				driverConn.(sqlite3driver.Conn).Raw().SetLastInsertRowID(0)
				return nil
			}); err != nil {
				return types.SQLStatementResult{}, err
			}
			finishRowID, err = watchRowID(ctx, conn, tx)
			if err != nil {
				return out, err
			}
			if statement.WantRows {
				wantsRows = true
				rows, err := query.QueryContext(ctx, args...)
				if err != nil {
					return types.SQLStatementResult{}, err
				}
				statementResult, err := collectRowsWithBudget(rows, &budget, statement.ExpectedReturnedRows)
				rows.Close()
				if err != nil {
					return types.SQLStatementResult{}, err
				}
				if command.RequireOne && len(statementResult.Rows) != 1 {
					return types.SQLStatementResult{}, fmt.Errorf("%w: statement must return exactly one row", errSQLPreconditionFailed)
				}
				if statement.ExpectedReturnedRows != nil && int64(len(statementResult.Rows)) != *statement.ExpectedReturnedRows {
					return types.SQLStatementResult{}, fmt.Errorf("%w: statement %d returned %d rows, expected %d", errSQLPreconditionFailed, statementIndex, len(statementResult.Rows), *statement.ExpectedReturnedRows)
				}
				return statementResult, nil
			}
			execResult, err := query.ExecContext(ctx, args...)
			if err != nil {
				return types.SQLStatementResult{}, err
			}
			statementResult := types.SQLStatementResult{}
			statementResult.RowsAffected, err = execResult.RowsAffected()
			if err != nil && statement.ExpectedRowsAffected != nil {
				return types.SQLStatementResult{}, err
			}
			statementResult.LastInsertID, _ = execResult.LastInsertId()
			if statement.ExpectedRowsAffected != nil && statementResult.RowsAffected != *statement.ExpectedRowsAffected {
				return types.SQLStatementResult{}, fmt.Errorf("%w: statement %d affected %d rows, expected %d", errSQLPreconditionFailed, statementIndex, statementResult.RowsAffected, *statement.ExpectedRowsAffected)
			}
			return statementResult, nil
		}()
		if err != nil {
			return result, err
		}
		result.Statements = append(result.Statements, statementResult)
	}
	if wantsRows {
		if _, err := encodeSQLResult(result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func preparedStatement(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, query string) (*sql.Stmt, error) {
	if statement := prepared[query]; prepared != nil && statement != nil {
		return statement, nil
	}
	statement, err := tx.PrepareContext(ctx, query)
	if err == nil && prepared != nil {
		prepared[query] = statement
	}
	return statement, err
}

func execPrepared(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt, query string, args ...any) (sql.Result, error) {
	statement, err := preparedStatement(ctx, tx, prepared, query)
	if err != nil {
		return nil, err
	}
	return statement.ExecContext(ctx, args...)
}

func sqlArg(arg any) (any, error) {
	switch value := arg.(type) {
	case nil, bool, string, []byte, int64, float64:
		return value, nil
	case map[string]any:
		encoded, ok := value["$rhiza_blob"]
		if !ok || len(value) != 1 {
			return nil, fmt.Errorf("unsupported SQL argument type %T", arg)
		}
		text, ok := encoded.(string)
		if !ok {
			return nil, fmt.Errorf("invalid SQL blob argument")
		}
		blob, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, fmt.Errorf("invalid SQL blob argument: %w", err)
		}
		return blob, nil
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return nil, fmt.Errorf("SQL argument overflows int64")
		}
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf("SQL argument overflows int64")
		}
		return int64(value), nil
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer, nil
		}
		floating, err := value.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid numeric SQL argument %q", value)
		}
		return floating, nil
	default:
		return nil, fmt.Errorf("unsupported SQL argument type %T", arg)
	}
}

// NormalizeSQLArgs converts JSON numbers into database/sql argument types.
func NormalizeSQLArgs(args []any) ([]any, error) {
	result := make([]any, len(args))
	for i, arg := range args {
		value, err := sqlArg(arg)
		if err != nil {
			return nil, err
		}
		result[i] = value
	}
	return result, nil
}

type resultBudget struct {
	rows  int
	bytes int
	limit int
}

func sqlResultWorkingBytes(result types.SQLCommandResult) int {
	size := 0
	add := func(n int) {
		if n > MaxResultBytes-size {
			size = MaxResultBytes + 1
		} else {
			size += n
		}
	}
	for _, statement := range result.Statements {
		add(maxSQLStatementCharge)
		for _, column := range statement.Columns {
			add(maxSQLColumnCharge + len(column))
		}
		for _, row := range statement.Rows {
			add(maxSQLRowCharge + len(row)*maxSQLCellCharge)
			for _, value := range row {
				add(sqlValueSize(value))
			}
		}
	}
	return size
}

func (m *Materializer) flushPendingSQLReceipts(ctx context.Context, tx *sql.Tx, prepared map[string]*sql.Stmt) error {
	start := 0
	for start < len(m.pendingSQLReceipts) && m.pendingSQLReceipts[start].persisted {
		start++
	}
	if start == len(m.pendingSQLReceipts) {
		return nil
	}
	inserted, err := insertReceiptsIfAbsent(ctx, tx, prepared, types.MutationSQL, m.pendingSQLReceipts[start:])
	if err != nil {
		return fmt.Errorf("record SQL receipts: %w", err)
	}
	if !inserted {
		return fmt.Errorf("SQL request appeared during apply")
	}
	for i := start; i < len(m.pendingSQLReceipts); i++ {
		pending := &m.pendingSQLReceipts[i]
		if originalsPolicy(m.materializationPolicy) {
			if err := insertRequestOriginal(ctx, tx, types.MutationSQL, pending.requestID, pending.record.receipt.Slot, pending.batchIndex, pending.executionItem); err != nil {
				return err
			}
		}
		m.pendingSQLReceipts[i].record.sqlResult = types.SQLCommandResult{}
		m.pendingSQLReceipts[i].persisted = true
	}
	m.pendingSQLResultBytes = 0
	return nil
}

type countingWriter int

func (w *countingWriter) Write(p []byte) (int, error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

func encodedJSONSize(value any) (int, error) {
	var size countingWriter
	if err := json.NewEncoder(&size).Encode(value); err != nil {
		return 0, err
	}
	return int(size) - 1, nil
}

func collectRows(rows *sql.Rows, limit int) (types.SQLStatementResult, error) {
	return collectRowsWithBudget(rows, &resultBudget{rows: limit, bytes: MaxResultBytes - maxSQLStatementCharge, limit: MaxResultBytes}, nil)
}

func collectRowsWithBudget(rows *sql.Rows, budget *resultBudget, expectedRows *int64) (types.SQLStatementResult, error) {
	columns, err := rows.Columns()
	if err != nil {
		return types.SQLStatementResult{}, err
	}
	if len(columns) > MaxSQLResultColumns {
		return types.SQLStatementResult{}, fmt.Errorf("invalid column count")
	}
	result := types.SQLStatementResult{Columns: columns}
	for _, column := range columns {
		budget.bytes -= maxSQLColumnCharge + len(column)
	}
	if budget.bytes < 0 {
		return result, fmt.Errorf("result exceeds %d bytes", budget.limit)
	}
	for rows.Next() {
		if expectedRows != nil && int64(len(result.Rows)) == *expectedRows {
			return result, fmt.Errorf("%w: statement returned more than %d rows", errSQLPreconditionFailed, *expectedRows)
		}
		if budget.rows == 0 {
			return result, fmt.Errorf("result exceeds %d rows", MaxReturningRows)
		}
		rowCharge := maxSQLRowCharge + len(columns)*maxSQLCellCharge
		if rowCharge > budget.bytes {
			return result, fmt.Errorf("result exceeds %d bytes of working memory", budget.limit)
		}
		budget.bytes -= rowCharge
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		for _, value := range values {
			size := sqlValueSize(value)
			if size > MaxCellBytes {
				return result, fmt.Errorf("result cell exceeds %d bytes", MaxCellBytes)
			}
			budget.bytes -= size
			if budget.bytes < 0 {
				return result, fmt.Errorf("result exceeds %d bytes", budget.limit)
			}
		}
		result.Rows = append(result.Rows, values)
		budget.rows--
	}
	return result, rows.Err()
}

func sqlValueSize(value any) int {
	switch value := value.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	case nil:
		return 1
	default:
		return 16
	}
}

func (m *Materializer) SQLRequestMatches(ctx context.Context, command types.SQLCommand) (bool, error) {
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return false, err
	}
	matches, _, err := m.requestMatches(ctx, types.MutationSQL, command.RequestID, fingerprint)
	return matches, err
}

// SQLRequestStatus returns the retained receipt and fingerprint match.
func (m *Materializer) SQLRequestStatus(ctx context.Context, command types.SQLCommand) (types.MutationReceipt, bool, bool, error) {
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	return m.SQLRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
}

// SQLRequestResultFingerprint returns the retained receipt and row results for
// an idempotent SQL mutation.
func (m *Materializer) SQLRequestResultFingerprint(ctx context.Context, requestID string, fingerprint [32]byte, wantRows bool) (types.MutationReceipt, types.SQLCommandResult, bool, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.sqlReceiptReadLocked(); err != nil {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, false, err
	}
	record, cached := m.recentSQLReceipts[requestID]
	tip := m.tip
	mightContain := m.sqlReceipts.mightContain(requestID, tip)
	if cached {
		if tip > record.receipt.RetryThroughSlot {
			return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
		}
		if !wantRows {
			return record.receipt, types.SQLCommandResult{}, true, record.fingerprint == fingerprint, nil
		}
	}
	if !mightContain {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	if len(m.readers) == 0 || m.readers[0] == nil {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, false, sql.ErrConnDone
	}
	record, err := scanReceipt(m.readers[0].QueryRowContext(ctx, receiptQuery(), types.MutationSQL, requestID), m.idempotencyWindow)
	if err == sql.ErrNoRows {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	if err != nil {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, false, err
	}
	if tip > record.receipt.RetryThroughSlot {
		return types.MutationReceipt{}, types.SQLCommandResult{}, false, true, nil
	}
	return record.receipt, record.sqlResult, true, record.fingerprint == fingerprint, nil
}

// SQLRequestStatusFingerprint returns the retained receipt for a precomputed fingerprint.
func (m *Materializer) SQLRequestStatusFingerprint(ctx context.Context, requestID string, fingerprint [32]byte) (types.MutationReceipt, bool, bool, error) {
	receipt, _, found, matches, err := m.SQLRequestResultFingerprint(ctx, requestID, fingerprint, false)
	return receipt, found, matches, err
}

// Query executes a read query.
