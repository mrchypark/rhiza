//go:build rhiza_local_testhooks && rhiza_receipt_quorum_fixture

package materializer

import (
	"context"
	"fmt"

	sqlite3 "github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

// TestInstallReceiptGate installs a fixture-owned gate on this writer only.
func (m *Materializer) TestInstallReceiptGate(ctx context.Context, entered chan<- context.Context, release <-chan struct{}) error {
	conn, err := m.writer.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Raw(func(raw any) error {
		native := raw.(sqlite3driver.Conn).Raw()
		if err := native.SetAuthorizer(nil); err != nil {
			return err
		}
		if err := native.CreateFunction("receipt_quorum_gate", 0, sqlite3.INNOCUOUS, func(out sqlite3.Context, _ ...sqlite3.Value) {
			select {
			case entered <- native.GetInterrupt():
			default:
			}
			<-release
			out.ResultInt(1)
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `CREATE TRIGGER receipt_quorum_gate_trigger BEFORE INSERT ON _rhiza_idempotency BEGIN SELECT receipt_quorum_gate(); SELECT sum(value) FROM (WITH RECURSIVE seq(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM seq WHERE value<100000) SELECT value FROM seq); END`)
	if restore := conn.Raw(func(raw any) error { return setSQLAuthorizer(raw.(sqlite3driver.Conn).Raw(), true, false, false) }); restore != nil {
		return fmt.Errorf("restore authorizer: %w", restore)
	}
	return err
}
