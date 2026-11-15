package materializer

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"

	"github.com/mrchypark/rhiza/internal/applydiag"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

// poisonErrorClass never records error text, paths, SQL, IDs or values.
func poisonErrorClass(err error) (applydiag.ErrorClass, bool, int) {
	if err == nil {
		return applydiag.ErrorNone, false, 0
	}
	var sqlite sqlite3.ExtendedErrorCode
	var file *os.PathError
	codeKnown := errors.As(err, &sqlite)
	code := 0
	if codeKnown {
		code = int(sqlite)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return applydiag.ErrorCanceled, codeKnown, code
	case errors.Is(err, context.DeadlineExceeded):
		return applydiag.ErrorDeadline, codeKnown, code
	case codeKnown:
		return applydiag.ErrorSQLite, true, code
	case errors.Is(err, sql.ErrConnDone):
		return applydiag.ErrorConnectionDone, false, 0
	case errors.Is(err, sql.ErrTxDone):
		return applydiag.ErrorTxDone, false, 0
	case errors.As(err, &file):
		return applydiag.ErrorFilesystem, false, 0
	default:
		return applydiag.ErrorUnknown, false, 0
	}
}

// Called only after m.mu and any Graph/SQLite locks are released. The event is
// emitted once per materializer incarnation, never once per health failure.
func logFirstSQLitePoison(origin applydiag.PoisonOrigin) {
	log.Printf("materializer sqlite poison: error_class=commit-unknown origin_known=%t trigger=%s primary_site=%s failed_slot_known=%t failed_slot=%d tip_known=%t tip_before_reset=%d batch_first=%d batch_last=%d primary_class=%s primary_code_known=%t primary_code=%d cleanup_class=%s cleanup_code_known=%t cleanup_code=%d", origin.Known, origin.Trigger, origin.PrimarySite, origin.FailedSlotKnown, origin.FailedSlot, origin.TipKnown, origin.TipBeforeReset, origin.BatchFirst, origin.BatchLast, origin.PrimaryClass, origin.PrimaryCodeKnown, origin.PrimaryCode, origin.CleanupClass, origin.CleanupCodeKnown, origin.CleanupCode)
}
