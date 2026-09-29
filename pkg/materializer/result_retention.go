package materializer

const (
	maxRetainedSQLResultBytes     = 256 << 20
	maxSQLReceiptEnvelopeBytes    = 256
	maxSQLCommandsPerDecidedValue = 64
)

type sqlReceiptRetentionUsage struct {
	resultBytes uint64
	receipts    uint64
}

func (u sqlReceiptRetentionUsage) admits(resultBytes, window uint64) bool {
	return u.resultBytes <= maxRetainedSQLResultBytes &&
		resultBytes <= maxRetainedSQLResultBytes-u.resultBytes &&
		u.receipts < uint64(maxSQLCommandsPerDecidedValue)*window
}

func (u *sqlReceiptRetentionUsage) add(resultBytes uint64) {
	u.resultBytes += resultBytes
	u.receipts++
}

func (u sqlReceiptRetentionUsage) chargedBytes() uint64 {
	return u.resultBytes + u.receipts*maxSQLReceiptEnvelopeBytes
}
