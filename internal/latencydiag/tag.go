package latencydiag

// TagHeader is unauthenticated diagnostic input. Tagged correlation is valid
// only for one trusted loopback benchmark client; untrusted clients can forge
// in-domain values and duplicate work. The fixed ring can also drop under DoS.
const TagHeader = "X-Rhiza-Latency-Tag"

// Pre-enqueue stages are fixed so diagnostics never retain caller-provided
// stage labels, request IDs, SQL, or error text.
const (
	StagePreEnqueueClone       = "pre_enqueue_clone"
	StagePreEnqueueValidate    = "pre_enqueue_validate"
	StagePreEnqueueFingerprint = "pre_enqueue_fingerprint"
	StagePreEnqueueEncode      = "pre_enqueue_encode"
	StageRequestLockWait       = "request_lock_wait"
	StageRequestLockHold       = "request_lock_hold"
	StageMaterializerReceipt   = "materializer_receipt_pre_enqueue"
	StageBatcherEnqueue        = "batcher_enqueue"
)

// SampleTag keeps the tagged diagnostic to one fixed 1/16 request sample.
func SampleTag(tag uint64) bool { return tag != 0 && tag%16 == 0 }
