package latencydiag

// TagHeader is unauthenticated diagnostic input. Tagged correlation is valid
// only for one trusted loopback benchmark client; untrusted clients can forge
// in-domain values and duplicate work. The fixed ring can also drop under DoS.
const TagHeader = "X-Rhiza-Latency-Tag"

// SampleTag keeps the tagged diagnostic to one fixed 1/16 request sample.
func SampleTag(tag uint64) bool { return tag != 0 && tag%16 == 0 }
