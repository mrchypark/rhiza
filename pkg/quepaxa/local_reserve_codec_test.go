package quepaxa

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"math"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// This test measures encoded lengths at the actual local codec boundaries. It
// is evidence about codec growth, not admission policy or a production reserve.
func TestLocalReserveCodecLengthEvidence(t *testing.T) {
	ids := []struct {
		name string
		id   NodeID
	}{
		{name: "ordinary", id: "node"},
		{name: "align_1", id: "a"},
		{name: "align_2", id: "ab"},
		{name: "align_3", id: "abc"},
		{name: "align_0", id: "abcd"},
		{name: "align_1_longer", id: "abcde"},
		{name: "align_2_longer", id: "abcdef"},
		{name: "align_3_longer", id: "abcdefg"},
		{name: "align_0_longer", id: "abcdefgh"},
		{name: "actual_escaped", id: "local\"id\n\\"},
		{name: "unicode", id: "노드🦊"},
		{name: "nul", id: "a\x00b"},
		{name: "control", id: "a\x01\tb\n"},
		{name: "html_escaped", id: "</script>&"},
		{name: "invalid_utf8", id: NodeID(string([]byte{0xff, 0xfe, 'x'}))},
	}
	var allReceiptMax int
	for _, testID := range ids {
		nodeID := testID.id
		t.Run(testID.name, func(t *testing.T) {
			var ceilingPriority Priority
			var ceilingHash ValueHash
			for i := range ceilingPriority {
				ceilingPriority[i] = 0xff
				ceilingHash[i] = 0xff
			}
			lowerPriority := ceilingPriority
			lowerPriority[len(lowerPriority)-1]--
			first := &Proposal{Priority: ceilingPriority, ProposerID: nodeID, Hash: ceilingHash}
			second := &Proposal{Priority: lowerPriority, ProposerID: nodeID, Hash: ceilingHash}

			states := []struct {
				name  string
				state ISR
			}{
				{name: "step0_nil", state: ISR{}},
				{name: "step4_alias_current", state: ISR{Step: 4, FirstCurrent: second, AggregateCurrent: second}},
				{name: "step4_distinct_current", state: ISR{Step: 4, FirstCurrent: second, AggregateCurrent: first}},
				{name: "step5_distinct_prior", state: ISR{Step: 5, FirstCurrent: second, AggregateCurrent: second, AggregatePrior: first}},
				{name: "step5_prior_alias", state: ISR{Step: 5, FirstCurrent: second, AggregateCurrent: second, AggregatePrior: second}},
				{name: "step6_all_alias", state: ISR{Step: 6, FirstCurrent: first, AggregateCurrent: first, AggregatePrior: first}},
			}
			var receiptMax int
			var largestReceiptShape string
			for _, item := range states {
				payload := encodeRecorderEntry(Slot(math.MaxUint64), item.state, false)
				encoded := (qlog.Entry{Slot: math.MaxUint64, Type: qlog.EntryReceipt, Payload: payload}).Encode()
				t.Logf("receipt id=%q raw_id_bytes=%d state=%s payload_bytes=%d entry_bytes=%d", nodeID, len(nodeID), item.name, len(payload), len(encoded))
				if len(encoded) > receiptMax {
					receiptMax = len(encoded)
					largestReceiptShape = item.name
				}
			}
			if largestReceiptShape != "step5_distinct_prior" {
				t.Fatalf("measured maximum receipt shape=%q, want step5_distinct_prior", largestReceiptShape)
			}
			allReceiptMax = max(allReceiptMax, receiptMax)

			// A real hash-matching value distinguishes reachable certificate
			// content from the structural all-0xff codec ceiling below.
			value := bytes.Repeat([]byte{0xff}, MaxReplicatedValueBytes)
			matching := Proposal{Priority: highestPriority, ProposerID: nodeID, Hash: sha256.Sum256(value)}
			validShape := Decision{
				ConfigID: ^uint(0), Slot: Slot(math.MaxUint64), Step: 6,
				Proposal: matching,
				Summaries: []Summary{{
					RecorderID: nodeID, Step: 6,
					FirstCurrent: &matching, AggregatePrior: &matching,
				}},
			}
			validCert, err := encodeCertificate(^uint(0), validShape)
			if err != nil {
				t.Fatal(err)
			}
			step4Cert, err := encodeCertificate(^uint(0), Decision{
				ConfigID: ^uint(0), Slot: Slot(math.MaxUint64), Step: 4,
				Proposal:  matching,
				Summaries: []Summary{{RecorderID: nodeID, Step: 4, FirstCurrent: &matching}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(step4Cert) > len(validCert) {
				t.Fatalf("hash-matching certificate lengths step4=%d exceeds step6=%d", len(step4Cert), len(validCert))
			}
			validRecord, err := encodeDecisionRecord(value, validCert)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("decision id=%q value_bytes=%d value_mod3=%d hash_matching_step4_cert_bytes=%d hash_matching_step6_cert_bytes=%d decision_record_bytes=%d", nodeID, len(value), len(value)%3, len(step4Cert), len(validCert), len(validRecord))

			// Maximum decimal JSON arrays are intentionally synthetic: their
			// all-0xff hash does not match value. This measures encoder output,
			// not a cryptographically valid decision certificate.
			ceilingDecision := Decision{
				ConfigID: ^uint(0), Slot: Slot(math.MaxUint64), Step: 6,
				Proposal:  *first,
				Summaries: []Summary{{RecorderID: nodeID, Step: 6, FirstCurrent: first, AggregatePrior: first}},
			}
			ceilingCert, err := encodeCertificate(^uint(0), ceilingDecision)
			if err != nil {
				t.Fatal(err)
			}
			maxRef := proposalRef{Priority: ceilingPriority, ProposerID: nodeID, Hash: ceilingHash}
			maxSummary := summaryRef{RecorderID: nodeID, Step: 6, FirstCurrent: &maxRef, AggregatePrior: &maxRef}
			boundCertificate, err := json.Marshal(certificate{
				ConfigID: ^uint(0), Slot: Slot(math.MaxUint64), Step: 6,
				Proposal: maxRef, Summaries: []summaryRef{maxSummary},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(ceilingCert) != len(boundCertificate) {
				t.Fatalf("structural certificate bytes=%d, per-ID max-width canonical bound=%d", len(ceilingCert), len(boundCertificate))
			}
			if len(validCert) > len(boundCertificate) || len(step4Cert) > len(boundCertificate) {
				t.Fatalf("hash-matching certificate lengths step4=%d step6=%d exceed per-ID structural bound=%d", len(step4Cert), len(validCert), len(boundCertificate))
			}
			t.Logf("structural_codec_ceiling_certificate id=%q bytes=%d hash_is_value_matching=false", nodeID, len(ceilingCert))
			jsonID, err := json.Marshal(string(nodeID))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("per_instance_bound id=%q raw_id_bytes=%d json_quoted_id_bytes=%d receipt_entry_max_bytes=%d certificate_bytes=%d", nodeID, len(nodeID), len(jsonID), receiptMax, len(boundCertificate))
			ceilingRecord, err := encodeDecisionRecord(value, ceilingCert)
			if err != nil {
				t.Fatal(err)
			}
			proposalEntry := (qlog.Entry{Slot: math.MaxUint64, Hash: matching.Hash, Type: qlog.EntryProposal, Payload: value}).Encode()
			decisionPayload := append(append([]byte(nil), decisionEntryMagic...), ceilingRecord...)
			decisionEntry := (qlog.Entry{Slot: math.MaxUint64, Hash: matching.Hash, Type: qlog.EntryDecide, Payload: decisionPayload}).Encode()
			codecBound := len(proposalEntry) + 3*receiptMax + len(decisionEntry)
			t.Logf("sampled_id_structural_ceiling id=%q proposal_entry_bytes=%d receipt_entry_max_bytes=%d receipt_attempts=3 decision_entry_bytes=%d combined_bytes=%d", nodeID, len(proposalEntry), receiptMax, len(decisionEntry), codecBound)
			for _, remainder := range []int{0, 1, 2} {
				base64Value := bytes.Repeat([]byte{0xff}, 3+remainder)
				record, err := encodeDecisionRecord(base64Value, validCert)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("decision_base64_remainder id=%q remainder=%d value_bytes=%d record_bytes=%d", nodeID, remainder, len(base64Value), len(record))
			}

			// These are actual schedule/seal encodings at maximal numeric
			// fields, but only representative list sizes. The codecs accept
			// arbitrary-length lists, so these observations do not establish a
			// global schedule/seal ceiling.
			schedule, err := EncodeLeaderSchedule([]NodeID{nodeID, nodeID})
			if err != nil {
				t.Fatal(err)
			}
			seal := CheckpointSeal{
				ConfigID: ^uint(0), Index: Slot(math.MaxUint64),
				RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3},
				NextLeaderOrder: []NodeID{nodeID}, FollowingLeaderOrder: []NodeID{nodeID},
			}
			encodedSeal, err := EncodeCheckpointSeal(seal)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("schedule id=%q members=2 bytes=%d; seal id=%q members=1+1 bytes=%d", nodeID, len(schedule), nodeID, len(encodedSeal))
			if len(encodedSeal) > MaxReplicatedValueBytes {
				t.Fatalf("checkpoint seal encoded to %d bytes, exceeds value cap %d", len(encodedSeal), MaxReplicatedValueBytes)
			}
		})
	}
	t.Logf("observed receipt entry maximum over sampled IDs=%d bytes", allReceiptMax)
}
