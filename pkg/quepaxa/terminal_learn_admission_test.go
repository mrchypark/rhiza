package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestInstalledAbortLearnRemainsExactAcrossWALRestart(t *testing.T) {
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
	bIdentity := sha256.Sum256([]byte("terminal-learn-b-wal"))
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b", WALIdentity: hex.EncodeToString(bIdentity[:])}}}
	dir := t.TempDir()
	walPath := filepath.Join(dir, "a.qlog")
	wal, err := qlog.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	core, err := New(Config{NodeID: "a", Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	freeze, err := core.BeginReconfiguration(context.Background(), target)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	pendingKey, ok := core.StartupTerminalGapSnapshot()
	if !ok {
		_ = wal.Close()
		t.Fatal("fresh abortable transition has no pending terminal key")
	}
	freezeValue, ok := core.CertifiedValue(freeze)
	if !ok {
		_ = wal.Close()
		t.Fatal("abortable transition has no certified freeze")
	}
	freezeConfig, freezeDecision, err := decodeCertificate(freezeValue.Certificate)
	if err != nil {
		_ = wal.Close()
		t.Fatalf("decode freeze certificate: %v", err)
	}
	freezeDecision.ConfigID = freezeConfig
	freezeDecision.Proposal.Value = append([]byte(nil), freezeValue.Value...)
	if got, ok := core.PendingTerminalGapForLearn(freezeDecision); !ok || got != pendingKey {
		_ = wal.Close()
		t.Fatalf("pending Learn did not capture exact transition key: got=%+v ok=%t want=%+v", got, ok, pendingKey)
	}
	wrongPendingKey := pendingKey
	wrongPendingKey.FreezeHash[0] ^= 0xff
	if core.PendingTerminalGap(wrongPendingKey) {
		_ = wal.Close()
		t.Fatal("pending transition revalidation accepted a different freeze identity")
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	terminalValue, ok := core.CertifiedValue(freeze + leaderEpochSize)
	if !ok {
		_ = wal.Close()
		t.Fatal("aborted reconfiguration has no certified terminal")
	}
	configID, decision, err := decodeCertificate(terminalValue.Certificate)
	if err != nil {
		_ = wal.Close()
		t.Fatalf("decode abort terminal certificate: %v", err)
	}
	decision.ConfigID = configID
	decision.Proposal.Value = append([]byte(nil), terminalValue.Value...)
	if core.PendingTerminalGap(pendingKey) {
		_ = wal.Close()
		t.Fatal("captured pending key remained valid after abort terminal installed")
	}
	if !core.AllowsInstalledTerminalLearn(decision) {
		_ = wal.Close()
		t.Fatal("exact installed abort terminal was not eligible for retry")
	}
	wrongSlot := decision
	wrongSlot.Slot++
	if core.AllowsInstalledTerminalLearn(wrongSlot) {
		_ = wal.Close()
		t.Fatal("installed terminal retry accepted a different slot")
	}
	wrongCertificate := decision
	wrongCertificate.Summaries = append([]Summary(nil), decision.Summaries...)
	wrongCertificate.Summaries[0].RecorderID = "not-the-recorder"
	if core.AllowsInstalledTerminalLearn(wrongCertificate) {
		_ = wal.Close()
		t.Fatal("installed terminal retry accepted a changed certificate")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	restartedWAL, err := qlog.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedWAL.Close()
	restarted, err := New(Config{NodeID: "a", Cluster: initial, WAL: restartedWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	recovered, ok := restarted.CertifiedValue(decision.Slot)
	if !ok || recovered.Hash != terminalValue.Hash || recovered.Value == nil {
		t.Fatalf("restart did not retain exact abort terminal: present=%t hash_match=%t", ok, ok && recovered.Hash == terminalValue.Hash)
	}
	if !restarted.AllowsInstalledTerminalLearn(decision) {
		t.Fatal("exact abort terminal retry was not allowed after WAL recovery")
	}
	if restarted.AllowsInstalledTerminalLearn(Decision{ConfigID: decision.ConfigID, Slot: decision.Slot, Proposal: Proposal{Hash: decision.Proposal.Hash, Value: append([]byte(nil), decision.Proposal.Value...)}}) {
		t.Fatal("WAL-recovered terminal accepted a certificate-less retry")
	}
	recoveredConfigID, recoveredDecision, err := decodeCertificate(recovered.Certificate)
	if err != nil || recoveredConfigID != configID || recoveredDecision.Slot != decision.Slot || recoveredDecision.Proposal.Hash != decision.Proposal.Hash {
		t.Fatalf("recovered certificate differs: config=%d decision=%+v err=%v want config=%d slot=%d hash=%x", recoveredConfigID, recoveredDecision, err, configID, decision.Slot, decision.Proposal.Hash)
	}
}
