package quepaxa_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExternalModuleCanUsePublicAPI(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	dir := t.TempDir()
	goMod := "module example.com/quepaxa-consumer\n\ngo 1.27.0\n\nrequire github.com/mrchypark/rhiza v0.0.0\nreplace github.com/mrchypark/rhiza => " + repo + "\n"
	consumer := `package consumer

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

type transport struct{}

func (transport) SendRecord(_ context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	proposal := request.Proposal
	proposal.Value = append([]byte(nil), proposal.Value...)
	return quepaxa.Summary{RecorderID: to, Step: request.Step, FirstCurrent: &proposal}, nil
}

func (transport) SendDecision(context.Context, quepaxa.Decision) error { return nil }
func (transport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) { return 0, nil }
func (transport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error { return nil }
func (transport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) { return nil, nil }

var _ quepaxa.Transport = transport{}

func TestPublicAPI(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil { t.Fatal(err) }
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}},
		WAL: wal,
		Transport: transport{},
	})
	if err != nil { t.Fatal(err) }
	if slot, receipts, err := core.Propose(context.Background(), []byte("external")); err != nil || slot != 1 || len(receipts) != 2 {
		t.Fatalf("slot=%d receipts=%d err=%v", slot, len(receipts), err)
	}
}
`
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "consumer_test.go"), []byte(consumer), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=auto")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("external module: %v\n%s", err, output)
	}
}
