//go:build rhiza_stall_diagnostics

package quepaxa

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
)

func TestStallDiagnosticLabelsOmitRequestPayload(t *testing.T) {
	ready, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, finish := stallRecordContext(context.Background(), "n2", RecordRequest{Slot: 41, Step: 7, Proposal: Proposal{Value: []byte("secret-payload-canary")}})
		defer finish(nil)
		close(ready)
		<-release
	}()
	<-ready
	defer close(release)
	var profile bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&profile, 1); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"rhiza_slot", "41", "rhiza_step", "7", "rhiza_peer", "n2"} {
		if !strings.Contains(profile.String(), want) {
			t.Fatalf("missing label %q", want)
		}
	}
	if strings.Contains(profile.String(), "secret-payload-canary") {
		t.Fatal("request payload leaked")
	}
}
