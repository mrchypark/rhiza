package operator

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

func TestExpectedAnchorBindingUsesTrustedConfigurationAndExactCluster(t *testing.T) {
	config := &recoveryanchor.StorageBindingConfig{Policy: recoveryanchor.StorageBindingDerived, Provider: "s3", Endpoint: "https://objects.example", Bucket: "archive", Prefix: "root"}
	got, err := expectedAnchorBinding(config, "", "target-a")
	if err != nil {
		t.Fatal(err)
	}
	want, err := config.ExpectedBinding("target-a")
	if err != nil || got != want {
		t.Fatalf("operator binding=%+v want=%+v err=%v", got, want, err)
	}
	other, err := expectedAnchorBinding(config, "", "target-b")
	if err != nil || other == got {
		t.Fatalf("target-specific mapping did not differ: target-a=%+v target-b=%+v err=%v", got, other, err)
	}
	if _, err := expectedAnchorBinding(config, "receipt-controlled", "target-a"); err == nil {
		t.Fatal("simultaneous derived and opaque expectations were accepted")
	}
	legacy, err := expectedAnchorBinding(nil, "external-opaque", "target-a")
	if err != nil || legacy != (recoveryanchor.Binding{ClusterID: "target-a", StorageID: "external-opaque"}) {
		t.Fatalf("legacy opaque API result=%+v err=%v", legacy, err)
	}
}
