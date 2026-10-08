package rhiza

import (
	"encoding/json"
	"testing"

	"github.com/mrchypark/rhiza/pkg/node"
)

func TestArchiveStatsPublicJSONShapeWithoutArchive(t *testing.T) {
	got, err := json.Marshal((&DB{node: &node.Node{}}).ArchiveStats())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"stages":{"publication_admission":{"available":false,"count":null,"duration_ns_sum":null},"archive_load":{"available":false,"count":null,"duration_ns_sum":null},"extent_build_upload":{"available":false,"count":null,"duration_ns_sum":null},"head_publish":{"available":false,"count":null,"duration_ns_sum":null},"publication_release":{"available":false,"count":null,"duration_ns_sum":null}}}`
	if string(got) != want {
		t.Fatalf("ArchiveStats JSON=%s want=%s", got, want)
	}
}
