package main

import (
	"strings"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestGCSAsyncEmptyCadenceRequiresLongConfiguredPublicationIntervals(t *testing.T) {
	if err := validateGCSAsyncEmptyCadence("24h", "24h", "536870912"); err != nil {
		t.Fatalf("valid bounded async-empty cadence rejected: %v", err)
	}
	for _, settings := range [][3]string{
		{"5s", "24h", "536870912"},
		{"24h", "10s", "536870912"},
		{"24h", "24h", "0"},
		{"24h", "24h", "2147483649"},
	} {
		if err := validateGCSAsyncEmptyCadence(settings[0], settings[1], settings[2]); err == nil {
			t.Fatalf("accepted unsafe async-empty cadence %q", settings)
		}
	}
}

func TestGCSAsyncEmptyRequiresPredeclaredAutomaticLossPolicy(t *testing.T) {
	if !gcsAsyncEmptyPolicyEnabled(obj{"automatic": true, "policy": obj{"allowDataLoss": true}}) {
		t.Fatal("rejected persisted automatic allowDataLoss policy")
	}
	for _, spec := range []obj{
		{"automatic": false, "policy": obj{"allowDataLoss": true}},
		{"automatic": true, "policy": obj{"allowDataLoss": false}},
		{"automatic": true},
	} {
		if gcsAsyncEmptyPolicyEnabled(spec) {
			t.Fatalf("accepted missing or disabled automatic loss policy: %v", spec)
		}
	}
}

func TestGCSAsyncEmptyHeadEvidenceRequiresStableInitialAndSealedVersions(t *testing.T) {
	before := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1001", Tip: 0, HeadSHA256: strings.Repeat("a", 64)}
	if err := validateInitialZeroHead(before); err != nil {
		t.Fatalf("valid initial empty HEAD rejected: %v", err)
	}
	if err := validateUnchangedZeroHead(before, before); err != nil {
		t.Fatalf("unchanged initial source HEAD rejected: %v", err)
	}
	after := before
	after.Version = "1002"
	after.Sealed = true
	after.HeadSHA256 = strings.Repeat("b", 64)
	if err := validateSealedZeroHead(before, after); err != nil {
		t.Fatalf("valid sealed zero-tip HEAD transition rejected: %v", err)
	}
	for _, bad := range []gcsArchiveHeadEvidence{
		{Version: "1001", VersionType: int(objstore.Generation), Tip: 1, HeadSHA256: strings.Repeat("a", 64)},
		{Version: "1001", VersionType: int(objstore.Generation), Tip: 0, Sealed: true, HeadSHA256: strings.Repeat("a", 64)},
		{Version: "1001", VersionType: int(objstore.ETag), Tip: 0, HeadSHA256: strings.Repeat("a", 64)},
	} {
		if err := validateInitialZeroHead(bad); err == nil {
			t.Fatalf("accepted non-genesis or wrong-version-type initial HEAD: %+v", bad)
		}
	}
	for _, bad := range []gcsArchiveHeadEvidence{
		{VersionType: int(objstore.ETag), Version: "1002", Tip: 0, Sealed: true, HeadSHA256: strings.Repeat("b", 64)},
		{VersionType: int(objstore.Generation), Version: "1001", Tip: 0, Sealed: true, HeadSHA256: strings.Repeat("b", 64)},
		{VersionType: int(objstore.Generation), Version: "1002", Tip: 1, Sealed: true, HeadSHA256: strings.Repeat("b", 64)},
		{VersionType: int(objstore.Generation), Version: "1002", Tip: 0, HeadSHA256: strings.Repeat("b", 64)},
		{VersionType: int(objstore.Generation), Version: "1002", Tip: 0, Sealed: true, HeadSHA256: before.HeadSHA256},
	} {
		if err := validateSealedZeroHead(before, bad); err == nil {
			t.Fatalf("accepted invalid sealed HEAD transition: %+v", bad)
		}
	}
	changedBeforeProof := before
	changedBeforeProof.Version = "1002"
	if err := validateUnchangedZeroHead(before, changedBeforeProof); err == nil {
		t.Fatal("accepted source HEAD publication between ACK and the fence proof")
	}
}
