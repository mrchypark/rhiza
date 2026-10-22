package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentBootstrapRendersDistinctWorkloadAndControlIdentities(t *testing.T) {
	run := "a1b2c3d4"
	readonlyGSA := "rhiza-v0191-readonly-" + run
	cmd := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "render-local")
	cmd.Env = append(cmd.Env, qualificationAssetEnv(run)...)
	cmd.Env = append(cmd.Env,
		"PATH="+os.Getenv("PATH"),
		"RHIZA_GCS_INDEPENDENT_LAYOUT=yes",
		"RHIZA_READONLY_WORKLOAD_GSA_ID="+readonlyGSA,
		"RHIZA_RUN_ID="+run,
		"RHIZA_NAMESPACE=rhiza-qualification-"+run,
		"RHIZA_HARNESS_SHA="+strings.Repeat("a", 40),
		"RHIZA_AUTH_EXPIRES=2099-01-01T00:00:00Z",
		"RHIZA_HOST_IMAGE=registry.example/rhiza@sha256:"+strings.Repeat("a", 64),
		"RHIZA_METADATA_IMAGE=registry.example/cloud-cli@sha256:"+strings.Repeat("b", 64),
		"RHIZA_NODE_A=node-a", "RHIZA_NODE_B=node-b", "RHIZA_NODE_C=node-c",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("independent bootstrap render failed: %v: %s", err, output)
	}
	for _, want := range []string{
		"name: rhiza-workload",
		"iam.gke.io/gcp-service-account: " + readonlyGSA + "@example-project.iam.gserviceaccount.com",
		"name: rhiza-operator",
		"name: rhiza-fencer",
		"iam.gke.io/gcp-service-account: rhiza-v0191-gcs-" + run + "@example-project.iam.gserviceaccount.com",
	} {
		if !bytes.Contains(output, []byte(want)) {
			t.Errorf("independent render missing %q", want)
		}
	}
	if strings.Contains(string(output), "name: rhiza-workload\n  namespace: rhiza-qualification-"+run+"\n  annotations:\n    iam.gke.io/gcp-service-account: rhiza-v0191-gcs-") {
		t.Fatal("workload KSA was rendered with the writer identity")
	}
}

func TestIndependentBootstrapKeepsReaderAtDataChildAndPreservesCleanupReceipts(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"${RHIZA_GCS_INDEPENDENT_LAYOUT:=no}",
		"${RHIZA_READONLY_WORKLOAD_GSA_ID:?fresh read-only workload GSA account ID required}",
		"[ \"${RHIZA_GCS_INDEPENDENT_CONSUMER:-}\" = run-votercount-gcs-chaos/v2 ]",
		"[ \"$RHIZA_READONLY_WORKLOAD_GSA_ID\" != \"$RHIZA_DATA_GSA_ID\" ]",
		"data_prefix=${prefix}data/",
		"control_prefix=${prefix}control/",
		"data=$readonly_email",
		"folder_role=roles/storage.objectViewer",
		"record data-wi-grant g iam service-accounts add-iam-policy-binding \"$readonly_email\" --member=\"$workload_member\"",
		"record operator-wi-grant g iam service-accounts add-iam-policy-binding \"$data\" --member=\"$operator_member\"",
		"record fencer-wi-grant g iam service-accounts add-iam-policy-binding \"$data\" --member=\"$fencer_member\"",
		"record data-folder-grant folder_iam_data add",
		"record control-folder-after folder_iam_control get",
		"check_readonly_inherited_access",
		"wait_deleted_sa readonly",
		"record data-folder-delete g storage managed-folders delete \"$data_folder\"",
		"record control-folder-delete g storage managed-folders delete \"$control_folder\"",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("independent bootstrap contract missing %q", want)
		}
	}
	dataIAM := strings.Index(text, "folder_iam_data() (")
	controlIAM := strings.Index(text, "folder_iam_control() (")
	if dataIAM < 0 || controlIAM < dataIAM {
		t.Fatal("independent managed-folder IAM scopes missing")
	}
	dataScope := text[dataIAM:controlIAM]
	if !strings.Contains(dataScope, "folder=$data_folder") || !strings.Contains(dataScope, "prefix=$data_prefix") || !strings.Contains(dataScope, "data=$readonly_email") {
		t.Fatal("viewer IAM helper does not bind the read-only GSA to the data child only")
	}
	controlEnd := strings.Index(text[controlIAM:], "folder_iam() (")
	if controlEnd < 0 {
		t.Fatal("control-folder IAM helper is not bounded")
	}
	controlScope := text[controlIAM : controlIAM+controlEnd]
	if !strings.Contains(controlScope, "folder=$control_folder") || !strings.Contains(controlScope, "prefix=$control_prefix") || !strings.Contains(controlScope, "data=$readonly_email") {
		t.Fatal("control-folder IAM inspection scope missing")
	}
	if strings.Contains(controlScope, "folder_iam add") {
		t.Fatal("control-folder IAM helper may grant application access")
	}
}

func TestIndependentRollbackReconcilesInterruptedCreateMarkers(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	legacyRunner, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(legacyRunner), ".gcs_layout==\"independent\"") || !strings.Contains(string(legacyRunner), "Legacy workload runner cannot consume independent GCS identity receipts") {
		t.Fatal("legacy runner must reject an independent identity receipt before workload/cloud operations")
	}
	for _, want := range []string{
		"recover_created_gsa_marker data \"$data\"",
		"recover_created_gsa_marker readonly \"$readonly_email\"",
		"recover_created_folder_marker folder \"$folder\" \"$prefix\"",
		"recover_created_folder_marker data-folder \"$data_folder\" \"$data_prefix\"",
		"recover_created_folder_marker control-folder \"$control_folder\" \"$control_prefix\"",
		"receipt_succeeded \"$gsa_receipt\"",
		"type==\"object\" and .email==$email and .uniqueId==$original[0].uniqueId and .description==$owner",
		"check_sa \"$gsa_tag\" \"$gsa_account\"",
		"receipt_succeeded \"$folder_receipt\"",
		"check_folder_create_readback \"$folder_receipt\" \"$RHIZA_AUTH_STATE/$folder_tag-reconcile-current.json\" \"$folder_path\"",
		"GSA ownership marker has no create receipt",
		"Folder ownership marker has no create receipt",
		"GSA create outcome exists without exact identity receipt",
		"Folder create outcome exists without exact identity receipt",
		"Independent identity receipts require the v2 workload launcher/rollback consumer",
		"verify_rollback_side_effect_markers",
		"ci-create:ci cluster-role-create:cluster-role pool-create:pool provider-create:provider ci-project-grant:ci-project-grant ci-wi-grant:ci-wi-grant",
		"Successful side effect lacks an exact ownership marker",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("interrupted-create rollback contract missing %q", want)
		}
	}
	gateAt := strings.Index(text, "verify_rollback_side_effect_markers\n")
	terminalAt := strings.Index(text, `'{status:"complete",run:$run,owner:$owner}'`)
	if gateAt < 0 || terminalAt < 0 || gateAt > terminalAt {
		t.Fatal("rollback may write terminal completion before all side-effect receipts are reconciled")
	}
	cleanupAt := strings.Index(text, "record provider-disable ")
	if cleanupAt < 0 || gateAt > cleanupAt {
		t.Fatal("rollback may begin teardown before reconciling every side-effect receipt")
	}
	lastRecoveryAt := strings.Index(text, "recover_created_folder_marker control-folder \"$control_folder\" \"$control_prefix\"\n")
	firstCleanupMutationAt := strings.Index(text, "record provider-disable g iam workload-identity-pools providers update-oidc")
	if lastRecoveryAt < 0 || firstCleanupMutationAt < 0 || gateAt <= lastRecoveryAt || gateAt >= firstCleanupMutationAt {
		t.Fatal("first rollback receipt gate must run after marker recovery and before the first cleanup mutation")
	}

	for _, recovery := range []struct {
		name  string
		start string
		check string
	}{
		{name: "GSA", start: "recover_created_gsa_marker() {", check: "check_sa \"$gsa_tag\" \"$gsa_account\""},
		{name: "folder", start: "recover_created_folder_marker() {", check: "check_folder_create_readback \"$folder_receipt\" \"$RHIZA_AUTH_STATE/$folder_tag-reconcile-current.json\" \"$folder_path\""},
	} {
		start := strings.Index(text, recovery.start)
		if start < 0 {
			t.Fatalf("%s reconciliation helper not found", recovery.name)
		}
		end := strings.Index(text[start+len(recovery.start):], "\n}")
		if end < 0 {
			t.Fatalf("%s reconciliation helper is not bounded", recovery.name)
		}
		helper := text[start : start+len(recovery.start)+end]
		receipt := strings.Index(helper, "receipt_succeeded ")
		identityCheck := strings.Index(helper, recovery.check)
		marker := strings.LastIndex(helper, "mark \"$")
		if receipt < 0 || identityCheck <= receipt || marker <= identityCheck {
			t.Errorf("%s marker is not restored only after successful receipt and exact live identity check", recovery.name)
		}
	}

	for _, call := range []string{
		"recover_created_gsa_marker data \"$data\"",
		"recover_created_gsa_marker readonly \"$readonly_email\"",
		"recover_created_folder_marker folder \"$folder\" \"$prefix\"",
		"recover_created_folder_marker data-folder \"$data_folder\" \"$data_prefix\"",
		"recover_created_folder_marker control-folder \"$control_folder\" \"$control_prefix\"",
	} {
		callAt := strings.Index(text, call)
		terminalAt := strings.Index(text, "'{status:\"complete\",run:$run,owner:$owner}'")
		if callAt < 0 || terminalAt < 0 || terminalAt < callAt {
			t.Errorf("rollback can reach terminal completion before %q", call)
		}
	}

	for _, mismatch := range []string{"", "data", "readonly", "folder", "data-folder", "control-folder", "missing-data-json-success", "missing-data-json-failed", "missing-readonly-json-success", "missing-readonly-json-failed", "missing-folder-json-success", "missing-folder-json-failed"} {
		t.Run("interrupted-create-"+map[string]string{"": "exact", "data": "writer-gsa-replaced", "readonly": "readonly-gsa-replaced", "folder": "parent-folder-replaced", "data-folder": "data-folder-replaced", "control-folder": "control-folder-replaced", "missing-data-json-success": "writer-gsa-missing-json-success-exit", "missing-data-json-failed": "writer-gsa-missing-json-failed-exit", "missing-readonly-json-success": "readonly-gsa-missing-json-success-exit", "missing-readonly-json-failed": "readonly-gsa-missing-json-failed-exit", "missing-folder-json-success": "missing-folder-json-success-exit", "missing-folder-json-failed": "missing-folder-json-failed-exit"}[mismatch], func(t *testing.T) {
			stateDir := t.TempDir()
			if err := os.Chmod(stateDir, 0o700); err != nil {
				t.Fatal(err)
			}
			fixtureDir := t.TempDir()
			owner := "rhiza-postrelease-a1b2c3d4-" + strings.Repeat("a", 40)
			bucket := "example-bucket"
			prefix := "qualification/a1b2c3d4/"
			dataPrefix, controlPrefix := prefix+"data/", prefix+"control/"
			dataEmail := "rhiza-v0191-data-a1b2c3d4@example-project.iam.gserviceaccount.com"
			readonlyEmail := "rhiza-v0191-readonly-a1b2c3d4@example-project.iam.gserviceaccount.com"
			writeReceipt := func(name, contents string) {
				t.Helper()
				path := filepath.Join(stateDir, name)
				if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			for _, tag := range []string{"data", "readonly"} {
				email := dataEmail
				uid := "data-uid"
				if tag == "readonly" {
					email, uid = readonlyEmail, "readonly-uid"
				}
				if mismatch != "missing-"+tag+"-json-success" && mismatch != "missing-"+tag+"-json-failed" {
					writeReceipt(tag+"-create.json", `{"email":"`+email+`","uniqueId":"`+uid+`","description":"`+owner+`"}`)
				}
				exitCode := "0\n"
				if mismatch == "missing-"+tag+"-json-failed" {
					exitCode = "56\n"
				}
				writeReceipt(tag+"-create.exit", exitCode)
			}
			if !strings.HasPrefix(mismatch, "missing-folder-json-") {
				writeReceipt("folder-create.json", folderCreateReceiptJSON(bucket, prefix))
			}
			folderExit := "0\n"
			if mismatch == "missing-folder-json-failed" {
				folderExit = "56\n"
			}
			writeReceipt("folder-create.exit", folderExit)
			writeReceipt("data-folder-create.json", folderCreateReceiptJSON(bucket, dataPrefix))
			writeReceipt("data-folder-create.exit", "0\n")
			writeReceipt("control-folder-create.json", folderCreateReceiptJSON(bucket, controlPrefix))
			writeReceipt("control-folder-create.exit", "0\n")
			for _, name := range []string{"data-live.json", "readonly-live.json", "folder-live.json", "data-folder-live.json", "control-folder-live.json"} {
				contents := ""
				switch name {
				case "data-live.json":
					contents = `{"email":"` + dataEmail + `","uniqueId":"data-uid","description":"` + owner + `"}`
				case "readonly-live.json":
					contents = `{"email":"` + readonlyEmail + `","uniqueId":"readonly-uid","description":"` + owner + `"}`
				case "folder-live.json":
					contents = folderIdentityJSON(bucket, prefix)
				case "data-folder-live.json":
					contents = folderIdentityJSON(bucket, dataPrefix)
				case "control-folder-live.json":
					contents = folderIdentityJSON(bucket, controlPrefix)
				}
				if mismatch == "data" && name == "data-live.json" {
					contents = strings.Replace(contents, "data-uid", "replacement-uid", 1)
				}
				if mismatch == "readonly" && name == "readonly-live.json" {
					contents = strings.Replace(contents, "readonly-uid", "replacement-uid", 1)
				}
				if mismatch == "folder" && name == "folder-live.json" || mismatch == "data-folder" && name == "data-folder-live.json" || mismatch == "control-folder" && name == "control-folder-live.json" {
					contents = strings.Replace(contents, "fixture-created", "replacement", 1)
				}
				if err := os.WriteFile(filepath.Join(fixtureDir, name), []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			functions, err := bootstrapOwnershipHelpers(text)
			if err != nil {
				t.Fatal(err)
			}
			script := `set -eu
die() { printf '%s\n' "$*" >&2; exit 1; }
project=example-project bucket=example-bucket prefix=qualification/a1b2c3d4/
data_prefix=qualification/a1b2c3d4/data/ control_prefix=qualification/a1b2c3d4/control/
folder=gs://example-bucket/qualification/a1b2c3d4/
data_folder=gs://example-bucket/qualification/a1b2c3d4/data/
control_folder=gs://example-bucket/qualification/a1b2c3d4/control/
data=rhiza-v0191-data-a1b2c3d4@example-project.iam.gserviceaccount.com
readonly_email=rhiza-v0191-readonly-a1b2c3d4@example-project.iam.gserviceaccount.com
owner='` + owner + `'
RHIZA_AUTH_STATE='` + stateDir + `'
RHIZA_TEST_FIXTURE_DIR='` + fixtureDir + `'
gcloud() { case "$*" in
  *'iam service-accounts describe '*data*) cat "$RHIZA_TEST_FIXTURE_DIR/data-live.json" ;;
  *'iam service-accounts describe '*readonly_email*) cat "$RHIZA_TEST_FIXTURE_DIR/readonly-live.json" ;;
  *'managed-folders describe '*'/data/'*) cat "$RHIZA_TEST_FIXTURE_DIR/data-folder-live.json" ;;
  *'managed-folders describe '*'/control/'*) cat "$RHIZA_TEST_FIXTURE_DIR/control-folder-live.json" ;;
	  *'managed-folders describe '*'/qualification/a1b2c3d4/'*) cat "$RHIZA_TEST_FIXTURE_DIR/folder-live.json" ;;
  *) die 'unexpected gcloud in ownership fixture' ;;
esac; }
g() { gcloud --project="$project" --quiet "$@"; }
` + functions + `
recover_created_gsa_marker readonly "$readonly_email"
recover_created_gsa_marker data "$data"
recover_created_folder_marker folder "$folder" "$prefix"
recover_created_folder_marker data-folder "$data_folder" "$data_prefix"
recover_created_folder_marker control-folder "$control_folder" "$control_prefix"
`
			cmd := exec.Command("/bin/sh", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+os.Getenv("PATH"))
			output, runErr := cmd.CombinedOutput()
			if mismatch == "" {
				if runErr != nil {
					t.Fatalf("exact interrupted-create recovery failed: %v: %s", runErr, output)
				}
				for _, marker := range []string{"data", "readonly", "folder", "data-folder", "control-folder"} {
					if got, err := os.ReadFile(filepath.Join(stateDir, marker+".created")); err != nil || string(got) != owner+"\n" {
						t.Errorf("exact create receipt did not restore %s marker: %q, %v", marker, got, err)
					}
				}
				return
			}
			if runErr == nil {
				t.Fatalf("untrusted create state accepted for %s: %s", mismatch, output)
			}
			markerName := mismatch + ".created"
			if strings.HasPrefix(mismatch, "missing-data-json-") {
				markerName = "data.created"
			}
			if strings.HasPrefix(mismatch, "missing-readonly-json-") {
				markerName = "readonly.created"
			}
			if strings.HasPrefix(mismatch, "missing-folder-json-") {
				markerName = "folder.created"
			}
			if _, err := os.Stat(filepath.Join(stateDir, markerName)); !os.IsNotExist(err) {
				t.Fatalf("untrusted %s was marked owned: %v", mismatch, err)
			}
		})
	}
}

func TestRollbackMarkerGateCoversEveryIndependentAndCIWrite(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	functions, err := bootstrapOwnershipHelpers(string(source))
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []string{
		"data-create:data", "folder-role-create:folder-role", "folder-create:folder", "kubernetes-create:kubernetes",
		"fixture-secret-create:fixture-secret",
		"data-wi-grant:data-wi-grant", "operator-wi-grant:operator-wi-grant", "folder-grant:folder-grant",
		"ci-create:ci", "cluster-role-create:cluster-role", "pool-create:pool", "provider-create:provider",
		"ci-project-grant:ci-project-grant", "ci-wi-grant:ci-wi-grant", "readonly-create:readonly",
		"data-folder-create:data-folder", "control-folder-create:control-folder", "fencer-wi-grant:fencer-wi-grant",
		"data-folder-grant:data-folder-grant",
	} {
		if !strings.Contains(string(source), pair) {
			t.Errorf("rollback marker gate omits side-effect mapping %q", pair)
		}
		tag, _, found := strings.Cut(pair, ":")
		if !found {
			t.Fatalf("invalid side-effect test pair %q", pair)
		}
		t.Run(tag+"_successful_receipt_without_marker", func(t *testing.T) {
			stateDir := t.TempDir()
			if err := os.Chmod(stateDir, 0o700); err != nil {
				t.Fatal(err)
			}
			for name, body := range map[string]string{tag + ".json": "{}\n", tag + ".exit": "0\n"} {
				if err := os.WriteFile(filepath.Join(stateDir, name), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			script := `set -eu
die() { printf '%s\\n' "$*" >&2; exit 1; }
auth_mode=ci
RHIZA_GCS_INDEPENDENT_LAYOUT=yes
RHIZA_AUTH_STATE='` + stateDir + `'
owner='fixture-owner'
` + functions + `
verify_rollback_side_effect_markers
`
			cmd := exec.Command("/bin/sh", "-c", script)
			output, runErr := cmd.CombinedOutput()
			if runErr == nil || !strings.Contains(string(output), "Successful side effect lacks an exact ownership marker") {
				t.Fatalf("unmarked success receipt was not rejected for %s: err=%v output=%s", tag, runErr, output)
			}
		})
	}
}

func bootstrapOwnershipHelpers(source string) (string, error) {
	defs := []struct{ name, endToken string }{
		{"check_folder_identity", "check_folder_create_readback() {"},
		{"check_folder_create_readback", "folder_iam_data() ("},
		{"record", "if [ \"$mode\" = apply ]; then"},
		{"mark", "owned() {"},
		{"owned", "if [ \"$mode\" = gsa-delete-wait-worker ]; then"},
		{"check_sa", "receipt_succeeded() {"},
		{"receipt_succeeded", "marker_writable() {"},
		{"marker_writable", "recover_created_gsa_marker() {"},
		{"recover_created_gsa_marker", "recover_created_folder_marker() {"},
		{"recover_created_folder_marker", "verify_rollback_side_effect_markers() {"},
		{"verify_rollback_side_effect_markers", "wait_deleted_sa() {"},
	}
	var out strings.Builder
	for _, def := range defs {
		start := strings.Index(source, def.name+"() {")
		if start < 0 {
			return "", fmt.Errorf("bootstrap helper %s missing", def.name)
		}
		end := strings.Index(source[start+len(def.name)+len("() {"):], "\n"+def.endToken)
		if end < 0 {
			return "", fmt.Errorf("bootstrap helper boundary %s -> %s missing", def.name, def.endToken)
		}
		out.WriteString(source[start : start+len(def.name)+len("() {")+end])
		out.WriteByte('\n')
	}
	return out.String(), nil
}

func folderIdentityJSON(bucket, name string) string {
	return `{"bucket":"` + bucket + `","name":"` + name + `","id":"` + bucket + "/" + name + `","createTime":"fixture-created","updateTime":"fixture-updated","metageneration":"1"}`
}

func folderCreateReceiptJSON(bucket, name string) string {
	return `{"name":"projects/_/buckets/` + bucket + `/managedFolders/` + name + `","createTime":"fixture-created"}`
}
