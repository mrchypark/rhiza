package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func extractGSADeleteWaitWorkerForTest(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(source, []byte("\ngsa_delete_wait_worker() {\n"))
	if start < 0 {
		t.Fatal("GSA delete worker missing")
	}
	relativeEnd := bytes.Index(source[start+1:], []byte("\n}\n"))
	if relativeEnd < 0 {
		t.Fatal("GSA delete worker end missing")
	}
	return string(source[start+1 : start+1+relativeEnd+2])
}

func runGSADeleteWaitFixtureForTest(t *testing.T, scenario string) (int, []byte, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner := "rhiza-postrelease-fixture-owner"
	data := "rhiza-data@example-project.iam.gserviceaccount.com"
	if err := os.WriteFile(filepath.Join(dir, "data-create.json"), []byte("{\"email\":\""+data+"\",\"uniqueId\":\"fixture-uid\",\"description\":\""+owner+"\"}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.created"), []byte(owner+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gcloud := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"printf '%s\n' \"$*\" >> \"$FAKE_DIR/calls\"",
		"case \"$*\" in",
		" *'iam service-accounts describe'*)",
		"  case \"$FAKE_SCENARIO\" in",
		"   exact-not-found) printf '%s\n' 'ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account.' >&2; exit 1 ;;",
		"   permission-empty-list|permission-present|permission-malformed|permission-list-error) printf '%s\n' 'ERROR: (gcloud.iam.service-accounts.describe) PERMISSION_DENIED: denied on resource (or it may not exist).' >&2; exit 1 ;;",
		"   permission-only) printf '%s\n' 'ERROR: PERMISSION_DENIED' >&2; exit 1 ;;",
		"  esac ;;",
		" *'iam service-accounts list'*)",
		"  case \"$FAKE_SCENARIO\" in",
		"   permission-empty-list) printf '%s\n' '[]' ;;",
		"   permission-present) printf '%s\n' '[{\"email\":\"rhiza-data@example-project.iam.gserviceaccount.com\",\"uniqueId\":\"fixture-uid\"}]' ;;",
		"   permission-malformed) printf '%s\n' '{\"bad\":' ;;",
		"   permission-list-error) printf '%s\n' 'ERROR: HTTPError 500' >&2; exit 1 ;;",
		"   *) exit 97 ;;",
		"  esac ;;",
		" *) exit 97 ;;",
		"esac",
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(gcloud), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	worker := strings.Join([]string{
		"set -eu",
		"die() { printf '%s\n' \"$*\" >&2; exit 1; }",
		"owned() { [ -f \"$RHIZA_AUTH_STATE/$1.created\" ] && [ \"$(cat \"$RHIZA_AUTH_STATE/$1.created\")\" = \"$owner\" ]; }",
		"owner=rhiza-postrelease-fixture-owner",
		"project=fixture-project",
		"ci=unused@example.invalid",
		"data=rhiza-data@example-project.iam.gserviceaccount.com",
		extractGSADeleteWaitWorkerForTest(t),
		"gsa_delete_wait_worker data",
	}, "\n")
	command := exec.Command("/bin/sh", "-c", worker)
	command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario, "RHIZA_AUTH_STATE="+state)
	output, runErr := command.CombinedOutput()
	code := 0
	if runErr != nil {
		var exited *exec.ExitError
		if !errors.As(runErr, &exited) {
			t.Fatal(runErr)
		}
		code = exited.ExitCode()
	}
	return code, output, dir
}

func TestGSADeleteWaitWorkerUsesAuthorizedListAbsence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		wantExit int
		wantList bool
	}{
		{"exact-not-found", 0, false},
		{"permission-empty-list", 0, true},
		{"permission-present", 1, true},
		{"permission-malformed", 1, true},
		{"permission-list-error", 1, true},
		{"permission-only", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output, dir := runGSADeleteWaitFixtureForTest(t, tc.name)
			if code != tc.wantExit {
				t.Fatalf("exit=%d want=%d output=%s", code, tc.wantExit, output)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(calls, []byte(" delete ")) {
				t.Fatalf("worker issued delete operation: %q", calls)
			}
			if !bytes.Contains(calls, []byte("iam service-accounts describe rhiza-data@example-project.iam.gserviceaccount.com --format=json")) {
				t.Fatalf("worker did not issue expected describe: %q", calls)
			}
			hasList := bytes.Contains(calls, []byte("iam service-accounts list --format=json"))
			if hasList != tc.wantList {
				t.Fatalf("list call=%t want=%t calls=%q", hasList, tc.wantList, calls)
			}
			if hasList && bytes.Contains(calls, []byte("--filter=")) {
				t.Fatalf("worker issued filtered list: %q", calls)
			}
			if bytes.Contains(output, []byte("token")) || bytes.Contains(output, []byte("secret")) {
				t.Fatalf("worker leaked credential text: %s", output)
			}
			if tc.wantExit == 0 {
				if exit, err := os.ReadFile(filepath.Join(dir, "data-absence-1.exit")); err != nil || strings.TrimSpace(string(exit)) != "1" {
					t.Fatalf("describe receipt missing: %q err=%v", exit, err)
				}
				if tc.wantList {
					if exit, err := os.ReadFile(filepath.Join(dir, "data-absence-1.list.exit")); err != nil || strings.TrimSpace(string(exit)) != "0" {
						t.Fatalf("list receipt missing: %q err=%v", exit, err)
					}
				}
			} else if !strings.Contains(string(output), "GSA") {
				t.Fatalf("failure lacked worker diagnostic: %s", output)
			}
		})
	}
}

func TestGSADeleteWaitWorkerRejectsOriginalIdentityInList(t *testing.T) {
	code, output, dir := runGSADeleteWaitFixtureForTest(t, "permission-present")
	if code != 1 {
		t.Fatalf("exit=%d want=1 output=%s", code, output)
	}
	list, err := os.ReadFile(filepath.Join(dir, "data-absence-1.list.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(list, []byte("fixture-uid")) {
		t.Fatalf("list receipt did not retain original identity: %s", list)
	}
}
