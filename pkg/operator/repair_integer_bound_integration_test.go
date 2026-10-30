package operator

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const integerBoundPlural = "integerbounds"

func integerBoundCRDName(group string) string { return integerBoundPlural + "." + group }

func integerBoundCRD(owner, group string, corrected bool) string {
	validation, maximum := "", ""
	if corrected {
		validation = `,"x-kubernetes-validations":[{"rule":"self <= 9223372036854775805","message":"expectedConfigID bound"}]`
	} else {
		maximum = `,"maximum":9223372036854775805`
	}
	return fmt.Sprintf(`{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"%s","labels":{"rhizatest":"%s"}},"spec":{"group":"%s","scope":"Namespaced","names":{"plural":"integerbounds","singular":"integerbound","kind":"IntegerBound","listKind":"IntegerBoundList"},"versions":[{"name":"v1","served":true,"storage":true,"schema":{"openAPIV3Schema":{"type":"object","properties":{"spec":{"type":"object","properties":{"expectedConfigID":{"type":"integer","format":"int64","minimum":1%s%s}}}}}}}]}}`, integerBoundCRDName(group), owner, group, maximum, validation)
}

func integerBoundDelete(resource, name, namespace, uid string) (string, []string, error) {
	if uid == "" {
		return "", nil, errors.New("cleanup requires exact UID")
	}
	var path string
	if resource == "crd" && namespace == "" {
		plural, group, ok := strings.Cut(name, ".")
		if !ok || plural != integerBoundPlural || !strings.HasSuffix(group, ".example.invalid") {
			return "", nil, errors.New("unexpected test CRD")
		}
		path = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + url.PathEscape(name)
	} else {
		plural, group, ok := strings.Cut(resource, ".")
		if !ok || plural != integerBoundPlural || !strings.HasSuffix(group, ".example.invalid") || !validNamespace(namespace) || !validNamespace(name) {
			return "", nil, errors.New("unexpected test resource")
		}
		path = "/apis/" + url.PathEscape(group) + "/v1/namespaces/" + url.PathEscape(namespace) + "/" + integerBoundPlural + "/" + url.PathEscape(name)
	}
	body, err := json.Marshal(struct {
		APIVersion    string `json:"apiVersion"`
		Kind          string `json:"kind"`
		Preconditions struct {
			UID string `json:"uid"`
		} `json:"preconditions"`
	}{APIVersion: "v1", Kind: "DeleteOptions", Preconditions: struct {
		UID string `json:"uid"`
	}{UID: uid}})
	return string(body), []string{"delete", "--raw", path, "-f", "-"}, err
}

func TestRepairIntegerBoundFixtureBuilders(t *testing.T) {
	for _, suffix := range []string{"u", "f"} {
		group := "ib0123456789ab" + suffix + ".example.invalid"
		name := integerBoundCRDName(group)
		if name != integerBoundPlural+"."+group {
			t.Fatal("CRD name is not plural.group")
		}
		for _, corrected := range []bool{false, true} {
			var crd struct {
				Metadata struct {
					Name   string
					Labels map[string]string
				}
				Spec struct {
					Group string
					Names struct{ Plural string }
				}
			}
			if err := json.Unmarshal([]byte(integerBoundCRD("owned", group, corrected)), &crd); err != nil {
				t.Fatal(err)
			}
			if crd.Metadata.Name != crd.Spec.Names.Plural+"."+crd.Spec.Group || crd.Metadata.Name != name || crd.Metadata.Labels["rhizatest"] != "owned" {
				t.Fatal("actual CRD document is not canonical/test-owned")
			}
		}
		for _, item := range []struct{ resource, name, namespace, path string }{
			{"crd", name, "", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + name},
			{integerBoundPlural + "." + group, "ib0123456789abok0", "contract", "/apis/" + group + "/v1/namespaces/contract/integerbounds/ib0123456789abok0"},
		} {
			body, args, err := integerBoundDelete(item.resource, item.name, item.namespace, "owned-uid")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(args, " ") != "delete --raw "+item.path+" -f -" || strings.Contains(strings.Join(args, " "), "--preconditions") {
				t.Fatalf("unsupported or inexact cleanup command: %v", args)
			}
			var options struct {
				APIVersion, Kind string
				Preconditions    struct{ UID string }
			}
			if json.Unmarshal([]byte(body), &options) != nil || options.APIVersion != "v1" || options.Kind != "DeleteOptions" || options.Preconditions.UID != "owned-uid" {
				t.Fatal("raw DELETE lost UID precondition")
			}
		}
	}
	if _, _, err := integerBoundDelete("crd", "ib0123456789abu", "", "owned-uid"); err == nil {
		t.Fatal("noncanonical CRD cleanup name accepted")
	}
	if _, _, err := integerBoundDelete("crd", "integerbounds.ib0u.example.invalid", "", ""); err == nil {
		t.Fatal("UID-less delete accepted")
	}
}

func TestRepairIntegerBoundAPIServerContract(t *testing.T) {
	for _, key := range []string{"RHIZA_CONTRACT_KUBECONFIG", "RHIZA_CONTRACT_CONTEXT", "RHIZA_CONTRACT_NAMESPACE"} {
		if os.Getenv(key) == "" {
			t.Skip("requires the disposable API contract environment")
		}
	}
	ns := os.Getenv("RHIZA_CONTRACT_NAMESPACE")
	var nonce [6]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	base := "ib" + hex.EncodeToString(nonce[:])
	unsafeGroup, fixedGroup := base+"u.example.invalid", base+"f.example.invalid"
	owner := base
	kubectl := func(stdin string, args ...string) ([]byte, error) {
		t.Helper()
		args = append([]string{"--kubeconfig", os.Getenv("RHIZA_CONTRACT_KUBECONFIG"), "--context", os.Getenv("RHIZA_CONTRACT_CONTEXT"), "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(stdin)
		return cmd.CombinedOutput()
	}
	cleanupOwned := func(resource, name, namespace string) {
		args := []string{"get", resource, name}
		if namespace != "" {
			args = append(args, "-n", namespace)
		}
		args = append(args, "-o", "json")
		out, err := kubectl("", args...)
		if err != nil {
			if strings.Contains(string(out), "NotFound") {
				return
			}
			t.Errorf("inspect test-owned %s %s before cleanup: %v: %s", resource, name, err, out)
			return
		}
		var object struct {
			Metadata struct {
				UID    string            `json:"uid"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(out, &object); err != nil {
			t.Errorf("decode test-owned %s %s before cleanup: %v", resource, name, err)
			return
		}
		if object.Metadata.Labels["rhizatest"] != owner || object.Metadata.UID == "" {
			t.Errorf("refusing to delete %s %s without this test's ownership label and UID", resource, name)
			return
		}
		body, deleteArgs, err := integerBoundDelete(resource, name, namespace, object.Metadata.UID)
		if err != nil {
			t.Errorf("build owned deletion: %v", err)
			return
		}
		if out, err := kubectl(body, deleteArgs...); err != nil && !strings.Contains(string(out), "NotFound") {
			t.Errorf("delete test-owned %s %s: %v: %s", resource, name, err, out)
			return
		}
		args = []string{"wait", "--for=delete", "--timeout=30s", resource + "/" + name}
		if namespace != "" {
			args = append(args, "-n", namespace)
		}
		if out, err := kubectl("", args...); err != nil && !strings.Contains(string(out), "NotFound") {
			t.Errorf("wait for owned deletion %s %s: %v: %s", resource, name, err, out)
			return
		}
		args = []string{"get", resource, name}
		if namespace != "" {
			args = append(args, "-n", namespace)
		}
		out, err = kubectl("", args...)
		if err == nil {
			t.Errorf("test-owned %s %s remains after cleanup", resource, name)
		} else if !strings.Contains(string(out), "NotFound") {
			t.Errorf("could not verify %s %s cleanup: %v: %s", resource, name, err, out)
		}
	}
	applyCRD := func(group string, corrected bool) {
		t.Helper()
		name := integerBoundCRDName(group)
		// Register cleanup before create: a timed-out create may still persist.
		t.Cleanup(func() { cleanupOwned("crd", name, "") })
		crd := integerBoundCRD(owner, group, corrected)
		if out, err := kubectl(crd, "create", "-f", "-"); err != nil {
			t.Fatalf("create owned CRD: %v: %s", err, out)
		}
		if out, err := kubectl("", "wait", "--for=condition=Established", "--timeout=60s", "crd/"+name); err != nil {
			t.Fatalf("wait for owned CRD: %v: %s", err, out)
		}
	}
	object := func(group, name, value string) string {
		return fmt.Sprintf(`{"apiVersion":"%s/v1","kind":"IntegerBound","metadata":{"name":"%s","namespace":"%s","labels":{"rhizatest":"%s"}},"spec":{"expectedConfigID":%s}}`, group, name, ns, owner, value)
	}
	probe := func(group, label, value string, accept bool, expected string) {
		t.Helper()
		name := base + label
		resource := "integerbounds." + group
		// This also removes a resource whose create request persisted despite a
		// lost or timed-out response. The ownership label prevents deleting a
		// same-name object created by another actor.
		t.Cleanup(func() { cleanupOwned(resource, name, ns) })
		out, err := kubectl(object(group, name, value), "create", "-f", "-")
		if accept {
			if err != nil {
				t.Errorf("expected integer %s to be accepted: %v: %s", value, err, out)
				return
			}
			cleanupOwned(resource, name, ns)
			return
		}
		if err == nil || expected == "" || !strings.Contains(string(out), expected) {
			t.Errorf("expected rejection of %s containing %q; err=%v output=%s", value, expected, err, out)
		}
	}
	applyCRD(unsafeGroup, false)
	probe(unsafeGroup, "bad", "1", false, "Maximum boundary")

	applyCRD(fixedGroup, true)
	for i, value := range []string{"1", "9007199254740993", "9223372036854775805"} {
		probe(fixedGroup, fmt.Sprintf("ok%d", i), value, true, "")
	}
	for i, invalid := range []struct{ value, cause string }{
		{"0", "greater than or equal to 1"},
		{"9223372036854775806", "expectedConfigID bound"},
		{"9223372036854775807", "expectedConfigID bound"},
		{`"1"`, "expectedConfigID"},
		{"9223372036854775808", "expectedConfigID"},
	} {
		probe(fixedGroup, fmt.Sprintf("no%d", i), invalid.value, false, invalid.cause)
	}
}
