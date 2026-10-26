package operator

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateLogicalWriterPodAcceptsRenderedRestoreInitOnly(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/logical-authority/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var statefulSet object
	for {
		var doc object
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if str(doc["kind"]) == "StatefulSet" {
			statefulSet = doc
			break
		}
	}
	if statefulSet == nil {
		t.Fatal("logical authority manifest has no StatefulSet")
	}
	templateMetadata := cloneObject(t, asObject(nested(statefulSet, "spec", "template", "metadata")))
	templateAnnotations := asObject(templateMetadata["annotations"])
	if templateAnnotations == nil {
		templateAnnotations = object{}
		templateMetadata["annotations"] = templateAnnotations
	}
	templateAnnotations[fencingProtocolAnnotation] = "1"
	templateAnnotations[fencingRootAnnotation] = "root-uid"
	base := object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid"}}, "spec": cloneObject(t, asObject(nested(statefulSet, "spec", "template", "spec")))}
	main := asObject(list(nested(base, "spec", "containers"))[0])
	init := asObject(list(nested(base, "spec", "initContainers"))[0])
	pinned := strings.Replace(str(main["image"]), "REPLACE_WITH_64_HEX_DIGEST", strings.Repeat("a", 64), 1)
	main["image"], init["image"] = pinned, pinned
	templateSpec := cloneObject(t, asObject(nested(statefulSet, "spec", "template", "spec")))
	asObject(list(templateSpec["containers"])[0])["image"] = pinned
	asObject(list(templateSpec["initContainers"])[0])["image"] = pinned
	if err := validateLogicalWriterPod(base); err != nil {
		t.Fatalf("rendered trusted restore-init template rejected: %v", err)
	}
	sts := object{"metadata": object{"name": "rhiza", "uid": "sts-uid"}, "spec": object{"template": object{
		"metadata": templateMetadata, "spec": templateSpec}}}
	if err := validateLogicalWriterTemplate(base, sts); err != nil {
		t.Fatalf("Pod matching current trusted template rejected: %v", err)
	}

	for _, mutate := range []struct {
		name string
		fn   func(object)
	}{
		{"untrusted image", func(p object) {
			asObject(list(nested(p, "spec", "initContainers"))[0])["image"] = "attacker/image:latest"
		}},
		{"command", func(p object) {
			asObject(list(nested(p, "spec", "initContainers"))[0])["args"] = []any{"sh", "-c", "true"}
		}},
		{"main command", func(p object) {
			asObject(list(nested(p, "spec", "containers"))[0])["command"] = []any{"sh", "-c", "true"}
		}},
		{"both commands", func(p object) {
			asObject(list(nested(p, "spec", "containers"))[0])["command"] = []any{"sh"}
			asObject(list(nested(p, "spec", "initContainers"))[0])["command"] = []any{"sh"}
		}},
		{"both images", func(p object) {
			image := "attacker/rhiza@sha256:" + strings.Repeat("b", 64)
			asObject(list(nested(p, "spec", "containers"))[0])["image"] = image
			asObject(list(nested(p, "spec", "initContainers"))[0])["image"] = image
		}},
		{"secret reader", func(p object) {
			asObject(list(nested(p, "spec", "initContainers"))[0])["envFrom"] = []any{object{"secretRef": object{"name": "peer-creds"}}}
		}},
		{"wrong staging path", func(p object) {
			asObject(list(nested(p, "spec", "initContainers"))[0])["volumeMounts"] = []any{object{"name": "restore-staging", "mountPath": "/data"}}
		}},
		{"capability add", func(p object) {
			nested(asObject(list(nested(p, "spec", "initContainers"))[0]), "securityContext", "capabilities").(object)["add"] = []any{"SYS_ADMIN"}
		}},
		{"unrelated init", func(p object) {
			asObject(nested(p, "spec"))["initContainers"] = append(list(nested(p, "spec", "initContainers")), object{"name": "extra"})
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			copy := cloneObject(t, base)
			mutate.fn(copy)
			if err := validateLogicalWriterTemplate(copy, sts); err == nil {
				t.Fatal("untrusted restore-init variant was accepted")
			}
		})
	}
	t.Run("pod digest differs from current StatefulSet", func(t *testing.T) {
		copy := cloneObject(t, base)
		asObject(list(nested(copy, "spec", "containers"))[0])["image"] = "ghcr.io/example/rhiza@sha256:" + strings.Repeat("b", 64)
		if err := validateLogicalWriterTemplate(copy, sts); err == nil {
			t.Fatal("Pod with a different pinned digest was accepted")
		}
	})
	t.Run("matched changed command remains untrusted", func(t *testing.T) {
		copy := cloneObject(t, base)
		copySTS := cloneObject(t, sts)
		for _, candidate := range []object{copy, asObject(nested(copySTS, "spec", "template"))} {
			asObject(list(nested(candidate, "spec", "containers"))[0])["command"] = []any{"sh"}
		}
		if err := validateLogicalWriterTemplate(copy, copySTS); err == nil {
			t.Fatal("matched changed Pod/template commands were accepted")
		}
	})
}

func cloneObject(t *testing.T, in object) object {
	t.Helper()
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out object
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
