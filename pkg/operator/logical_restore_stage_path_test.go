package operator

import (
	"bytes"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLogicalAuthorityRestoreStageIsChildOfMountedVolume(t *testing.T) {
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
	podSpec := asObject(nested(statefulSet, "spec", "template", "spec"))
	containers := map[string]object{}
	for _, group := range []string{"initContainers", "containers"} {
		for _, raw := range list(podSpec[group]) {
			container := asObject(raw)
			containers[str(container["name"])] = container
		}
	}
	for _, name := range []string{"restore-init", "rhiza"} {
		container := containers[name]
		if container == nil {
			t.Fatalf("manifest lacks %q container", name)
		}
		stagePath := ""
		for _, raw := range list(container["env"]) {
			env := asObject(raw)
			if str(env["name"]) == "RHIZA_HA_INIT_RESTORE_DIR" {
				stagePath = str(env["value"])
			}
		}
		if stagePath != "/ha-restore-staging/stage" {
			t.Fatalf("%s restore path=%q, want a publish child under the mounted volume", name, stagePath)
		}
		mountFound := false
		for _, raw := range list(container["volumeMounts"]) {
			mount := asObject(raw)
			if str(mount["name"]) != "restore-staging" {
				continue
			}
			mountFound = str(mount["mountPath"]) == "/ha-restore-staging"
			if name == "rhiza" && mount["readOnly"] != true {
				t.Fatal("main container restore staging mount must remain read-only")
			}
		}
		if !mountFound {
			t.Fatalf("%s does not mount restore-staging at /ha-restore-staging", name)
		}
	}
}

func TestLogicalWriterValidatorRequiresReadOnlyMainRestoreStage(t *testing.T) {
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
	pod := object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}},
		"spec": nested(statefulSet, "spec", "template", "spec")}
	for _, group := range []string{"initContainers", "containers"} {
		container := asObject(list(nested(pod, "spec", group))[0])
		container["image"] = "rhiza:test@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	if err := validateLogicalWriterPod(pod); err != nil {
		t.Fatalf("rendered logical writer Pod rejected before mutation: %v", err)
	}
	main := asObject(list(nested(pod, "spec", "containers"))[0])
	for _, raw := range list(main["volumeMounts"]) {
		mount := asObject(raw)
		if str(mount["name"]) == "restore-staging" {
			mount["readOnly"] = false
		}
	}
	if err := validateLogicalWriterPod(pod); err == nil {
		t.Fatal("logical writer validator accepted a writable restore stage mount")
	}
}
