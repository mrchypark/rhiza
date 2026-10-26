package node

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"gopkg.in/yaml.v3"
)

func TestManifestMountParentSupportsPrepareRetryAndRejectsForeignStage(t *testing.T) {
	stagePath, mountPath := logicalAuthorityRestoreStagePaths(t)
	stageRelative, err := filepath.Rel(filepath.Clean(mountPath), filepath.Clean(stagePath))
	if err != nil || stageRelative == "." || stageRelative == ".." || filepath.IsAbs(stageRelative) ||
		(len(stageRelative) >= 3 && stageRelative[:3] == ".."+string(filepath.Separator)) {
		t.Fatalf("restore target %q must be a child of mount %q: relative=%q err=%v", stagePath, mountPath, stageRelative, err)
	}

	fixture := newHAAdoptionFixture(t)
	configForStage := func(stage string) *types.ExecutionConfig {
		config := fixture.configFor(filepath.Join(t.TempDir(), "main-data"))
		config.ObjStoreProvider = "filesystem"
		config.HAInitRestoreDir = stage
		return config
	}

	volumeRoot := filepath.Join(t.TempDir(), "restore-volume")
	if err := os.Mkdir(volumeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	stageDir := filepath.Join(volumeRoot, stageRelative)
	bucket := &restoreInitWriteTrackingBucket{Bucket: fixture.bucket}
	config := configForStage(stageDir)
	if err := PrepareHAInitRestore(fixture.ctx, config, bucket); err != nil {
		t.Fatalf("prepare with an already-existing emptyDir mount parent: %v", err)
	}
	if err := PrepareHAInitRestore(fixture.ctx, config, bucket); err != nil {
		t.Fatalf("idempotent init retry against the same mount parent: %v", err)
	}
	if _, present, err := readHAStageIndex(stageDir); err != nil || !present {
		t.Fatalf("expected published child stage: present=%t err=%v", present, err)
	}
	if bucket.uploads != 0 || bucket.deletes != 0 {
		t.Fatalf("restore-init mutated remote storage: uploads=%d deletes=%d", bucket.uploads, bucket.deletes)
	}

	foreignVolume := filepath.Join(t.TempDir(), "restore-volume")
	if err := os.Mkdir(foreignVolume, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignStage := filepath.Join(foreignVolume, stageRelative)
	if err := os.MkdirAll(foreignStage, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignFile := filepath.Join(foreignStage, "foreign.txt")
	foreignBytes := []byte("do not overwrite pre-existing content")
	if err := os.WriteFile(foreignFile, foreignBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHAInitRestore(fixture.ctx, configForStage(foreignStage), bucket); err == nil {
		t.Fatal("restore-init accepted foreign contents in its child stage directory")
	}
	got, err := os.ReadFile(foreignFile)
	if err != nil || string(got) != string(foreignBytes) {
		t.Fatalf("restore-init modified foreign stage content: bytes=%q err=%v", got, err)
	}
}

func logicalAuthorityRestoreStagePaths(t *testing.T) (stagePath, mountPath string) {
	t.Helper()
	data, err := os.ReadFile("../../deploy/operator/logical-authority/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	type mount struct {
		Name      string `yaml:"name"`
		MountPath string `yaml:"mountPath"`
		ReadOnly  bool   `yaml:"readOnly"`
	}
	type env struct {
		Name  string `yaml:"name"`
		Value string `yaml:"value"`
	}
	type container struct {
		Name         string  `yaml:"name"`
		Env          []env   `yaml:"env"`
		VolumeMounts []mount `yaml:"volumeMounts"`
	}
	var stage, mountRoot string
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc struct {
			Kind string `yaml:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						InitContainers []container `yaml:"initContainers"`
						Containers     []container `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		if doc.Kind != "StatefulSet" {
			continue
		}
		for _, candidate := range append(doc.Spec.Template.Spec.InitContainers, doc.Spec.Template.Spec.Containers...) {
			if candidate.Name != "restore-init" && candidate.Name != "rhiza" {
				continue
			}
			var configuredStage string
			for _, variable := range candidate.Env {
				if variable.Name == "RHIZA_HA_INIT_RESTORE_DIR" {
					configuredStage = variable.Value
				}
			}
			var configuredMount string
			for _, volume := range candidate.VolumeMounts {
				if volume.Name == "restore-staging" {
					configuredMount = volume.MountPath
					if candidate.Name == "rhiza" && !volume.ReadOnly {
						t.Fatal("main restore staging mount is not read-only")
					}
				}
			}
			if configuredStage == "" || configuredMount == "" {
				t.Fatalf("%s lacks configured restore path or mount", candidate.Name)
			}
			if stage == "" {
				stage, mountRoot = configuredStage, configuredMount
			} else if stage != configuredStage || mountRoot != configuredMount {
				t.Fatalf("init/main restore paths differ: stage=%q/%q mount=%q/%q", stage, configuredStage, mountRoot, configuredMount)
			}
		}
		break
	}
	if stage == "" || mountRoot == "" {
		t.Fatal("logical authority StatefulSet lacks restore stage configuration")
	}
	return stage, mountRoot
}
