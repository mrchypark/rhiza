package operator

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func phase1Int32(v int32) *int32 { return &v }
func phase1Int64(v int64) *int64 { return &v }

func phase1Recovery() V1Alpha2Recovery {
	return V1Alpha2Recovery{APIVersion: V1Alpha2APIVersion, Kind: "RhizaRecovery", Spec: V1Alpha2RecoverySpec{Target: V1Alpha2Target{Name: "cluster", UID: "uid"}, ExpectedGeneration: 1}}
}

func phase1Cluster() V1Alpha2Cluster {
	return V1Alpha2Cluster{APIVersion: V1Alpha2APIVersion, Kind: "RhizaCluster", Spec: V1Alpha2ClusterSpec{LogicalID: "cluster", VoterCount: 3, Workload: V1Alpha2Workload{PodTemplate: V1Alpha2PodTemplate{Spec: V1Alpha2PodSpec{Containers: []V1Alpha2Container{{Name: "app", Image: "image"}}}}}}}
}

func TestPhase1RecoveryValidation(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*V1Alpha2Recovery)
		wantErr bool
	}{
		{"valid", func(*V1Alpha2Recovery) {}, false},
		{"version", func(r *V1Alpha2Recovery) { r.APIVersion = "v1alpha1" }, true},
		{"kind", func(r *V1Alpha2Recovery) { r.Kind = "RhizaCluster" }, true},
		{"missing name", func(r *V1Alpha2Recovery) { r.Spec.Target.Name = "" }, true},
		{"missing uid", func(r *V1Alpha2Recovery) { r.Spec.Target.UID = "" }, true},
		{"invalid target dots", func(r *V1Alpha2Recovery) { r.Spec.Target.Name = "a..b" }, true},
		{"uid nbsp", func(r *V1Alpha2Recovery) { r.Spec.Target.UID = "\u00a0" }, true},
		{"uid em space", func(r *V1Alpha2Recovery) { r.Spec.Target.UID = "\u2003" }, true},
		{"uid unicode", func(r *V1Alpha2Recovery) { r.Spec.Target.UID = "\u00a0한\u2003" }, false},
		{"zero backoff", func(r *V1Alpha2Recovery) { r.Spec.BackoffLimit = phase1Int32(0) }, false},
		{"negative backoff", func(r *V1Alpha2Recovery) { r.Spec.BackoffLimit = phase1Int32(-1) }, true},
		{"zero deadline", func(r *V1Alpha2Recovery) { r.Spec.ActiveDeadlineSeconds = phase1Int64(0) }, true},
		{"negative ttl", func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(-1) }, true},
		{"zero ttl", func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(0) }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := phase1Recovery()
			tt.change(&r)
			if err := ValidateV1Alpha2Recovery(r); (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestPhase1RecoveryUpdates(t *testing.T) {
	tests := []struct {
		name      string
		old, next func(*V1Alpha2Recovery)
		wantErr   bool
	}{
		{"old version", func(r *V1Alpha2Recovery) { r.APIVersion = "rhiza.mrchypark.dev/v1alpha1" }, nil, true},
		{"old kind", func(r *V1Alpha2Recovery) { r.Kind = "RhizaCluster" }, nil, true},
		{"target name", nil, func(r *V1Alpha2Recovery) { r.Spec.Target.Name = "other" }, true},
		{"target uid", nil, func(r *V1Alpha2Recovery) { r.Spec.Target.UID = "other" }, true},
		{"generation", nil, func(r *V1Alpha2Recovery) { r.Spec.ExpectedGeneration++ }, true},
		{"default backoff to six", nil, func(r *V1Alpha2Recovery) { r.Spec.BackoffLimit = phase1Int32(6) }, false},
		{"six to default backoff", func(r *V1Alpha2Recovery) { r.Spec.BackoffLimit = phase1Int32(6) }, nil, false},
		{"backoff change", nil, func(r *V1Alpha2Recovery) { r.Spec.BackoffLimit = phase1Int32(0) }, true},
		{"unlimited to finite", nil, func(r *V1Alpha2Recovery) { r.Spec.ActiveDeadlineSeconds = phase1Int64(60) }, true},
		{"finite to unlimited", func(r *V1Alpha2Recovery) { r.Spec.ActiveDeadlineSeconds = phase1Int64(60) }, nil, true},
		{"finite deadline change", func(r *V1Alpha2Recovery) { r.Spec.ActiveDeadlineSeconds = phase1Int64(60) }, func(r *V1Alpha2Recovery) { r.Spec.ActiveDeadlineSeconds = phase1Int64(61) }, true},
		{"ttl add zero", nil, func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(0) }, false},
		{"ttl remove", func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(1) }, nil, false},
		{"ttl change", func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(1) }, func(r *V1Alpha2Recovery) { r.Spec.TTLSecondsAfterFinished = phase1Int64(2) }, false},
		{"metadata and status", nil, func(r *V1Alpha2Recovery) {
			r.Metadata = map[string]json.RawMessage{"labels": json.RawMessage(`{"key":"value"}`)}
			r.Status = json.RawMessage(`{"phase":"Complete"}`)
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old, next := phase1Recovery(), phase1Recovery()
			if tt.old != nil {
				tt.old(&old)
			}
			if tt.next != nil {
				tt.next(&next)
			}
			if err := ValidateV1Alpha2RecoveryUpdate(old, next); (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestPhase1ClusterValidation(t *testing.T) {
	containers := func(count int) func(*V1Alpha2Cluster) {
		return func(c *V1Alpha2Cluster) {
			c.Spec.Workload.PodTemplate.Spec.Containers = make([]V1Alpha2Container, count)
			for i := range c.Spec.Workload.PodTemplate.Spec.Containers {
				c.Spec.Workload.PodTemplate.Spec.Containers[i] = V1Alpha2Container{Name: fmt.Sprintf("app-%d", i), Image: fmt.Sprintf("image-%d", i)}
			}
		}
	}
	tests := []struct {
		name    string
		change  func(*V1Alpha2Cluster)
		wantErr bool
	}{
		{"valid", func(*V1Alpha2Cluster) {}, false}, {"version", func(c *V1Alpha2Cluster) { c.APIVersion = "v1alpha1" }, true}, {"kind", func(c *V1Alpha2Cluster) { c.Kind = "RhizaRecovery" }, true},
		{"two voters", func(c *V1Alpha2Cluster) { c.Spec.VoterCount = 2 }, true}, {"64 voters", func(c *V1Alpha2Cluster) { c.Spec.VoterCount = 64 }, false}, {"65 voters", func(c *V1Alpha2Cluster) { c.Spec.VoterCount = 65 }, true},
		{"64 containers", containers(64), false}, {"65 containers", containers(65), true},
		{"2048 character image", func(c *V1Alpha2Cluster) {
			c.Spec.Workload.PodTemplate.Spec.Containers[0].Image = strings.Repeat("a", 2048)
		}, false},
		{"2049 character image", func(c *V1Alpha2Cluster) {
			c.Spec.Workload.PodTemplate.Spec.Containers[0].Image = strings.Repeat("a", 2049)
		}, true},
		{"off", func(c *V1Alpha2Cluster) { c.Spec.AutomaticRecovery = "OFF" }, false}, {"A", func(c *V1Alpha2Cluster) { c.Spec.AutomaticRecovery = "A" }, false}, {"B", func(c *V1Alpha2Cluster) { c.Spec.AutomaticRecovery = "B" }, false}, {"unknown automatic", func(c *V1Alpha2Cluster) { c.Spec.AutomaticRecovery = "C" }, true},
		{"orphan", func(c *V1Alpha2Cluster) { c.Spec.DeletionPolicy = "Orphan" }, false}, {"delete", func(c *V1Alpha2Cluster) { c.Spec.DeletionPolicy = "Delete" }, false}, {"unknown deletion", func(c *V1Alpha2Cluster) { c.Spec.DeletionPolicy = "Other" }, true},
		{"48 hours", func(c *V1Alpha2Cluster) { c.Spec.ProvisionalRetentionSeconds = phase1Int64(48 * 60 * 60) }, false}, {"negative retention", func(c *V1Alpha2Cluster) { c.Spec.ProvisionalRetentionSeconds = phase1Int64(-1) }, true},
		{"retention max", func(c *V1Alpha2Cluster) { c.Spec.ProvisionalRetentionSeconds = phase1Int64(9223372036) }, false},
		{"retention over max", func(c *V1Alpha2Cluster) { c.Spec.ProvisionalRetentionSeconds = phase1Int64(9223372037) }, true},
		{"logical id nbsp", func(c *V1Alpha2Cluster) { c.Spec.LogicalID = "\u00a0" }, true},
		{"logical id em space", func(c *V1Alpha2Cluster) { c.Spec.LogicalID = "\u2003" }, true},
		{"logical id unicode", func(c *V1Alpha2Cluster) { c.Spec.LogicalID = "\u00a0한\u2003" }, false},
		{"image nbsp", func(c *V1Alpha2Cluster) { c.Spec.Workload.PodTemplate.Spec.Containers[0].Image = "\u00a0" }, true},
		{"image em space", func(c *V1Alpha2Cluster) { c.Spec.Workload.PodTemplate.Spec.Containers[0].Image = "\u2003" }, true},
		{"image unicode", func(c *V1Alpha2Cluster) { c.Spec.Workload.PodTemplate.Spec.Containers[0].Image = "\u00a0한\u2003" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := phase1Cluster()
			tt.change(&c)
			if err := ValidateV1Alpha2Cluster(c); (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestPhase1UpdatePreservesMetadataIdentity(t *testing.T) {
	for _, field := range []string{"uid", "name", "namespace"} {
		for _, mutation := range []string{"remove", "null", "change"} {
			t.Run(field+"/"+mutation, func(t *testing.T) {
				metadata := func() map[string]json.RawMessage {
					return map[string]json.RawMessage{"uid": json.RawMessage(`"uid"`), "name": json.RawMessage(`"name"`), "namespace": json.RawMessage(`"namespace"`)}
				}
				change := func(m map[string]json.RawMessage) {
					switch mutation {
					case "remove":
						delete(m, field)
					case "null":
						m[field] = json.RawMessage(`null`)
					case "change":
						m[field] = json.RawMessage(`"different"`)
					}
				}
				oldCluster, nextCluster := phase1Cluster(), phase1Cluster()
				oldCluster.Metadata, nextCluster.Metadata = metadata(), metadata()
				change(nextCluster.Metadata)
				if err := ValidateV1Alpha2ClusterUpdate(oldCluster, nextCluster); err == nil {
					t.Error("cluster accepted changed identity")
				}
				oldRecovery, nextRecovery := phase1Recovery(), phase1Recovery()
				oldRecovery.Metadata, nextRecovery.Metadata = metadata(), metadata()
				change(nextRecovery.Metadata)
				if err := ValidateV1Alpha2RecoveryUpdate(oldRecovery, nextRecovery); err == nil {
					t.Error("recovery accepted changed identity")
				}
			})
		}
	}
}

func TestPhase1ClusterUpdateRejectsOldProvenance(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*V1Alpha2Cluster)
	}{
		{"old version", func(c *V1Alpha2Cluster) { c.APIVersion = "rhiza.mrchypark.dev/v1alpha1" }},
		{"old kind", func(c *V1Alpha2Cluster) { c.Kind = "RhizaRecovery" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			old, next := phase1Cluster(), phase1Cluster()
			tt.change(&old)
			if err := ValidateV1Alpha2ClusterUpdate(old, next); err == nil {
				t.Fatal("accepted old identity")
			}
		})
	}
}

func TestPhase1StrictDecode(t *testing.T) {
	valid := `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"r","namespace":"ns","uid":"r-uid"},"spec":{"target":{"name":"cluster","uid":"cluster-uid"},"expectedGeneration":1}}`
	if _, err := DecodeV1Alpha2Recovery([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","spec":{"target":{"name":"cluster","uid":"uid","namespace":"other"},"expectedGeneration":1}}`,
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","spec":{"target":{"name":"cluster","uid":"uid"},"expectedGeneration":1,"statefulSet":"legacy"}}`,
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","spec":{"target":{"name":"cluster","uid":"uid"}}}`,
	} {
		if _, err := DecodeV1Alpha2Recovery([]byte(input)); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
	for _, input := range []string{
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","spec":{"logicalID":"c","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}],"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"data"}}]}}}}}`,
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","spec":{"logicalID":"c","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image","env":[{"name":"X","value":"x"}]}]}}}}}`,
		`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","spec":{"logicalID":"c","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}],"initContainers":[{"name":"init","image":"image"}]}}}}}`,
	} {
		if _, err := DecodeV1Alpha2Cluster([]byte(input)); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
}
