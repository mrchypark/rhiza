package operator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const V1Alpha2APIVersion = "rhiza.mrchypark.dev/v1alpha2"

// Status is opaque: no authority or evidence format is selected by this API.
type V1Alpha2Cluster struct {
	APIVersion string                     `json:"apiVersion"`
	Kind       string                     `json:"kind"`
	Metadata   map[string]json.RawMessage `json:"metadata"`
	Spec       V1Alpha2ClusterSpec        `json:"spec"`
	Status     json.RawMessage            `json:"status,omitempty"`
}

type V1Alpha2ClusterSpec struct {
	GenesisIntent               *string          `json:"genesisIntent,omitempty"`
	LogicalID                   string           `json:"logicalID"`
	VoterCount                  int32            `json:"voterCount"`
	Workload                    V1Alpha2Workload `json:"workload"`
	AutomaticRecovery           string           `json:"automaticRecovery,omitempty"`
	ProvisionalRetentionSeconds *int64           `json:"provisionalRetentionSeconds,omitempty"`
	DeletionPolicy              string           `json:"deletionPolicy,omitempty"`
}

// Only this intentionally narrow template subset is supported. Unknown pod
// fields, including volumes, environment and init containers, are rejected.
type V1Alpha2Workload struct {
	PodTemplate V1Alpha2PodTemplate `json:"podTemplate"`
}
type V1Alpha2PodTemplate struct {
	Spec V1Alpha2PodSpec `json:"spec"`
}
type V1Alpha2PodSpec struct {
	Containers []V1Alpha2Container `json:"containers"`
}
type V1Alpha2Container struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type V1Alpha2Recovery struct {
	APIVersion string                     `json:"apiVersion"`
	Kind       string                     `json:"kind"`
	Metadata   map[string]json.RawMessage `json:"metadata"`
	Spec       V1Alpha2RecoverySpec       `json:"spec"`
	Status     json.RawMessage            `json:"status,omitempty"`
}
type V1Alpha2RecoverySpec struct {
	Target                  V1Alpha2Target `json:"target"`
	ExpectedGeneration      int64          `json:"expectedGeneration"`
	BackoffLimit            *int32         `json:"backoffLimit,omitempty"`
	ActiveDeadlineSeconds   *int64         `json:"activeDeadlineSeconds,omitempty"`
	TTLSecondsAfterFinished *int64         `json:"ttlSecondsAfterFinished,omitempty"`
}
type V1Alpha2Target struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

var v1alpha2Name = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Match Go's Unicode whitespace set explicitly so admission can use the same rule.
var v1alpha2NonBlank = regexp.MustCompile(`[^\t\n\v\f\r \x{0085}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]`)

func ValidateV1Alpha2Cluster(c V1Alpha2Cluster) error {
	if c.APIVersion != V1Alpha2APIVersion || c.Kind != "RhizaCluster" {
		return fmt.Errorf("expected v1alpha2 RhizaCluster")
	}
	if !v1alpha2NonBlank.MatchString(c.Spec.LogicalID) || utf8.RuneCountInString(c.Spec.LogicalID) > 256 {
		return fmt.Errorf("logicalID is required")
	}
	if c.Spec.GenesisIntent != nil && *c.Spec.GenesisIntent != "Create" {
		return fmt.Errorf("invalid genesisIntent")
	}
	if c.Spec.VoterCount < 3 || c.Spec.VoterCount > 64 {
		return fmt.Errorf("voterCount must be between 3 and 64")
	}
	if c.Spec.AutomaticRecovery != "" && c.Spec.AutomaticRecovery != "OFF" && c.Spec.AutomaticRecovery != "A" && c.Spec.AutomaticRecovery != "B" {
		return fmt.Errorf("invalid automaticRecovery")
	}
	if c.Spec.DeletionPolicy != "" && c.Spec.DeletionPolicy != "Orphan" && c.Spec.DeletionPolicy != "Delete" {
		return fmt.Errorf("invalid deletionPolicy")
	}
	if v := c.Spec.ProvisionalRetentionSeconds; v != nil && (*v < 0 || *v > math.MaxInt64/int64(time.Second)) {
		return fmt.Errorf("invalid provisionalRetentionSeconds")
	}
	containers := c.Spec.Workload.PodTemplate.Spec.Containers
	// This phase-one template resource bound is not proof of voter membership.
	if len(containers) == 0 || len(containers) > 64 {
		return fmt.Errorf("podTemplate requires between 1 and 64 containers")
	}
	seen := make(map[string]bool, len(containers))
	for _, container := range containers {
		if len(container.Name) > 63 || !v1alpha2Name.MatchString(container.Name) || strings.Contains(container.Name, ".") || !v1alpha2NonBlank.MatchString(container.Image) || utf8.RuneCountInString(container.Image) > 2048 || seen[container.Name] || strings.HasPrefix(container.Name, "rhiza-") {
			return fmt.Errorf("invalid, duplicate, or reserved container name or image")
		}
		seen[container.Name] = true
	}
	return nil
}

func ValidateV1Alpha2Recovery(r V1Alpha2Recovery) error {
	if r.APIVersion != V1Alpha2APIVersion || r.Kind != "RhizaRecovery" {
		return fmt.Errorf("expected v1alpha2 RhizaRecovery")
	}
	if len(r.Spec.Target.Name) > 253 || !v1alpha2Name.MatchString(r.Spec.Target.Name) || !v1alpha2NonBlank.MatchString(r.Spec.Target.UID) {
		return fmt.Errorf("target name and UID are required")
	}
	if r.Spec.ExpectedGeneration < 0 {
		return fmt.Errorf("expectedGeneration must be nonnegative")
	}
	if v := r.Spec.BackoffLimit; v != nil && *v < 0 {
		return fmt.Errorf("backoffLimit must be nonnegative")
	}
	if v := r.Spec.ActiveDeadlineSeconds; v != nil && *v <= 0 {
		return fmt.Errorf("activeDeadlineSeconds must be positive")
	}
	if v := r.Spec.TTLSecondsAfterFinished; v != nil && *v < 0 {
		return fmt.Errorf("ttlSecondsAfterFinished must be nonnegative")
	}
	return nil
}

func ValidateV1Alpha2ClusterUpdate(old, next V1Alpha2Cluster) error {
	if err := ValidateV1Alpha2Cluster(old); err != nil {
		return fmt.Errorf("invalid old cluster: %w", err)
	}
	if err := ValidateV1Alpha2Cluster(next); err != nil {
		return err
	}
	if err := validateV1Alpha2MetadataUpdate(old.Metadata, next.Metadata); err != nil {
		return err
	}
	if !reflect.DeepEqual(old.Spec.GenesisIntent, next.Spec.GenesisIntent) || old.Spec.LogicalID != next.Spec.LogicalID || old.Spec.VoterCount != next.Spec.VoterCount || !reflect.DeepEqual(old.Spec.Workload, next.Spec.Workload) {
		return fmt.Errorf("unsupported cluster identity or workload update")
	}
	return nil
}

func effectiveBackoff(v *int32) int32 {
	if v == nil {
		return 6
	}
	return *v
}
func ValidateV1Alpha2RecoveryUpdate(old, next V1Alpha2Recovery) error {
	if err := ValidateV1Alpha2Recovery(old); err != nil {
		return fmt.Errorf("invalid old recovery: %w", err)
	}
	if err := ValidateV1Alpha2Recovery(next); err != nil {
		return err
	}
	if err := validateV1Alpha2MetadataUpdate(old.Metadata, next.Metadata); err != nil {
		return err
	}
	if old.Spec.Target != next.Spec.Target || old.Spec.ExpectedGeneration != next.Spec.ExpectedGeneration || effectiveBackoff(old.Spec.BackoffLimit) != effectiveBackoff(next.Spec.BackoffLimit) || !reflect.DeepEqual(old.Spec.ActiveDeadlineSeconds, next.Spec.ActiveDeadlineSeconds) {
		return fmt.Errorf("recovery intent and effective budget are immutable")
	}
	return nil
}

// Identical metadata is not proof of live identity or authorization. The
// runtime must independently verify the target against the live object.
func validateV1Alpha2MetadataUpdate(old, next map[string]json.RawMessage) error {
	for _, key := range []string{"name", "namespace", "uid"} {
		oldRaw, oldOK := old[key]
		nextRaw, nextOK := next[key]
		if !oldOK {
			continue
		}
		if !nextOK || bytes.Equal(bytes.TrimSpace(oldRaw), []byte("null")) || bytes.Equal(bytes.TrimSpace(nextRaw), []byte("null")) {
			return fmt.Errorf("metadata.%s must remain present and nonempty", key)
		}
		var oldValue, nextValue string
		if err := json.Unmarshal(oldRaw, &oldValue); err != nil {
			return fmt.Errorf("invalid old metadata.%s: %w", key, err)
		}
		if err := json.Unmarshal(nextRaw, &nextValue); err != nil {
			return fmt.Errorf("invalid new metadata.%s: %w", key, err)
		}
		if !v1alpha2NonBlank.MatchString(oldValue) || !v1alpha2NonBlank.MatchString(nextValue) || oldValue != nextValue {
			return fmt.Errorf("metadata.%s is immutable", key)
		}
	}
	return nil
}

// DecodeV1Alpha2Cluster and DecodeV1Alpha2Recovery reject unsupported wire
// fields. In particular target.namespace cannot silently become same-namespace.
func DecodeV1Alpha2Cluster(data []byte) (V1Alpha2Cluster, error) {
	var c V1Alpha2Cluster
	if err := decodeV1Alpha2(data, &c); err != nil {
		return c, err
	}
	return c, ValidateV1Alpha2Cluster(c)
}
func DecodeV1Alpha2Recovery(data []byte) (V1Alpha2Recovery, error) {
	var r V1Alpha2Recovery
	if err := decodeV1Alpha2(data, &r); err != nil {
		return r, err
	}
	var raw struct {
		Spec map[string]json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return r, err
	}
	if v, ok := raw.Spec["expectedGeneration"]; !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return r, fmt.Errorf("expectedGeneration is required")
	}
	return r, ValidateV1Alpha2Recovery(r)
}
func decodeV1Alpha2(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
