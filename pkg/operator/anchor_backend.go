package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// KubernetesAnchorBackend stores recovery anchor records in a Kubernetes
// ConfigMap. The ConfigMap must pre-exist; this backend never creates one.
// CAS uses resourceVersion on the mutated ConfigMap itself and refuses to
// operate on a ConfigMap that lacks an existing anchor record.
type KubernetesAnchorBackend struct {
	Kube *Kubernetes
}

const anchorDataKey = "anchor.json"

var anchorNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func anchorConfigMapName(anchorID string) (string, error) {
	if anchorID == "" {
		return "", fmt.Errorf("anchor ID is required")
	}
	name := "rhiza-anchor-" + anchorID
	if !anchorNameRE.MatchString(strings.TrimPrefix(name, "rhiza-anchor-")) || len(name) > 253 {
		return "", fmt.Errorf("anchor ID %q produces invalid ConfigMap name %q", anchorID, name)
	}
	return name, nil
}

func (b *KubernetesAnchorBackend) anchorPath(anchorID string) string {
	name, _ := anchorConfigMapName(anchorID)
	return "/api/v1/namespaces/" + b.Kube.Namespace + "/configmaps/" + name
}

// Read returns the anchor record stored in the ConfigMap and the current
// resourceVersion. Returns an error if the ConfigMap or record is missing.
// Strictly decodes the record, rejecting unknown fields and trailing bytes.
func (b *KubernetesAnchorBackend) Read(ctx context.Context, anchorID string) (recoveryanchor.Record, string, error) {
	if b.Kube == nil {
		return recoveryanchor.Record{}, "", fmt.Errorf("anchor backend: Kubernetes client is required")
	}
	if _, err := anchorConfigMapName(anchorID); err != nil {
		return recoveryanchor.Record{}, "", err
	}
	var cm object
	if err := b.Kube.Get(ctx, b.anchorPath(anchorID), &cm); err != nil {
		return recoveryanchor.Record{}, "", fmt.Errorf("anchor %q: %w", anchorID, err)
	}
	rv := str(nested(cm, "metadata", "resourceVersion"))
	if rv == "" {
		return recoveryanchor.Record{}, "", fmt.Errorf("anchor %q: ConfigMap has no resourceVersion", anchorID)
	}
	raw := str(nested(cm, "data", anchorDataKey))
	if raw == "" {
		return recoveryanchor.Record{}, rv, fmt.Errorf("anchor %q: no anchor record in ConfigMap", anchorID)
	}
	var rec recoveryanchor.Record
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return recoveryanchor.Record{}, rv, fmt.Errorf("anchor %q: corrupt record: %w", anchorID, err)
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return recoveryanchor.Record{}, rv, fmt.Errorf("anchor %q: trailing data after record", anchorID)
	}
	return rec, rv, nil
}

// CAS writes rec to the ConfigMap when its current resourceVersion matches
// version and an existing anchor record is present. Refuses to operate on a
// ConfigMap that lacks anchor.json. Preserves the entire ConfigMap metadata.
// version must not be empty.
func (b *KubernetesAnchorBackend) CAS(ctx context.Context, anchorID string, version string, rec recoveryanchor.Record) error {
	if b.Kube == nil {
		return fmt.Errorf("anchor backend: Kubernetes client is required")
	}
	if version == "" {
		return fmt.Errorf("anchor backend: CAS requires a non-empty resourceVersion")
	}
	if _, err := anchorConfigMapName(anchorID); err != nil {
		return err
	}
	var cm object
	if err := b.Kube.Get(ctx, b.anchorPath(anchorID), &cm); err != nil {
		return fmt.Errorf("anchor %q: read for CAS: %w", anchorID, err)
	}
	liveRV := str(nested(cm, "metadata", "resourceVersion"))
	if liveRV == "" {
		return fmt.Errorf("anchor %q: ConfigMap has no resourceVersion", anchorID)
	}
	if liveRV != version {
		return fmt.Errorf("anchor %q: resourceVersion mismatch: want %s, got %s", anchorID, version, liveRV)
	}
	// Refuse to operate on a ConfigMap that has no existing anchor record.
	if str(nested(cm, "data", anchorDataKey)) == "" {
		return fmt.Errorf("anchor %q: ConfigMap has no existing anchor record", anchorID)
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("anchor %q: marshal record: %w", anchorID, err)
	}
	set := asObject(cm["data"])
	if set == nil {
		set = object{}
	}
	set[anchorDataKey] = string(data)
	cm["data"] = set
	return b.Kube.Put(ctx, b.anchorPath(anchorID), cm, nil)
}
