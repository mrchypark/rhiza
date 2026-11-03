package operator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

// This is the application's supported mixed-object profile, not an API limit.
const managedMixedObjectLimit = 1 << 20

const managedMixedFutureReserve = (300 + 64 + 64 + 128) << 10
const managedMixedMetadataCeiling = 128 << 10
const managedMixedProjectedBaseLimit = 448 << 10

// This compares two observations for writes; the retained reservation below
// also binds a later Load to the same authority without needing an old snapshot.
func validateManagedMixedTransition(old, next map[string]json.RawMessage) error {
	protocol, err := mixedApplicationProtocol(old)
	if err != nil || protocol == "" {
		return err
	}
	before, err := jsonObject(old["status"])
	if err != nil {
		return err
	}
	after, err := jsonObject(next["status"])
	if err != nil {
		return err
	}
	a, err := jsonObject(before["authority"])
	if err != nil {
		return err
	}
	b, err := jsonObject(after["authority"])
	if err != nil {
		return err
	}
	if !sameJSON(a["pendingWrite"], json.RawMessage("true")) || !sameJSON(b["pendingWrite"], json.RawMessage("true")) {
		return nil
	}
	for _, key := range []string{"owner", "generation", "binding", "pendingRequest"} {
		if !sameJSON(a[key], b[key]) {
			return fmt.Errorf("mixed pending reservation context cannot change")
		}
	}
	for _, key := range []string{"applicationInit", "bootstrapPlan"} {
		if !sameJSON(before[key], after[key]) {
			return fmt.Errorf("mixed pending initialization context cannot change")
		}
	}
	return nil
}

func validateMixedRepairBudget(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	if err := mixedComponentSize(raw, V1Alpha2RepairRecordLimit); err != nil {
		return err
	}
	record, err := jsonObject(raw)
	if err != nil {
		return err
	}
	var state string
	_ = json.Unmarshal(record["state"], &state)
	if state != "Closed" {
		return nil
	}
	var plan V1Alpha2RepairPlan
	if decodeV1Alpha2(record["plan"], &plan) != nil {
		return fmt.Errorf("mixed repair plan missing")
	}
	// Terminal completion is larger than Abort and retains the same handoff.
	// Preserve every existing field while projecting the complete successor.
	completion := V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: math.MaxInt64, Slot: math.MaxInt64, Hash: fmt.Sprintf("%064x", 0), FinalVoterIDs: plan.FinalVoterIDs}
	record["state"] = json.RawMessage(`"Completed"`)
	record["completion"], _ = json.Marshal(completion)
	return mixedComponentSize(record, V1Alpha2RepairRecordLimit)
}

func mixedApplicationProtocol(obj map[string]json.RawMessage) (string, error) {
	spec, err := jsonObject(obj["spec"])
	if err != nil {
		return "", err
	}
	var protocol string
	if raw, present := spec["applicationProtocol"]; present {
		if json.Unmarshal(raw, &protocol) != nil || protocol == "" {
			return "", fmt.Errorf("unsupported application protocol")
		}
		if _, _, err := types.ManagedApplicationPolicy(protocol); err != nil {
			return "", fmt.Errorf("unsupported application protocol")
		}
	}
	return protocol, nil
}

func managedControlProfile(obj map[string]json.RawMessage) (string, string, error) {
	protocol, err := mixedApplicationProtocol(obj)
	if err != nil {
		return "", "", err
	}
	if protocol == "" {
		return "", V1Alpha2ManagedSQLEvidenceFormat, nil
	}
	status, err := jsonObject(obj["status"])
	if err != nil {
		return "", "", err
	}
	var assignment V1Alpha2ApplicationInitAssignment
	if decodeV1Alpha2(status["applicationInit"], &assignment) != nil || assignment.Plan.ApplicationProtocol != protocol {
		return "", "", fmt.Errorf("application protocol markers disagree")
	}
	if err := ValidateV1Alpha2ApplicationProfile(protocol, assignment.Plan.EvidenceFormat); err != nil {
		return "", "", err
	}
	return protocol, assignment.Plan.EvidenceFormat, nil
}

func managedSQLReservationContextRequired(obj map[string]json.RawMessage) (bool, error) {
	protocol, _, err := managedControlProfile(obj)
	if err != nil {
		return false, err
	}
	switch protocol {
	case types.ManagedApplicationProtocolSQLKV, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol:
		return true, nil
	case "":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported application protocol")
	}
}

func mixedComponentSize(value any, limit int) error {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > limit {
		return fmt.Errorf("mixed application object exceeds supported profile")
	}
	return nil
}

func validateManagedMixedProfile(obj map[string]json.RawMessage) error {
	protocol, evidenceFormat, err := managedControlProfile(obj)
	if err != nil {
		return err
	}
	status, err := jsonObject(obj["status"])
	if err != nil {
		return err
	}
	var assignment V1Alpha2ApplicationInitAssignment
	if raw, exists := status["applicationInit"]; exists {
		if decodeV1Alpha2(raw, &assignment) != nil || assignment.Plan.ApplicationProtocol != protocol || protocol != "" && assignment.Plan.EvidenceFormat != evidenceFormat {
			return fmt.Errorf("application protocol markers disagree")
		}
	} else if protocol != "" {
		return fmt.Errorf("mixed application initialization marker missing")
	}
	if protocol == "" {
		return nil
	}
	if err := mixedComponentSize(obj, managedMixedObjectLimit); err != nil {
		return err
	}
	if err := mixedComponentSize(obj["metadata"], managedMixedMetadataCeiling); err != nil {
		return err
	}
	if err := mixedComponentSize(obj["spec"], 16<<10); err != nil {
		return err
	}
	if err := mixedComponentSize(status["bootstrapPlan"], 96<<10); err != nil {
		return err
	}
	if err := validateMixedRepairBudget(status["repair"]); err != nil {
		return err
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		return err
	}
	var pendingWrite bool
	if bytes.Equal(bytes.TrimSpace(auth["pendingWrite"]), []byte("null")) || len(auth["pendingWrite"]) == 0 || json.Unmarshal(auth["pendingWrite"], &pendingWrite) != nil {
		return fmt.Errorf("invalid mixed pending state")
	}
	pendingRaw, present := auth["pendingRequest"]
	if present != pendingWrite {
		return fmt.Errorf("mixed pending identity presence mismatch")
	}
	if pendingWrite && !sameJSON(auth["everApplicationReserved"], json.RawMessage("true")) {
		return fmt.Errorf("mixed pending reservation history missing")
	}
	if pendingWrite {
		var request v1alpha2PendingRequest
		if decodeV1Alpha2(pendingRaw, &request) != nil || !sameJSON(request.PriorEvidence, auth["evidence"]) {
			return fmt.Errorf("invalid mixed pending request")
		}
		var view V1Alpha2AuthorityView
		if json.Unmarshal(auth["owner"], &view.Owner) != nil || json.Unmarshal(auth["binding"], &view.Binding) != nil || json.Unmarshal(auth["generation"], &view.Generation) != nil || request.Reservation == nil || *request.Reservation != *reservationContext(view) {
			return fmt.Errorf("mixed pending authority differs from reservation")
		}
		switch request.Kind {
		case "sql":
			if len(request.OriginalKV) != 0 {
				return fmt.Errorf("invalid mixed pending original union")
			}
			if _, err := decodePendingSQL(request.OriginalSQL, request.V1Alpha2WriteRequest); err != nil {
				return err
			}
		case "kv":
			if len(request.OriginalSQL) != 0 {
				return fmt.Errorf("invalid mixed pending original union")
			}
			if _, err := decodePendingKV(request.OriginalKV, request.V1Alpha2WriteRequest); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported mixed pending kind")
		}
	}
	opaque := make(map[string]json.RawMessage)
	for key, raw := range status {
		switch key {
		case "authority", "genesis", "applicationInit", "bootstrapPlan", "repair", "recoveryQueue":
		default:
			opaque["status."+key] = raw
		}
	}
	for key, raw := range auth {
		switch key {
		case "owner", "generation", "binding", "pendingWrite", "everApplicationReserved", "evidence", "pendingRequest", "transition":
		default:
			opaque["authority."+key] = raw
		}
	}
	for _, parent := range []struct {
		name string
		keys []string
	}{{"owner", []string{"namespace", "name", "uid", "logicalID"}}, {"binding", []string{"deploymentUID", "storageID"}}} {
		fields, err := jsonObject(auth[parent.name])
		if err != nil {
			return err
		}
		for _, key := range parent.keys {
			delete(fields, key)
		}
		if len(fields) > 0 {
			opaque["authority."+parent.name], _ = json.Marshal(fields)
		}
	}
	for key, raw := range obj {
		switch key {
		case "apiVersion", "kind", "metadata", "spec", "status":
		default:
			opaque["object."+key] = raw
		}
	}
	if err := mixedComponentSize(opaque, 64<<10); err != nil {
		return err
	}
	if err := mixedComponentSize(auth["evidence"], 64<<10); err != nil {
		return err
	}
	delete(auth, "evidence")
	if raw, exists := auth["pendingRequest"]; exists {
		pending, err := jsonObject(raw)
		if err != nil {
			return err
		}
		if err := mixedComponentSize(pending["priorEvidence"], 64<<10); err != nil {
			return err
		}
		for _, key := range []string{"originalSQL", "originalKV"} {
			if err := mixedComponentSize(pending[key], types.MaxManagedKVOriginalBytes); err != nil {
				return err
			}
			delete(pending, key)
		}
		delete(pending, "priorEvidence")
		auth["pendingRequest"], _ = json.Marshal(pending)
	}
	status["authority"], _ = json.Marshal(auth)
	delete(status, "repair")
	base := make(map[string]json.RawMessage, len(obj))
	for key, raw := range obj {
		base[key] = raw
	}
	base["status"], _ = json.Marshal(status)
	// Metadata's initial64 + cumulative growth64 is one fixed128KiB ceiling,
	// never another fresh allowance at each Load. Other components replace
	// original300 + prior64 + current64 + complete repair128 KiB.
	base["metadata"] = json.RawMessage(`{}`)
	raw, err := json.Marshal(base)
	projectedBase := len(raw) - 2 + managedMixedMetadataCeiling
	if err != nil || projectedBase > managedMixedProjectedBaseLimit || projectedBase+managedMixedFutureReserve > managedMixedObjectLimit {
		return fmt.Errorf("mixed application future headroom unavailable")
	}
	return nil
}
