package operator

import (
	"encoding/json"
	"log"
	"strconv"
)

const (
	startupRegistrationDenialEventName      = "startup_registration_denial"
	startupRegistrationDenialMaxLine        = 4 << 10
	startupRegistrationStageRootInit        = "root-init"
	startupRegistrationStageGrant           = "startup-grant"
	startupRegistrationStageLedgerRegister  = "ledger-register"
	startupRegistrationStageProducerReserve = "producer-reserve"
	startupRegistrationStageArchiveScope    = "archive-scope"
)

type startupRegistrationDenialEvent struct {
	Event              string `json:"event"`
	Action             string `json:"action"`
	Result             string `json:"result"`
	HTTPStatus         int    `json:"httpStatus"`
	Stage              string `json:"stage"`
	Namespace          string `json:"namespace,omitempty"`
	StatefulSetUID     string `json:"statefulSetUID,omitempty"`
	PodUID             string `json:"podUID,omitempty"`
	SourceClusterID    string `json:"sourceClusterID,omitempty"`
	AttemptedRunID     string `json:"attemptedRunID,omitempty"`
	RequestedNodeID    string `json:"requestedNodeID,omitempty"`
	GrantResolved      bool   `json:"grantResolved"`
	ResolvedNodeID     string `json:"resolvedNodeID,omitempty"`
	AssignmentID       string `json:"assignmentID,omitempty"`
	RuntimeContainerID string `json:"runtimeContainerID,omitempty"`
	OmittedReason      string `json:"omittedReason,omitempty"`
}

func emitStartupRegistrationDenial(event startupRegistrationDenialEvent) error {
	return emitStartupRegistrationDenialWithLogger(log.Default(), event)
}

func emitStartupRegistrationDenialWithLogger(logger *log.Logger, event startupRegistrationDenialEvent) error {
	line, err := encodeStartupRegistrationDenial(event)
	if err != nil {
		return err
	}
	return logger.Output(2, line)
}

func encodeStartupRegistrationDenial(event startupRegistrationDenialEvent) (string, error) {
	event.Event = startupRegistrationDenialEventName
	event.Action = "register"
	event.Result = "closed"
	event.HTTPStatus = 410
	if !validStartupRegistrationStage(event.Stage) {
		event.Stage = "unknown"
		setStartupDenialOmittedReason(&event, "invalid_stage")
	}
	for _, field := range []struct {
		value *string
		limit int
	}{
		{&event.Namespace, 256}, {&event.StatefulSetUID, 256}, {&event.PodUID, 256},
		{&event.SourceClusterID, 256}, {&event.AttemptedRunID, 128}, {&event.RequestedNodeID, 256},
		{&event.ResolvedNodeID, 256}, {&event.AssignmentID, 128}, {&event.RuntimeContainerID, 256},
	} {
		if len(*field.value) > field.limit {
			*field.value = ""
			setStartupDenialOmittedReason(&event, "identity_field_oversize")
		}
	}
	if !event.GrantResolved {
		event.ResolvedNodeID = ""
		event.AssignmentID = ""
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	if len(encoded)+1 > startupRegistrationDenialMaxLine {
		fallback := startupRegistrationDenialEvent{
			Event: startupRegistrationDenialEventName, Action: "register", Result: "closed", HTTPStatus: 410,
			Stage: event.Stage, OmittedReason: "event_oversize",
		}
		encoded, err = json.Marshal(fallback)
		if err != nil {
			return "", err
		}
	}
	return string(encoded), nil
}

func validStartupRegistrationStage(stage string) bool {
	switch stage {
	case startupRegistrationStageRootInit, startupRegistrationStageGrant, startupRegistrationStageLedgerRegister,
		startupRegistrationStageProducerReserve, startupRegistrationStageArchiveScope:
		return true
	default:
		return false
	}
}

func setStartupDenialOmittedReason(event *startupRegistrationDenialEvent, reason string) {
	if event.OmittedReason == "" {
		event.OmittedReason = reason
	}
}

// safeFenceLogField escapes control characters before they can create forged
// log records and caps attacker-influenced values in adjacent diagnostics.
func safeFenceLogField(value string, maxBytes int) string {
	if maxBytes < 0 {
		maxBytes = 0
	}
	if len(value) > maxBytes {
		value = value[:maxBytes]
	}
	return strconv.QuoteToASCII(value)
}
