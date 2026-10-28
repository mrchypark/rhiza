package fencing

import (
	"reflect"
	"testing"
)

func TestAuthorityMessageActionPayloadAllowlist(t *testing.T) {
	allowed := map[string]string{
		"register": "", "bind": "", "stop": "", "drain": "Drain", "exit": "Request", "never-bound-exit": "Request",
		"peer-closure-request": "PeerClosure", "peer-closure-status": "PeerClosure",
		"bootstrap-status": "BootstrapStatus", "bootstrap-runtime-admission": "RuntimeAdmission", "bootstrap-complete": "Completion",
		"init-begin": "BeginInit", "init-outcome": "InitOutcome",
		"membership-repair-status": "RepairStatus", "membership-repair-reserve": "RepairReserve", "membership-repair-authorize": "RepairPhase",
		"membership-repair-qualify": "RepairQualification", "membership-repair-phase-prepare": "RepairPhasePrepare",
		"membership-repair-phase-observe": "RepairPhaseObserve", "membership-repair-phase-complete": "RepairPhaseComplete",
	}
	typ := reflect.TypeOf(AuthorityMessage{})
	payloadFields := make(map[string]reflect.StructField)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Type.Kind() == reflect.Pointer {
			payloadFields[field.Name] = field
		}
	}
	for field := range payloadFields {
		found := false
		for _, expected := range allowed {
			if expected == field {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("AuthorityMessage payload %s is missing from action allowlist test", field)
		}
	}
	for action, expected := range allowed {
		t.Run(action, func(t *testing.T) {
			base := AuthorityMessage{Action: action}
			if expected != "" {
				setAuthorityMessagePayload(t, &base, payloadFields[expected])
			}
			if err := ValidateAuthorityMessageShape(base); err != nil {
				t.Fatalf("valid action payload rejected: %v", err)
			}
			for name, field := range payloadFields {
				if name == expected {
					continue
				}
				mixed := base
				setAuthorityMessagePayload(t, &mixed, field)
				if err := ValidateAuthorityMessageShape(mixed); err == nil {
					t.Errorf("action accepted unrelated payload %s", name)
				}
			}
		})
	}
}

func setAuthorityMessagePayload(t *testing.T, message *AuthorityMessage, field reflect.StructField) {
	t.Helper()
	value := reflect.ValueOf(message).Elem().FieldByName(field.Name)
	if !value.IsValid() || !value.CanSet() || value.Kind() != reflect.Pointer {
		t.Fatalf("payload field %s is not settable pointer", field.Name)
	}
	value.Set(reflect.New(field.Type.Elem()))
}
