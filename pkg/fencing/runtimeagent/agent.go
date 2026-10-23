// Package runtimeagent exposes the narrow authenticated node-local runtime
// enrollment and exact-stop API. It is not a general command or PID executor.
package runtimeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
)

const maxMessage = 1 << 20

type Principal struct {
	Username    string
	PodUID      string
	WorkloadUID string
	Audiences   []string
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

// Backend owns the qualified CRI/cgroup profile. Enroll returns the complete
// CRI-derived binding and opaque captured scope. Stop and ResumeStop may return
// a receipt only after exact terminal CRI observations and cgroup emptiness.
type Backend interface {
	Enroll(context.Context, fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error)
	Stop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error)
	ResumeStop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error)
}

type Config struct {
	AuthorityID             string
	AuthorityNamespace      string
	AuthorityServiceAccount string
	AuthorityWorkloadUID    string
	Audience                string
	Directory               string
	Auth                    Authenticator
	Backend                 Backend
	Diagnostic              func(stage, code string)
}

type Service struct {
	mu          sync.Mutex
	cfg         Config
	base        string
	ctx         context.Context
	cancel      context.CancelFunc
	watching    map[string]bool
	writeRecord func(string, record) error
}

type record struct {
	Binding            fencing.RuntimeBinding  `json:"binding"`
	Scope              json.RawMessage         `json:"scope"`
	PassiveObservation *cri.PassiveObservation `json:"passiveObservation,omitempty"`
	Stop               *fencing.StopRequest    `json:"stop,omitempty"`
	Receipt            json.RawMessage         `json:"receipt,omitempty"`
}

type enrollRequest struct {
	Binding fencing.RuntimeBinding `json:"binding"`
}
type stopRequest struct {
	Binding fencing.RuntimeBinding `json:"binding"`
	Request fencing.StopRequest    `json:"request"`
}
type verifyRequest struct {
	Binding fencing.RuntimeBinding `json:"binding"`
	Request fencing.StopRequest    `json:"request"`
	Receipt json.RawMessage        `json:"receipt"`
}

func New(cfg Config) (*Service, error) {
	if cfg.AuthorityID == "" || cfg.AuthorityNamespace == "" || cfg.AuthorityServiceAccount == "" || cfg.AuthorityWorkloadUID == "" || cfg.Audience == "" || cfg.Directory == "" || cfg.Auth == nil || cfg.Backend == nil || !filepath.IsAbs(cfg.Directory) {
		return nil, fmt.Errorf("fencing: incomplete runtime-agent configuration")
	}
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(cfg.Directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("fencing: unsafe runtime-agent state directory")
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{cfg: cfg, base: cfg.Directory, ctx: ctx, cancel: cancel, watching: make(map[string]bool)}
	service.writeRecord = service.write
	return service, nil
}

// Close stops bounded passive observers. Process exit also closes all kernel
// descriptors, but callers should close the service during orderly shutdown.
func (s *Service) Close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if backend, ok := s.cfg.Backend.(cri.PassiveTerminalBackend); ok {
		for key := range s.watching {
			if state, err := s.read(key); err == nil {
				backend.CompletePassiveTerminal(state.Binding, state.Scope)
			}
		}
	}
}

func (s *Service) Handler() http.Handler { return http.HandlerFunc(s.handle) }

func (s *Service) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := s.authorize(r); err != nil {
		http.Error(w, "runtime authority rejected", http.StatusForbidden)
		return
	}
	switch r.URL.Path {
	case "/v1/enroll":
		var req enrollRequest
		if !decode(w, r, &req) {
			return
		}
		binding, err := s.enroll(r.Context(), req.Binding)
		if err != nil {
			http.Error(w, "runtime enrollment unavailable", http.StatusConflict)
			return
		}
		writeJSON(w, binding)
	case "/v1/stop":
		var req stopRequest
		if !decode(w, r, &req) {
			return
		}
		receipt, err := s.stop(r.Context(), req.Binding, req.Request)
		if err != nil {
			if diagnostic, ok := cri.StopDiagnosticFrom(err); ok {
				w.Header().Set("X-Rhiza-Stop-Stage", diagnostic.Stage)
				w.Header().Set("X-Rhiza-Stop-Code", diagnostic.Code)
			} else {
				w.Header().Set("X-Rhiza-Stop-Stage", "backend_stop")
				w.Header().Set("X-Rhiza-Stop-Code", "backend_unclassified")
			}
			http.Error(w, "runtime stop pending", http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(receipt)
	case "/v1/verify":
		var req verifyRequest
		if !decode(w, r, &req) {
			return
		}
		if err := s.verify(req.Binding, req.Request, req.Receipt); err != nil {
			http.Error(w, "runtime receipt mismatch", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func (s *Service) authorize(r *http.Request) error {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return errors.New("missing bearer token")
	}
	p, err := s.cfg.Auth.Authenticate(r.Context(), token)
	if err != nil || p.PodUID == "" || p.WorkloadUID != s.cfg.AuthorityWorkloadUID || p.Username != "system:serviceaccount:"+s.cfg.AuthorityNamespace+":"+s.cfg.AuthorityServiceAccount {
		return errors.New("not the dedicated fencing authority")
	}
	if len(p.Audiences) == 1 && p.Audiences[0] == s.cfg.Audience {
		return nil
	}
	return errors.New("authority token must have exactly the dedicated audience")
}

func (s *Service) enroll(ctx context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, error) {
	if base.AuthorityID != s.cfg.AuthorityID {
		s.enrollmentDiagnostic("request_validation", "authority_mismatch")
		return fencing.RuntimeBinding{}, errors.New("runtime enrollment authority differs")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := enrollmentKey(base)
	if old, err := s.read(key); err == nil {
		if !sameEnrollmentBase(old.Binding, base) || (base.CapturedScopeDigest != "" && old.Binding != base) {
			s.enrollmentDiagnostic("state_read", "binding_conflict")
			return fencing.RuntimeBinding{}, errors.New("runtime enrollment conflict")
		}
		return old.Binding, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		s.enrollmentDiagnostic("state_read", "state_unavailable")
		return fencing.RuntimeBinding{}, err
	}
	recovered, err := s.captureEnrollment(ctx, base)
	if err != nil {
		return fencing.RuntimeBinding{}, err
	}
	if err := s.write(key, recovered); err != nil {
		s.enrollmentDiagnostic("state_persist", "state_write_failed")
		return fencing.RuntimeBinding{}, err
	}
	s.startPassiveWatcher(key, recovered)
	return recovered.Binding, nil
}

func (s *Service) startPassiveWatcher(key string, state record) {
	if _, ok := s.cfg.Backend.(cri.PassiveTerminalBackend); !ok || state.Scope == nil || len(state.Scope) == 0 {
		return
	}
	var scope cri.CgroupScope
	d := json.NewDecoder(bytes.NewReader(state.Scope))
	d.DisallowUnknownFields()
	if d.Decode(&scope) != nil || scope.Passive == nil {
		return
	}
	if s.watching[key] {
		return
	}
	s.watching[key] = true
	go s.watchPassive(key, state.Binding, append(json.RawMessage(nil), state.Scope...))
}

func (s *Service) watchPassive(key string, binding fencing.RuntimeBinding, scope json.RawMessage) {
	defer func() {
		s.mu.Lock()
		delete(s.watching, key)
		s.mu.Unlock()
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	backend := s.cfg.Backend.(cri.PassiveTerminalBackend)
	var pending *cri.PassiveObservation
	writeFailures := 0
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		state, err := s.read(key)
		if err != nil || state.Binding != binding {
			s.mu.Unlock()
			return
		}
		if state.Stop != nil && state.PassiveObservation == nil {
			if pending != nil {
				s.mu.Unlock()
				return
			}
			backend.CompletePassiveTerminal(binding, scope)
			s.mu.Unlock()
			return
		}
		var observation cri.PassiveObservation
		if state.PassiveObservation != nil {
			observation = *state.PassiveObservation
			if pending != nil && *pending != observation {
				s.enrollmentDiagnostic("passive_observation_persist", "state_conflict")
				s.mu.Unlock()
				return
			}
			pending = &observation
		} else if pending != nil {
			observation = *pending
			state.PassiveObservation = &observation
		} else {
			var complete bool
			observation, complete, err = backend.ObservePassiveTerminal(s.ctx, binding, scope)
			if err != nil {
				backend.CompletePassiveTerminal(binding, scope)
				s.mu.Unlock()
				return
			}
			if !complete {
				s.mu.Unlock()
				continue
			}
			if err := validatePassiveRecord(scope, binding, observation); err != nil {
				backend.CompletePassiveTerminal(binding, scope)
				s.mu.Unlock()
				return
			}
			pending = &observation
			state.PassiveObservation = &observation
		}
		if err := validatePassiveRecord(scope, binding, observation); err != nil {
			s.enrollmentDiagnostic("passive_observation_persist", "observation_invalid")
			s.mu.Unlock()
			return
		}
		if err := s.writeRecord(key, state); err != nil {
			writeFailures++
			s.enrollmentDiagnostic("passive_observation_persist", "state_write_failed")
			s.mu.Unlock()
			if writeFailures >= 5 {
				return
			}
			continue
		}
		backend.CompletePassiveTerminal(binding, scope)
		s.mu.Unlock()
		return
	}
}

func (s *Service) captureEnrollment(ctx context.Context, expected fencing.RuntimeBinding) (record, error) {
	binding, scope, err := s.cfg.Backend.Enroll(ctx, expected)
	if err != nil {
		s.enrollmentDiagnostic("cri_enroll", enrollmentBackendErrorCode(err))
		var namespaceDiagnostic interface{ CgroupNamespaceDiagnosticCode() string }
		if errors.As(err, &namespaceDiagnostic) {
			s.enrollmentDiagnostic("cri_enroll", namespaceDiagnostic.CgroupNamespaceDiagnosticCode())
		}
		return record{}, err
	}
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID, AssignmentID: binding.AssignmentID}
	binding.CapturedScopeDigest = ""
	if err := binding.Validate(identity); err != nil {
		s.enrollmentDiagnostic("backend_result_validation", "binding_invalid")
		return record{}, errors.New("runtime backend returned incomplete enrollment")
	}
	if !sameEnrollmentBase(binding, expected) {
		s.enrollmentDiagnostic("backend_result_validation", "binding_mismatch")
		return record{}, errors.New("runtime backend returned incomplete enrollment")
	}
	if err := validateCapturedScope(scope, binding); err != nil {
		s.enrollmentDiagnostic("backend_result_validation", "scope_invalid")
		return record{}, err
	}
	digest := rawDigest(scope)
	if expected.CapturedScopeDigest != "" && expected.CapturedScopeDigest != digest {
		s.enrollmentDiagnostic("backend_result_validation", "scope_digest_mismatch")
		return record{}, errors.New("runtime backend captured scope differs from the enrolled scope")
	}
	binding.CapturedScopeDigest = digest
	if expected.CapturedScopeDigest != "" && binding != expected {
		s.enrollmentDiagnostic("backend_result_validation", "binding_mismatch")
		return record{}, errors.New("runtime backend re-enrollment differs from the immutable runtime binding")
	}
	return record{Binding: binding, Scope: append(json.RawMessage(nil), scope...)}, nil
}

func validateCapturedScope(data json.RawMessage, binding fencing.RuntimeBinding) error {
	if len(data) == 0 || !json.Valid(data) {
		return errors.New("runtime backend returned incomplete enrollment scope")
	}
	var scope cri.CgroupScope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&scope); err != nil || decoder.Decode(new(any)) != io.EOF ||
		scope.Path == "" || scope.Dev == 0 || scope.Ino == 0 || scope.PID == 0 || scope.StartTime == 0 ||
		scope.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || scope.ProfileID == "" {
		return errors.New("runtime backend returned invalid captured cgroup scope")
	}
	if scope.Passive != nil {
		if scope.Passive.RuntimeHash != fencing.RuntimeScopeBindingDigest(binding) || cri.ValidatePassiveScope(scope) != nil {
			return errors.New("runtime backend returned invalid passive terminal scope")
		}
	}
	return nil
}

func validatePassiveRecord(scopeData json.RawMessage, binding fencing.RuntimeBinding, observation cri.PassiveObservation) error {
	var scope cri.CgroupScope
	decoder := json.NewDecoder(bytes.NewReader(scopeData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&scope); err != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("runtime passive scope is invalid")
	}
	return cri.ValidatePassiveObservation(binding, scope, observation)
}

func rawDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (s *Service) enrollmentDiagnostic(stage, code string) {
	if s.cfg.Diagnostic != nil {
		s.cfg.Diagnostic(stage, code)
	}
}

func enrollmentBackendErrorCode(err error) string {
	if !errors.Is(err, cri.ErrUnverified) {
		return "backend_error"
	}
	for _, check := range []struct{ marker, code string }{
		{"incomplete qualified runtime enrollment", "binding_incomplete"},
		{"unsupported runtime enrollment profile", "profile_unsupported"},
		{"read host identity", "host_identity_unavailable"},
		{"local node or boot identity differs", "host_identity_mismatch"},
		{"enrolled node, machine, boot or kernel differs", "host_identity_mismatch"},
		{"runtime version is not the qualified profile", "runtime_profile_mismatch"},
		{"runtime version:", "runtime_api_unavailable"},
		{"enrolled runtime/API version differs", "runtime_profile_mismatch"},
		{"container ID is not containerd-qualified", "container_id_unqualified"},
		{"pre-child container metadata differs", "container_metadata_mismatch"},
		{"containerd verbose info is missing or ambiguous", "containerd_verbose_info_missing"},
		{"containerd verbose info is not an object", "containerd_verbose_info_invalid"},
		{"invalid containerd verbose info", "containerd_verbose_info_invalid"},
		{"invalid containerd verbose info key", "containerd_verbose_info_invalid"},
		{"invalid containerd verbose info value", "containerd_verbose_info_invalid"},
		{"incomplete containerd verbose info", "containerd_verbose_info_invalid"},
		{"trailing containerd verbose info", "containerd_verbose_info_invalid"},
		{"duplicate containerd verbose info field", "containerd_verbose_field_duplicate"},
		{"containerd verbose info field", "containerd_verbose_field_missing"},
		{"containerd verbose info schema differs", "containerd_verbose_schema_mismatch"},
		{"containerd verbose identity differs", "containerd_verbose_identity_mismatch"},
		{"containerd PID changed during cgroup capture", "containerd_pid_changed_during_capture"},
		{"containerd PID changed during pre-child capture", "containerd_pid_changed_during_capture"},
		{"sandbox status:", "sandbox_status_unavailable"},
		{"sandbox enrollment or namespace metadata differs", "sandbox_metadata_mismatch"},
		{"private execution namespaces are required", "execution_namespace_mismatch"},
		{"container status:", "container_status_unavailable"},
		{"container enrollment differs", "container_metadata_mismatch"},
		{"sandbox container inventory:", "sandbox_inventory_unavailable"},
		{"sandbox inventory is missing", "sandbox_inventory_missing"},
		{"sandbox has unknown or duplicate container scope", "sandbox_inventory_ambiguous"},
		{"target container inventory differs", "target_container_metadata_mismatch"},
		{"target container is outside the captured sandbox", "target_container_outside_sandbox"},
		{"invalid cgroup capture configuration", "cgroup_capture_configuration_invalid"},
		{"read enrolled process identity", "process_identity_unavailable"},
		{"read process cgroup", "process_cgroup_unavailable"},
		{"unified cgroup path is unsafe (root)", "process_cgroup_path_root"},
		{"unified cgroup path is unsafe (absolute)", "process_cgroup_path_absolute"},
		{"unified cgroup path is unsafe (dot)", "process_cgroup_path_dot"},
		{"unified cgroup path is unsafe (noncanonical)", "process_cgroup_path_noncanonical"},
		{"unified cgroup path is unsafe (traversal)", "process_cgroup_path_traversal"},
		{"unified cgroup path missing", "process_cgroup_path_missing"},
		{"resolve cgroup root", "cgroup_root_unavailable"},
		{"resolve cgroup scope", "cgroup_scope_unavailable"},
		{"cgroup directory unavailable", "cgroup_scope_unavailable"},
		{"cgroup filesystem identity unavailable", "cgroup_identity_unavailable"},
		{"cgroup is not populated", "cgroup_not_populated"},
		{"process changed during pre-child capture", "process_changed_during_capture"},
		{"process changed during cgroup capture", "process_changed_during_capture"},
		{"runtime enrollment identity differs", "runtime_binding_invalid"},
		{"runtime enrollment is incomplete", "runtime_binding_invalid"},
	} {
		if strings.Contains(err.Error(), check.marker) {
			return check.code
		}
	}
	return "cri_scope_unverified"
}

func (s *Service) stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if binding.AuthorityID != s.cfg.AuthorityID || binding.Validate(request.Identity) != nil || request.OperationID == "" || !validDigest(request.RequestHash) {
		return nil, cri.NewStopDiagnosticError("stop_request", "invalid_request", errors.New("invalid exact stop request"))
	}
	key := enrollmentKey(binding)
	state, err := s.read(key)
	if errors.Is(err, os.ErrNotExist) {
		if !validDigest(binding.CapturedScopeDigest) {
			return nil, cri.NewStopDiagnosticError("enrollment_state", "missing_or_unreadable", errors.New("runtime enrollment lacks a root-pinned captured scope digest"))
		}
		state, err = s.captureEnrollment(ctx, binding)
		if err != nil {
			return nil, cri.NewStopDiagnosticError("enrollment_capture", "capture_unverified", err)
		}
		if state.Binding != binding {
			return nil, cri.NewStopDiagnosticError("enrollment_state", "binding_mismatch", errors.New("runtime enrollment differs from the immutable stop binding"))
		}
		if err := s.write(key, state); err != nil {
			return nil, cri.NewStopDiagnosticError("enrollment_state", "missing_or_unreadable", err)
		}
	} else if err != nil || state.Binding != binding {
		return nil, cri.NewStopDiagnosticError("enrollment_state", "missing_or_unreadable", errors.New("runtime enrollment missing"))
	}
	// A retry reads the durable receipt before any CRI call or metadata lookup.
	if state.Stop != nil {
		if *state.Stop != request {
			return nil, cri.NewStopDiagnosticError("stop_request", "invalid_request", errors.New("different stop request for enrolled run"))
		}
		if len(state.Receipt) != 0 {
			return append(json.RawMessage(nil), state.Receipt...), nil
		}
		if state.PassiveObservation != nil {
			if backend, ok := s.cfg.Backend.(cri.PassiveTerminalBackend); ok {
				receipt, err := backend.BindPassiveTerminal(binding, request, state.Scope, *state.PassiveObservation)
				if err != nil {
					return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", err)
				}
				return s.persistReceipt(key, state, request, receipt)
			}
			return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", errors.New("passive terminal observation backend unavailable"))
		}
		receipt, err := s.cfg.Backend.ResumeStop(ctx, binding, request, state.Scope)
		if err != nil {
			return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", err)
		}
		return s.persistReceipt(key, state, request, receipt)
	}
	// Observe a previously enrolled process incarnation before writing fresh
	// stop intent. A complete passive observation is fsynced independently; only
	// after the exact request is durable may it be converted into a receipt.
	if state.PassiveObservation == nil {
		if backend, ok := s.cfg.Backend.(cri.PassiveTerminalBackend); ok {
			observation, complete, observeErr := backend.ObservePassiveTerminal(ctx, binding, state.Scope)
			if observeErr == nil && complete {
				if err := validatePassiveRecord(state.Scope, binding, observation); err != nil {
					return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", err)
				}
				state.PassiveObservation = &observation
				if err := s.write(key, state); err != nil {
					return nil, cri.NewStopDiagnosticError("passive_observation_persist", "observation_write_failed", err)
				}
				backend.CompletePassiveTerminal(binding, state.Scope)
			}
		}
	}
	// Persist exact intent before any potentially effective CRI request. After a
	// crash, only this exact tuple may be resumed from captured scope evidence.
	state.Stop = &request
	if err := s.write(key, state); err != nil {
		return nil, cri.NewStopDiagnosticError("stop_intent_persist", "intent_write_failed", err)
	}
	if state.PassiveObservation != nil {
		backend, ok := s.cfg.Backend.(cri.PassiveTerminalBackend)
		if !ok {
			return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", errors.New("passive terminal observation backend unavailable"))
		}
		receipt, err := backend.BindPassiveTerminal(binding, request, state.Scope, *state.PassiveObservation)
		if err != nil {
			return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", err)
		}
		return s.persistReceipt(key, state, request, receipt)
	}
	receipt, err := s.cfg.Backend.Stop(ctx, binding, request, state.Scope)
	if err != nil {
		return nil, cri.NewStopDiagnosticError("backend_stop", "backend_unclassified", err)
	}
	return s.persistReceipt(key, state, request, receipt)
}

func (s *Service) persistReceipt(key string, state record, request fencing.StopRequest, receipt json.RawMessage) (json.RawMessage, error) {
	if len(receipt) == 0 || len(receipt) > maxMessage || !json.Valid(receipt) {
		return nil, cri.NewStopDiagnosticError("receipt_validation", "receipt_invalid", errors.New("runtime backend returned invalid receipt"))
	}
	state.Stop, state.Receipt = &request, append(json.RawMessage(nil), receipt...)
	if err := s.write(key, state); err != nil {
		return nil, cri.NewStopDiagnosticError("receipt_persist", "receipt_write_failed", err)
	}
	if backend, ok := s.cfg.Backend.(cri.PassiveTerminalBackend); ok {
		backend.CompletePassiveTerminal(state.Binding, state.Scope)
	}
	return append(json.RawMessage(nil), receipt...), nil
}

func (s *Service) verify(binding fencing.RuntimeBinding, request fencing.StopRequest, receipt json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if binding.AuthorityID != s.cfg.AuthorityID || binding.Validate(request.Identity) != nil || !validDigest(request.RequestHash) || len(receipt) == 0 {
		return errors.New("invalid receipt tuple")
	}
	state, err := s.read(enrollmentKey(binding))
	if err != nil || state.Binding != binding || state.Stop == nil || *state.Stop != request || !bytes.Equal(state.Receipt, receipt) {
		return errors.New("durable receipt not found")
	}
	return nil
}

func sameEnrollmentBase(a, b fencing.RuntimeBinding) bool {
	a.NodeUID, a.MachineID, a.BootID, a.RuntimeID, a.SandboxID = "", "", "", "", ""
	b.NodeUID, b.MachineID, b.BootID, b.RuntimeID, b.SandboxID = "", "", "", "", ""
	a.CapturedScopeDigest, b.CapturedScopeDigest = "", ""
	return a == b
}

func enrollmentKey(binding fencing.RuntimeBinding) string {
	data, _ := json.Marshal([]string{binding.AuthorityID, binding.NamespaceUID, binding.BindingUID, binding.StatefulSetUID, binding.SourceClusterID, binding.NodeID, binding.PodUID, binding.RunID})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (s *Service) read(key string) (record, error) {
	path := filepath.Join(s.base, key+".json")
	info, err := os.Lstat(path)
	if err != nil {
		return record{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return record{}, errors.New("unsafe runtime record")
	}
	f, err := os.Open(path)
	if err != nil {
		return record{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxMessage+1))
	if err != nil || len(data) > maxMessage {
		return record{}, errors.New("invalid runtime record size")
	}
	var result record
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&result); err != nil {
		return record{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return record{}, errors.New("trailing runtime record")
	}
	return result, nil
}

func (s *Service) write(key string, value record) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxMessage {
		return errors.New("runtime record exceeds capacity")
	}
	f, err := os.CreateTemp(s.base, ".runtime-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.base, key+".json")); err != nil {
		return err
	}
	dir, err := os.Open(s.base)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxMessage))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		http.Error(w, "invalid runtime request", http.StatusBadRequest)
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		http.Error(w, "trailing runtime request", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
