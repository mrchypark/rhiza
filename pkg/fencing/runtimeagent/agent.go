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

	"github.com/mrchypark/rhiza/pkg/fencing"
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
}

type Service struct {
	mu   sync.Mutex
	cfg  Config
	base string
}

type record struct {
	Binding fencing.RuntimeBinding `json:"binding"`
	Scope   json.RawMessage        `json:"scope"`
	Stop    *fencing.StopRequest   `json:"stop,omitempty"`
	Receipt json.RawMessage        `json:"receipt,omitempty"`
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
	return &Service{cfg: cfg, base: cfg.Directory}, nil
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
		return fencing.RuntimeBinding{}, errors.New("runtime enrollment authority differs")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := enrollmentKey(base)
	if old, err := s.read(key); err == nil {
		if !sameEnrollmentBase(old.Binding, base) {
			return fencing.RuntimeBinding{}, errors.New("runtime enrollment conflict")
		}
		return old.Binding, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fencing.RuntimeBinding{}, err
	}
	binding, scope, err := s.cfg.Backend.Enroll(ctx, base)
	if err != nil {
		return fencing.RuntimeBinding{}, err
	}
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if err := binding.Validate(identity); err != nil || !sameEnrollmentBase(binding, base) || len(scope) == 0 || !json.Valid(scope) {
		return fencing.RuntimeBinding{}, errors.New("runtime backend returned incomplete enrollment")
	}
	if err := s.write(key, record{Binding: binding, Scope: scope}); err != nil {
		return fencing.RuntimeBinding{}, err
	}
	return binding, nil
}

func (s *Service) stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if binding.AuthorityID != s.cfg.AuthorityID || binding.Validate(request.Identity) != nil || request.OperationID == "" || !validDigest(request.RequestHash) {
		return nil, errors.New("invalid exact stop request")
	}
	key := enrollmentKey(binding)
	state, err := s.read(key)
	if err != nil || state.Binding != binding {
		return nil, errors.New("runtime enrollment missing")
	}
	// A retry reads the durable receipt before any CRI call or metadata lookup.
	if state.Stop != nil {
		if *state.Stop != request {
			return nil, errors.New("different stop request for enrolled run")
		}
		if len(state.Receipt) != 0 {
			return append(json.RawMessage(nil), state.Receipt...), nil
		}
		receipt, err := s.cfg.Backend.ResumeStop(ctx, binding, request, state.Scope)
		if err != nil {
			return nil, err
		}
		return s.persistReceipt(key, state, request, receipt)
	}
	// Persist exact intent before any potentially effective CRI request. After a
	// crash, only this exact tuple may be resumed from captured scope evidence.
	state.Stop = &request
	if err := s.write(key, state); err != nil {
		return nil, err
	}
	receipt, err := s.cfg.Backend.Stop(ctx, binding, request, state.Scope)
	if err != nil {
		return nil, err
	}
	return s.persistReceipt(key, state, request, receipt)
}

func (s *Service) persistReceipt(key string, state record, request fencing.StopRequest, receipt json.RawMessage) (json.RawMessage, error) {
	if len(receipt) == 0 || len(receipt) > maxMessage || !json.Valid(receipt) {
		return nil, errors.New("runtime backend returned invalid receipt")
	}
	state.Stop, state.Receipt = &request, append(json.RawMessage(nil), receipt...)
	if err := s.write(key, state); err != nil {
		return nil, err
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
