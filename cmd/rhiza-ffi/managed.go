//go:build cgo

package main

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"

	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

type managedOpenRequest struct {
	RuntimeProfile    string `json:"runtime_profile"`
	IdentityFile      string `json:"identity_file"`
	HandoffFile       string `json:"handoff_file"`
	RepairHandoffFile string `json:"repair_handoff_file"`
}

func goOpenManaged(input []byte) []byte {
	var request managedOpenRequest
	if validInput(input) != nil || bytes.Equal(bytes.TrimSpace(input), []byte("null")) || decodeStrict(input, &request) != nil {
		return marshalError("invalid_request", "invalid managed open request")
	}
	if request.RuntimeProfile == "" {
		request.RuntimeProfile = operator.V1Alpha2ManagedRuntimeProfilePath
	}
	if request.IdentityFile == "" {
		request.IdentityFile = operator.V1Alpha2BootstrapIdentityPath
	}
	if request.HandoffFile == "" {
		request.HandoffFile = operator.V1Alpha2BootstrapHandoffPath
	}
	if request.RepairHandoffFile == "" {
		request.RepairHandoffFile = managedbootstrap.RepairHandoffPath
	}
	ref := operator.V1Alpha2ClusterRef{Name: os.Getenv("RHIZA_CLUSTER_NAME"), Namespace: os.Getenv("POD_NAMESPACE"), UID: os.Getenv("RHIZA_CLUSTER_UID")}
	pod, uid := os.Getenv("POD_NAME"), os.Getenv("POD_UID")
	if ref.Name == "" || ref.Namespace == "" || ref.UID == "" || pod == "" || uid == "" {
		return marshalError("invalid_request", "managed execution identity required")
	}
	f, err := os.Open(request.RuntimeProfile)
	if err != nil {
		return marshalError("invalid_request", "managed profile unavailable")
	}
	profile, err := operator.DecodeV1Alpha2RuntimeProfile(f)
	err = errors.Join(err, f.Close())
	if err != nil {
		return marshalError("invalid_request", "invalid managed profile")
	}
	kube, err := operator.NewInCluster(ref.Namespace)
	if err != nil {
		return marshalError("not_ready", "managed Kubernetes client unavailable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	opening, stop := context.WithTimeout(ctx, defaultTimeout)
	defer stop()
	cfg, cleanup, err := managedbootstrap.LoadConfig(opening, managedbootstrap.ConfigRequest{Kube: kube, Ref: ref, PodName: pod, PodUID: uid, IdentityPath: request.IdentityFile, BootstrapPath: request.HandoffFile, RepairPath: request.RepairHandoffFile, Profile: profile})
	if err != nil {
		cancel()
		_ = cleanup()
		return marshalError(errorCode(err), "managed startup inputs rejected")
	}
	host, err := managedsql.Open(opening, cfg)
	if err != nil {
		cancel()
		if host != nil {
			_ = host.Close()
		}
		_ = cleanup()
		return marshalError(errorCode(err), "managed startup rejected")
	}
	return registerEntry(&ffiEntry{managed: host, cleanup: cleanup, ctx: ctx, cancel: cancel, drained: make(chan struct{})})
}

func registerEntry(entry *ffiEntry) []byte {
	ffiRegistry.Lock()
	handle := ffiRegistry.next
	if handle == 0 || handle == math.MaxUint64 {
		ffiRegistry.Unlock()
		entry.cancel()
		_ = entry.closeOwned()
		return marshalError("handle_exhausted", "Rhiza handle space exhausted")
	}
	ffiRegistry.next++
	ffiRegistry.m[handle] = entry
	ffiRegistry.Unlock()
	return dataJSON(map[string]uint64{"handle": handle})
}

func (entry *ffiEntry) closeOwned() error {
	var err error
	if entry.managed != nil {
		err = entry.managed.Close()
	}
	if entry.db != nil {
		err = errors.Join(err, entry.db.Close())
	}
	if entry.cleanup != nil {
		err = errors.Join(err, entry.cleanup())
	}
	return err
}

func dispatchManaged(ctx context.Context, entry *ffiEntry, call callEnvelope) (any, error) {
	decode := func(target any) error {
		if decodeStrict(call.Request, target) != nil {
			return &ffiError{Code: "invalid_request", Message: "invalid managed request"}
		}
		return nil
	}
	switch call.Operation {
	case "execute":
		var r managedsql.ExecuteRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return entry.managed.Execute(ctx, r)
	case "query":
		var r managedsql.QueryRequest
		if err := decode(&r); err != nil {
			return nil, err
		}
		return entry.managed.Query(ctx, r)
	case "ready":
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		return entry.managed.Ready(ctx) == nil, nil
	case "recover_pending":
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		return nil, entry.managed.RecoverPending(ctx)
	default:
		return nil, &ffiError{Code: "invalid_request", Message: "operation unavailable on managed SQL handle"}
	}
}
