package cri

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestCRIStopDiagnosticsSeparateCRIEffectBoundaries(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*fakeRuntime)
		stage string
		code  string
	}{
		{"container stop", func(f *fakeRuntime) { f.stopError = codes.Unavailable }, "cri_stop_container", "container_stop_unconfirmed"},
		{"sandbox stop", func(f *fakeRuntime) { f.sandboxError = codes.Unavailable }, "cri_stop_sandbox", "sandbox_stop_unconfirmed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider, runtime, binding, request := newRuntimeFixture(t)
			test.setup(runtime)
			_, err := provider.Stop(t.Context(), binding, request)
			var diagnostic *StopDiagnosticError
			if !errors.As(err, &diagnostic) || diagnostic.Stage != test.stage || diagnostic.Code != test.code || !errors.Is(err, ErrUnverified) {
				t.Fatalf("stop diagnostic = %#v, err=%v; want %s/%s preserving ErrUnverified", diagnostic, err, test.stage, test.code)
			}
		})
	}
}
