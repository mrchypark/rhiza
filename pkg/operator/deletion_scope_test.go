package operator

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestConfigureDeletionScope(t *testing.T) {
	for _, test := range []struct {
		name, raw, own string
		want           []string
		all, bad       bool
	}{
		{name: "default own not watch", own: "operator", want: []string{"operator"}},
		{name: "empty own", bad: true},
		{name: "own cannot be wildcard", own: "*", bad: true},
		{name: "own cannot be CSV", own: "a,b", bad: true},
		{name: "CSV trim dedupe", raw: " a, b, a ", want: []string{"a", "b"}},
		{name: "explicit namespaces need no own", raw: "target", want: []string{"target"}},
		{name: "exclusive wildcard", raw: " * ", all: true},
		{name: "mixed wildcard", raw: "a,*", bad: true},
		{name: "empty CSV item", raw: "a,,b", bad: true},
		{name: "trailing comma", raw: "a,", bad: true},
		{name: "uppercase", raw: "A", bad: true},
		{name: "underscore", raw: "a_b", bad: true},
		{name: "dot", raw: "a.b", bad: true},
		{name: "long namespace", raw: strings.Repeat("a", 64), bad: true},
		{name: "leading dash", raw: "-a", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := &Kubernetes{Namespace: "watch"}
			if err := k.ConfigureDeletion(test.raw, test.own); (err != nil) != test.bad {
				t.Fatalf("configure error=%v", err)
			}
			if test.bad {
				if k.deleteAllNamespaces || len(k.deleteNamespaces) != 0 {
					t.Fatal("invalid configuration retained delete authority")
				}
				return
			}
			if k.deleteAllNamespaces != test.all || len(k.deleteNamespaces) != len(test.want) {
				t.Fatalf("scope=%v all=%v", k.deleteNamespaces, k.deleteAllNamespaces)
			}
			for _, name := range test.want {
				if !k.deleteNamespaces[name] {
					t.Fatalf("missing namespace %s", name)
				}
			}
		})
	}
}

func TestDeletePodRejectsScopeAndPathBeforeCredentials(t *testing.T) {
	for _, test := range []struct{ name, scope, namespace, pod string }{
		{"unconfigured", "", "operator", "learner"},
		{"outside own", "operator", "other", "learner"},
		{"collection", "*", "other", ""},
		{"namespace traversal", "*", "other/../operator", "learner"},
		{"Pod traversal", "*", "other", "../learner"},
		{"query", "*", "other", "learner?gracePeriodSeconds=0"},
		{"encoded slash", "*", "other", "a%2fb"},
		{"path", "*", "other", "learner/status"},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := &Kubernetes{BaseURL: "https://not-used.invalid", TokenFile: "/not-a-token"}
			if test.scope != "" {
				if err := k.ConfigureDeletion(test.scope, "operator"); err != nil {
					t.Fatal(err)
				}
			}
			err := k.DeletePod(t.Context(), test.namespace, test.pod, "exact-uid")
			if err == nil || strings.Contains(err.Error(), "token") {
				t.Fatalf("scope/path guard did not run before credentials: %v", err)
			}
		})
	}
}

func TestDeletePodAllowlistStillRequiresRBACAndUID(t *testing.T) {
	for _, scope := range []string{"target", "*"} {
		t.Run(scope, func(t *testing.T) {
			var calls atomic.Int32
			k, _, closeServer := testKubernetes(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				var options object
				if req.Method != http.MethodDelete || req.URL.Path != "/api/v1/namespaces/target/pods/learner" || json.NewDecoder(req.Body).Decode(&options) != nil || nested(options, "preconditions", "uid") != "exact-uid" {
					t.Error("incorrect scoped deletion request")
				}
				w.WriteHeader(http.StatusForbidden)
			}))
			defer closeServer()
			if err := k.ConfigureDeletion(scope, "operator"); err != nil {
				t.Fatal(err)
			}
			var api *APIError
			if err := k.DeletePod(t.Context(), "target", "learner", "exact-uid"); !errors.As(err, &api) || api.StatusCode != http.StatusForbidden {
				t.Fatalf("RBAC failure lost: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("requests=%d", calls.Load())
			}
		})
	}
}
