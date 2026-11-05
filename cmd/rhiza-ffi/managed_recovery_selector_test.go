//go:build cgo

package main

import "testing"

func TestManagedRecoverySelectorStrictPair(t *testing.T) {
	for _, tc := range []struct {
		name, input              string
		allowEmpty, valid, empty bool
	}{
		{"noarg", `{}`, true, true, true},
		{"status-needs-selector", `{}`, false, false, false},
		{"sql", `{"kind":"sql","request_id_b64":"eA=="}`, true, true, false},
		{"raw-id", `{"kind":"kv","request_id_b64":"/w=="}`, false, true, false},
		{"ordered-escaped", `{"request_id_b64":"e\u0041==","\u006bind":"s\u0071l"}`, false, true, false},
		{"duplicate-kind", `{"kind":"sql","kind":"sql","request_id_b64":"eA=="}`, true, false, false},
		{"duplicate-id", `{"kind":"sql","request_id_b64":"eA==","request_id_b64":"eA=="}`, true, false, false},
		{"escaped-duplicate", `{"kind":"sql","\u006bind":"sql","request_id_b64":"eA=="}`, true, false, false},
		{"escaped-id-duplicate", `{"kind":"sql","request_id_b64":"eA==","request_id_b\u00364":"eA=="}`, true, false, false},
		{"null-shadow", `{"kind":null,"kind":"sql","request_id_b64":"eA=="}`, true, false, false},
		{"id-null-shadow", `{"kind":"sql","request_id_b64":null,"request_id_b64":"eA=="}`, true, false, false},
		{"kind-alias", `{"Kind":"sql","request_id_b64":"eA=="}`, true, false, false},
		{"id-alias", `{"kind":"sql","REQUEST_ID_B64":"eA=="}`, true, false, false},
		{"kind-alias-shadow", `{"kind":"sql","Kind":"sql","request_id_b64":"eA=="}`, true, false, false},
		{"trailing-object", `{"kind":"sql","request_id_b64":"eA=="}{}`, true, false, false},
		{"trailing-scalar", `{} true`, true, false, false},
		{"array", `[]`, true, false, false},
		{"string", `"selector"`, true, false, false},
		{"empty-input", ``, true, false, false},
		{"unpadded", `{"kind":"kv","request_id_b64":"eA"}`, true, false, false},
		{"padding-alias", `{"kind":"kv","request_id_b64":"eB=="}`, true, false, false},
		{"null-object", `null`, true, false, false},
		{"null-kind", `{"kind":null,"request_id_b64":"eA=="}`, true, false, false},
		{"null-id", `{"kind":"sql","request_id_b64":null}`, true, false, false},
		{"kind-only", `{"kind":"sql"}`, true, false, false},
		{"id-only", `{"request_id_b64":"eA=="}`, true, false, false},
		{"bad-kind", `{"kind":"notify","request_id_b64":"eA=="}`, true, false, false},
		{"bad-base64", `{"kind":"kv","request_id_b64":"!"}`, true, false, false},
		{"extra", `{"kind":"kv","request_id_b64":"eA==","original":{}}`, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector, err := decodeManagedRequestSelector([]byte(tc.input), tc.allowEmpty)
			if (err == nil) != tc.valid || tc.valid && (selector == nil) != tc.empty {
				t.Fatalf("selector validity/empty mismatch: error=%v", err)
			}
			if tc.name == "raw-id" && selector != nil && selector.RequestID != "\xff" {
				t.Fatal("raw ID changed")
			}
			if tc.valid && !tc.empty && tc.name != "raw-id" && (selector.Kind != "sql" || selector.RequestID != "x") {
				t.Fatal("selector identity changed")
			}
		})
	}
}
