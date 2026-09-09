package operator

import "testing"

func TestEmbeddedRecoveryPort(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ports []any
		want  string
		bad   bool
	}{
		{"dedicated", []any{object{"name": "http", "containerPort": float64(8080)}, object{"name": "recovery", "containerPort": float64(9091)}}, "http://127.0.0.1:9091/recovery/status", false},
		{"legacy", []any{object{"name": "http", "containerPort": float64(8081)}}, "http://127.0.0.1:8081/recovery/status", false},
		{"default", nil, "http://127.0.0.1:8080/recovery/status", false},
		{"invalid", []any{object{"name": "recovery", "containerPort": float64(0)}}, "", true},
		{"udp", []any{object{"name": "recovery", "containerPort": float64(9091), "protocol": "UDP"}}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := object{"status": object{"podIP": "127.0.0.1"}, "spec": object{"containers": []any{object{"name": "application", "ports": tc.ports}}}}
			got, err := podEndpoint(pod, "application", "/recovery/status")
			if (err != nil) != tc.bad || got != tc.want {
				t.Fatalf("endpoint=%q err=%v", got, err)
			}
		})
	}
}
