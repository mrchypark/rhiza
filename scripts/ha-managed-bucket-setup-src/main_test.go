package main

import "testing"

func TestEndpointBoundary(t *testing.T) {
	bucket := "rhiza-managed-123-1-012345abcdef"
	for _, endpoint := range []string{"http://127.0.0.1:1234", "http://[::1]:1234"} {
		if err := validate(endpoint, bucket); err != nil {
			t.Fatal(err)
		}
	}
	for _, endpoint := range []string{"https://127.0.0.1:1234", "http://localhost:1234", "http://example.com:1234", "http://127.0.0.1:1234/path", "http://user@127.0.0.1:1234", "http://127.0.0.1:1234?x=1", "http://127.0.0.1"} {
		if validate(endpoint, bucket) == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
	if validate("http://127.0.0.1:1234", "production") == nil {
		t.Fatal("accepted arbitrary bucket")
	}
}
