package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func fixture() map[string]any {
	items := []any{}
	for i := 1; i <= 3; i++ {
		key := make([]byte, 32)
		key[0] = byte(i)
		report, _ := json.Marshal(map[string]any{"version": 1, "publicKey": base64.StdEncoding.EncodeToString(key)})
		items = append(items, map[string]any{"metadata": map[string]any{"uid": fmt.Sprint("pod-", i)}, "spec": map[string]any{"containers": []any{map[string]any{"name": "sql"}}}, "status": map[string]any{"podIP": fmt.Sprint("10.0.0.", i), "initContainerStatuses": []any{map[string]any{"name": "rhiza-prepare-identity", "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(report)}}}, map[string]any{"name": "rhiza-await-bootstrap-plan", "state": map[string]any{"running": map[string]any{}}}}, "containerStatuses": []any{map[string]any{"name": "sql", "state": map[string]any{"waiting": map[string]any{}}}}}})
	}
	return map[string]any{"items": items}
}
func encoded(v any) []byte { b, _ := json.Marshal(v); return b }
func TestGatedOracle(t *testing.T) {
	if _, err := gatedMembers(encoded(fixture())); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(map[string]any, map[string]any)
	}{
		{"running", func(s, c map[string]any) { c["state"] = map[string]any{"running": map[string]any{}} }},
		{"terminated", func(s, c map[string]any) { c["state"] = map[string]any{"terminated": map[string]any{"exitCode": 0}} }},
		{"restart", func(s, c map[string]any) { c["restartCount"] = 1 }},
		{"prior termination", func(s, c map[string]any) { c["lastState"] = map[string]any{"terminated": map[string]any{}} }},
		{"prior running", func(s, c map[string]any) { c["lastState"] = map[string]any{"running": map[string]any{}} }},
		{"started", func(s, c map[string]any) { c["started"] = true }},
		{"missing key", func(s, c map[string]any) { report(s)["message"] = `{"version":1}` }},
		{"invalid key", func(s, c map[string]any) { report(s)["message"] = `{"version":1,"publicKey":"bad"}` }},
		{"zero key", func(s, c map[string]any) {
			report(s)["message"] = `{"version":1,"publicKey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`
		}},
		{"duplicate field", func(s, c map[string]any) {
			report(s)["message"] = `{"version":1,"version":1,"publicKey":"AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`
		}},
		{"oversize", func(s, c map[string]any) { report(s)["message"] = strings.Repeat(" ", 257) }},
		{"case alias", func(s, c map[string]any) {
			report(s)["message"] = `{"Version":1,"publicKey":"AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`
		}},
		{"noncanonical key", func(s, c map[string]any) {
			report(s)["message"] = `{"version":1,"publicKey":"AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"}`
		}},
		{"last started", func(s, c map[string]any) { c["lastState"] = map[string]any{"started": true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture()
			s := f["items"].([]any)[0].(map[string]any)["status"].(map[string]any)
			c := s["containerStatuses"].([]any)[0].(map[string]any)
			tc.edit(s, c)
			if _, err := gatedMembers(encoded(f)); err == nil {
				t.Fatal("unsafe gate accepted")
			}
		})
	}
}
func report(s map[string]any) map[string]any {
	return s["initContainerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)["terminated"].(map[string]any)
}
func TestCommittedPlanOracle(t *testing.T) {
	pods := encoded(fixture())
	members, err := gatedMembers(pods)
	if err != nil {
		t.Fatal(err)
	}
	list := []observedMember{}
	for _, m := range members {
		list = append(list, m)
	}
	cluster := func(ms []observedMember) []byte {
		return encoded(map[string]any{"status": map[string]any{"bootstrapPlan": map[string]any{"members": ms}, "genesis": map[string]any{}, "applicationInit": map[string]any{}}})
	}
	if err := comparePlan(pods, cluster(list)); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"UID", "key", "node", "endpoint"} {
		t.Run(field, func(t *testing.T) {
			changed := append([]observedMember{}, list...)
			switch field {
			case "UID":
				changed[0].PodUID = "foreign"
			case "key":
				changed[0].Member.PublicKey = list[1].Member.PublicKey
			case "node":
				changed[0].Member.ID = "foreign"
			case "endpoint":
				changed[0].Member.PeerURL = "quic://10.0.0.99:9090"
			}
			if comparePlan(pods, cluster(changed)) == nil {
				t.Fatal("changed mapping accepted")
			}
		})
	}
}
