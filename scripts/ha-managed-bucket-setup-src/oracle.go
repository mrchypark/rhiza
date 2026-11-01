package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
)

// Reject duplicate object fields before typed decoding (including nested records).
func uniqueValue(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		seen := map[string]bool{}
		for d.More() {
			if delim == '{' {
				k, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := k.(string)
				if !ok || seen[name] {
					return errors.New("duplicate JSON field")
				}
				seen[name] = true
			}
			if err := uniqueValue(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}

func decodeUnique(data []byte, dst any, strict bool) error {
	d := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	if strict {
		d.DisallowUnknownFields()
	}
	return d.Decode(dst)
}

type observedMember struct {
	PodUID string `json:"podUID"`
	Member struct {
		ID        string `json:"node_id"`
		PeerURL   string `json:"peer_url"`
		PublicKey string `json:"public_key"`
	} `json:"member"`
}
type containerState struct{ Running, Terminated, Waiting, Started json.RawMessage }

func present(b json.RawMessage) bool { return len(b) > 0 && string(b) != "null" }

type containerStatus struct {
	Name             string
	State, LastState containerState
	RestartCount     int
	Started          bool
}

func gatedMembers(data []byte) (map[string]observedMember, error) {
	var list struct {
		Items []struct {
			Metadata struct{ UID string }
			Spec     struct{ Containers []struct{ Name string } }
			Status   struct {
				PodIP                 string
				InitContainerStatuses []struct {
					Name  string
					State struct {
						Running    json.RawMessage
						Terminated *struct {
							ExitCode int
							Message  string
						}
					}
				}
				ContainerStatuses []containerStatus
			}
		}
	}
	if len(data) > 4<<20 || decodeUnique(data, &list, false) != nil || len(list.Items) != 3 {
		return nil, errors.New("expected three bounded kubelet Pods")
	}
	result := map[string]observedMember{}
	keys := map[string]bool{}
	endpoints := map[string]bool{}
	for _, p := range list.Items {
		if p.Metadata.UID == "" || result[p.Metadata.UID].PodUID != "" || net.ParseIP(p.Status.PodIP) == nil || len(p.Spec.Containers) != 1 {
			return nil, errors.New("invalid actual Pod identity")
		}
		if len(p.Status.ContainerStatuses) > 1 {
			return nil, errors.New("unexpected main status")
		}
		for _, s := range p.Status.ContainerStatuses {
			if s.Name != p.Spec.Containers[0].Name || s.RestartCount != 0 || s.Started || present(s.State.Running) || present(s.State.Terminated) || present(s.LastState.Running) || present(s.LastState.Terminated) || present(s.LastState.Started) {
				return nil, errors.New("main has already started")
			}
		}
		var key string
		prepared, waiting := 0, 0
		for _, s := range p.Status.InitContainerStatuses {
			if s.Name == "rhiza-prepare-identity" {
				prepared++
				if s.State.Terminated == nil || s.State.Terminated.ExitCode != 0 || len(s.State.Terminated.Message) > 256 {
					return nil, errors.New("prepare not successfully terminated")
				}
				var report struct {
					Version   int    `json:"version"`
					PublicKey string `json:"publicKey"`
				}
				var fields map[string]json.RawMessage
				if json.Unmarshal([]byte(s.State.Terminated.Message), &fields) != nil || len(fields) != 2 || fields["version"] == nil || fields["publicKey"] == nil {
					return nil, errors.New("public report requires exact fields")
				}
				if decodeUnique([]byte(s.State.Terminated.Message), &report, true) != nil || report.Version != 1 {
					return nil, errors.New("invalid public report")
				}
				b, err := base64.StdEncoding.Strict().DecodeString(report.PublicKey)
				if err != nil || len(b) != 32 || bytes.Equal(b, make([]byte, 32)) || base64.StdEncoding.EncodeToString(b) != report.PublicKey {
					return nil, errors.New("invalid canonical public key")
				}
				key = report.PublicKey
			}
			if s.Name == "rhiza-await-bootstrap-plan" && present(s.State.Running) {
				waiting++
			}
		}
		if prepared != 1 || waiting != 1 {
			return nil, errors.New("missing prepare or running gate")
		}
		m := observedMember{PodUID: p.Metadata.UID}
		m.Member.ID = fmt.Sprintf("rhiza-n-%x", sha256.Sum256([]byte("rhiza-managed-node-v1\x00"+p.Metadata.UID)))
		m.Member.PeerURL = "quic://" + net.JoinHostPort(net.ParseIP(p.Status.PodIP).String(), "9090")
		m.Member.PublicKey = key
		if keys[key] || endpoints[m.Member.PeerURL] {
			return nil, errors.New("duplicate key or endpoint")
		}
		keys[key] = true
		endpoints[m.Member.PeerURL] = true
		result[m.PodUID] = m
	}
	return result, nil
}

func comparePlan(pods, cluster []byte) error {
	want, err := gatedMembers(pods)
	if err != nil {
		return err
	}
	var c struct {
		Status struct {
			BootstrapPlan            struct{ Members []observedMember }
			Genesis, ApplicationInit json.RawMessage
		}
	}
	if decodeUnique(cluster, &c, false) != nil || len(c.Status.BootstrapPlan.Members) != 3 || !present(c.Status.Genesis) || !present(c.Status.ApplicationInit) {
		return errors.New("missing committed plan")
	}
	got := map[string]observedMember{}
	for _, m := range c.Status.BootstrapPlan.Members {
		if _, exists := got[m.PodUID]; exists {
			return errors.New("duplicate committed Pod UID")
		}
		got[m.PodUID] = m
	}
	if !reflect.DeepEqual(want, got) {
		return errors.New("committed member mapping differs from captured kubelet reports")
	}
	return nil
}

func runOracle(args []string) error {
	if len(args) != 2 && len(args) != 3 {
		return errors.New("oracle expects gated Pods and optional committed cluster")
	}
	b, err := os.ReadFile(args[1])
	if err != nil {
		return err
	}
	if args[0] == "oracle-gated" && len(args) == 2 {
		_, err = gatedMembers(b)
		return err
	}
	if args[0] == "oracle-plan" && len(args) == 3 {
		c, err := os.ReadFile(args[2])
		if err != nil {
			return err
		}
		return comparePlan(b, c)
	}
	return errors.New("invalid oracle arguments")
}
