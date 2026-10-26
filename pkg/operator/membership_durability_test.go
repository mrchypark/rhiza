package operator

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestControllerMembershipCompletionWaitsForDurabilityAck(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	seedLegacyRemoveMembershipJournal(r)
	var seededRemove network.MembershipChange
	if err := json.Unmarshal(jsonBytes(r.Status.Membership.RemoveRequest), &seededRemove); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	} // replay journaled removal
	if len(api.changes) != 1 || !sameJSON(api.changes[0], seededRemove) {
		t.Fatalf("legacy REMOVE was not replayed verbatim: %+v", api.changes)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	} // confirm removal before journaling the add
	if len(api.changes) != 2 || !sameJSON(api.changes[1], seededRemove) {
		t.Fatalf("committed REMOVE persistence was not confirmed before ADD: %+v", api.changes)
	}
	if r.Status.Membership.Add == nil || r.Status.Membership.Phase != "Adding" {
		t.Fatalf("add was not journaled before dispatch: %+v", r.Status.Membership)
	}
	var observedConfig uint
	var staleRemoveCalls int
	c.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/membership/change" {
			return http.DefaultTransport.RoundTrip(req)
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var change network.MembershipChange
		if err := json.Unmarshal(data, &change); err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(data))
		if observedConfig > change.ExpectedConfigID+1 && change.Remove != "" {
			staleRemoveCalls++
			return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}
		return http.DefaultTransport.RoundTrip(req)
	})}

	committed := func(config uint) {
		voters := []quepaxa.NodeID{"n1", "n2", "n4"}
		api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: config, Voters: voters, Voting: true}
		api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: config, Voters: voters, Voting: true}
		api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: config, Voters: voters, WALIdentity: strings.Repeat("a", 64), Voting: true}
	}
	committed(3)
	observedConfig = 3
	api.changeStatus = http.StatusInternalServerError
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase == "Complete" || r.Status.Phase == "Complete" || len(api.changes) != 3 {
		t.Fatalf("voting membership completed without a successful persistence replay: status=%+v changes=%d", r.Status, len(api.changes))
	}
	member := voterMember("source", "n4", "new-token")
	member.WALIdentity = strings.Repeat("a", 64)
	want := network.MembershipChange{OperationID: membershipOperationID(r, "add"), ClusterID: "source", ExpectedConfigID: 2, Add: &member}
	if !sameJSON(api.changes[2], want) {
		t.Fatalf("persistence replay payload=%+v want exact journaled request %+v", api.changes[2], want)
	}

	// A later, different configuration is not evidence that this journaled add
	// received its durability acknowledgement.
	committed(4)
	observedConfig = 4
	api.changeStatus = 0
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase == "Complete" || len(api.changes) != 3 || staleRemoveCalls != 0 {
		t.Fatalf("different config bypassed completion guard: status=%+v changes=%d", r.Status, len(api.changes))
	}

	committed(3)
	observedConfig = 3
	api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, WALIdentity: strings.Repeat("b", 64), Voting: true}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase == "Complete" || staleRemoveCalls != 0 || len(api.changes) != 3 {
		t.Fatalf("wrong learner identity bypassed completion guard: status=%+v changes=%d stale-removes=%d", r.Status, len(api.changes), staleRemoveCalls)
	}
	committed(3)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Complete" || len(api.changes) != 4 || !sameJSON(api.changes[2], api.changes[3]) || staleRemoveCalls != 0 {
		t.Fatalf("exact committed request did not complete after persistence acknowledgement: status=%+v changes=%+v", r.Status, api.changes)
	}
}

func TestControllerRemovalDurabilityAckPrecedesReplacementAdd(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	seedLegacyRemoveMembershipJournal(r)
	var seededRemove network.MembershipChange
	if err := json.Unmarshal(jsonBytes(r.Status.Membership.RemoveRequest), &seededRemove); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 1 || !sameJSON(api.changes[0], seededRemove) {
		t.Fatalf("legacy REMOVE was not replayed verbatim: %+v", api.changes)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	api.changeStatus = http.StatusInternalServerError
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Removing" || r.Status.Membership.Add != nil || len(api.changes) != 2 || !sameJSON(api.changes[1], seededRemove) {
		t.Fatalf("replacement advanced before removal persistence acknowledgement: membership=%+v changes=%+v", r.Status.Membership, api.changes)
	}
	api.changeStatus = 0
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Adding" || r.Status.Membership.Add == nil || len(api.changes) != 3 || !sameJSON(api.changes[0], seededRemove) || !sameJSON(api.changes[1], seededRemove) || !sameJSON(api.changes[2], seededRemove) {
		t.Fatalf("exact removal replay did not unlock replacement journaling: membership=%+v changes=%+v", r.Status.Membership, api.changes)
	}
}

func TestControllerAbortedCommittedAdditionWaitsForDurabilityAck(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	seedLegacyRemoveMembershipJournal(r)
	var seededRemove network.MembershipChange
	if err := json.Unmarshal(jsonBytes(r.Status.Membership.RemoveRequest), &seededRemove); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 1 || !sameJSON(api.changes[0], seededRemove) {
		t.Fatalf("legacy REMOVE was not replayed verbatim: %+v", api.changes)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 2 || !sameJSON(api.changes[1], seededRemove) || r.Status.Membership.Add == nil {
		t.Fatalf("REMOVE durability was not confirmed before ADD journaling: membership=%+v changes=%+v", r.Status.Membership, api.changes)
	}
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
	api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, WALIdentity: strings.Repeat("a", 64), Voting: true}
	if err := writeRetirementOperation(ctx, c, r); err != nil {
		t.Fatal(err)
	}
	r.Spec.Membership.AbortAddition = true
	api.changeStatus = http.StatusInternalServerError
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Aborting" || r.Status.Phase == "Complete" || len(api.changes) != 3 {
		t.Fatalf("committed addition completed without persistence acknowledgement: status=%+v changes=%+v", r.Status, api.changes)
	}
	want, err := c.retiringMembershipRequest(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(api.changes[2], want) {
		t.Fatalf("committed add replay=%+v want exact request %+v", api.changes[2], want)
	}
	api.changeStatus = 0
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Complete" || len(api.changes) != 4 || !sameJSON(api.changes[2], api.changes[3]) {
		t.Fatalf("exact committed add replay did not complete abort: status=%+v changes=%+v", r.Status, api.changes)
	}
}

func sameJSON(a, b any) bool {
	return string(jsonBytes(a)) == string(jsonBytes(b))
}
