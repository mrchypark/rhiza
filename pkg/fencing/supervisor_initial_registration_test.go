package fencing

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type initialStartupAuthorityFixture struct {
	completionAuthority
	identity      Identity
	grant         StartupGrant
	initialCalls  int
	ordinaryCalls int
}

func (a *initialStartupAuthorityFixture) RegisterStartup(context.Context, Identity) (StartupGrant, error) {
	a.ordinaryCalls++
	return StartupGrant{}, errors.New("ordinary registration must not handle initial preflight")
}

func (a *initialStartupAuthorityFixture) RegisterInitialStartup(_ context.Context, identity Identity) (StartupGrant, error) {
	a.initialCalls++
	if identity != a.identity {
		return StartupGrant{}, errors.New("initial registration identity changed")
	}
	return a.grant, nil
}

type ordinaryOnlyStartupAuthorityFixture struct {
	completionAuthority
	calls int
}

func (a *ordinaryOnlyStartupAuthorityFixture) RegisterStartup(context.Context, Identity) (StartupGrant, error) {
	a.calls++
	return StartupGrant{}, nil
}

func TestSupervisorInitialRegistrationUsesOnlyExplicitAuthorityRoute(t *testing.T) {
	identity := testIdentity()
	identity.NodeID, identity.PodName, identity.RunID = "rhiza-2", "rhiza-2", "initial-run"
	provenance := InitialRegistrationProvenance{
		Version: InitialRegistrationProvenanceVersion, IdentityDigest: InitialRegistrationIdentityDigest(identity),
		MembershipHash: strings.Repeat("a", 64), StoreScopeHash: strings.Repeat("b", 64), RunIdentity: identity,
	}
	authority := &initialStartupAuthorityFixture{identity: identity, grant: StartupGrant{
		Identity: identity, Role: StartupRoleVoterActive, InitialRegistration: &provenance,
	}}
	supervisor := &Supervisor{Authority: authority, Identity: identity}
	grant, err := supervisor.registerStartup(t.Context(), authority, StartupPreflightResult{InitialRegistration: true})
	if err != nil {
		t.Fatalf("initial-registration preflight did not reach its explicit authority route: %v", err)
	}
	if err := grant.Validate(identity); err != nil {
		t.Fatalf("authority returned an invalid initial grant: %v", err)
	}
	if authority.initialCalls != 1 || authority.ordinaryCalls != 0 {
		t.Fatalf("initial preflight used the wrong authority route: initial=%d ordinary=%d", authority.initialCalls, authority.ordinaryCalls)
	}
}

func TestSupervisorInitialRegistrationFailsClosedWithoutExplicitAuthority(t *testing.T) {
	identity := testIdentity()
	authority := &ordinaryOnlyStartupAuthorityFixture{}
	supervisor := &Supervisor{Authority: authority, Identity: identity}
	_, err := supervisor.registerStartup(t.Context(), authority, StartupPreflightResult{InitialRegistration: true})
	if err == nil || authority.calls != 0 {
		t.Fatalf("initial preflight fell through to ordinary registration: err=%v ordinaryCalls=%d", err, authority.calls)
	}
}

func TestSupervisorInitialRegistrationRejectsOrdinaryGrant(t *testing.T) {
	identity := testIdentity()
	identity.NodeID, identity.PodName = "rhiza-0", "rhiza-0"
	authority := &initialStartupAuthorityFixture{identity: identity, grant: StartupGrant{
		Identity: identity, Role: StartupRoleVoterActive,
	}}
	supervisor := &Supervisor{Authority: authority, Identity: identity}
	if _, err := supervisor.registerStartup(t.Context(), authority, StartupPreflightResult{InitialRegistration: true}); err == nil {
		t.Fatal("ordinary active-voter grant substituted for first-registration evidence")
	}
}
