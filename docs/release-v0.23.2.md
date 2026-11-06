# Rhiza v0.23.2

This patch updates current documentation and synchronizes the Rust crate and
installation examples to 0.23.2. It does not change engine behavior, SDK APIs,
storage formats, dependencies or CI policy.

## Documentation scope

- Clarify Local init Open/Close followed by main-process reopen of the same
  DataDir, ordinary embedded Db ownership, and the separate managed Host loop.
- Separate genesis operator, healthy-quorum Host repair and certified engine
  membership responsibilities. Normal HA needs no separate always-on fencer or
  external lease service.
- Mark retired alpha1/StatefulSet/fencer guides as historical and retain their
  v0.23.1 originals through fixed-commit links. Automatic generation DR remains
  unimplemented and unqualified; queue/status bookkeeping is not execution.
- Preserve independent termination, recreation prevention, outstanding storage
  work, ACK-source coverage and live activation-authority requirements.
- Distinguish automated six-lane HA qualification from the separate opt-in
  manual live-cluster driver. Pod deletion is not process-termination evidence.

The documentation baseline is
[`7b5654977051760bd85cee3845742870d83be9a4`](https://github.com/mrchypark/rhiza/commit/7b5654977051760bd85cee3845742870d83be9a4).
Historical CI links retain their original source scope; they do not qualify
this version commit, a subsequent main merge or published 0.23.2 artifacts.
New-head checks and release-artifact verification are still required. These
version edits alone do not establish a completed release or a performance gain.

## Regression-fixture correction

The first candidate CI exposed an unguaranteed test precondition: a successful
two-of-three recorder quorum did not necessarily include the node whose
unapplied recorder state the test inspected. The fixture now holds the exact
original Record on the third node until the target participates, checks the
actual certificate recorder set, and retains the original recovery assertions.
The Record boundary uses the existing build-tagged test-hook mechanism and is
inactive in default builds. This is not a change to the QuePaxa quorum policy
or evidence that the historical run's exact RPC cancellation order is known.

The Rust managed qualification client also consumes SQL `commit_unknown` through
one exact-original recovery call. It verifies the returned SQL request ID and
committed receipt, slot, retention bound and expected result without executing
the mutation again. Missing, expired, conflicting or still-unknown outcomes
remain failures. Initial UNKNOWN and recovery observations are recorded
separately; the zero-container-restart qualification remains unchanged. This
corrects the client's handling of a valid API outcome, not a claim that the
historical UNKNOWN's internal cause was reconstructed or eliminated.

Bootstrap setup now requires committed receipts with valid slot/retention
metadata for its initial SQL mutations, and checks the INSERT effect explicitly.
Exact-row and zero-restart checks remain unchanged; failures identify the
phase, check and diagnostic artifact without logging request payloads.

The prepared-checkpoint membership fixture waits for each active voter to learn
the fixed certified cut and compares its exact prefix, config and membership
before preparing. Its delayed-voter regression verifies the original mismatch
refusal and subsequent preparation after real certified delivery. Production
seal validation is unchanged; historical mismatch details are not reconstructed.
