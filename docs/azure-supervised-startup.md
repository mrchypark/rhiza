# Azure storage with supervised fencing

Provision the Azure container before starting a Rhiza voter in legacy
cooperative fencing mode. The supervised startup constructor neither checks
whether the container exists nor creates it. Successful client construction is
not evidence that the container exists; a later storage operation can fail if
the container is missing.

This restriction keeps the never-bound process-exit proof sound: a child that
never obtained WAL admission must not have issued a remote storage mutation.
An Azure container-create response can be lost, so attempting that mutation and
then discarding its failed constructor is not evidence of storage quiescence.
Ordinary unmanaged startup through the provisioning constructor retains Azure
auto-provisioning. Normal managed HA uses a separate namespace-bound
constructor that also disables auto-provisioning, without requiring a legacy
fencing supervisor. Provision the Azure container before managed startup as well.

The regression uses the pinned real Azure provider/SDK against a loopback TLS
endpoint. It contrasts ordinary constructor provisioning with supervised zero
create requests. The supervised `Node.Open` regression fails at a deliberately
rejected WAL bind and checks that no remote mutation was attempted before it;
it does not prove that the constructor rejects a missing container. These tests
do not constitute real Azure or GCS qualification.
