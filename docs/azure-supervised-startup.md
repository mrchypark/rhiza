# Azure storage with supervised fencing

Provision the Azure container before starting a supervised Rhiza voter. In the
cooperative Kubernetes fencing mode, startup constructs the Azure client without
the provider's automatic container creation. A missing container causes startup
to fail; it is not created by the application before WAL admission.

This restriction keeps the never-bound process-exit proof sound: a child that
never obtained WAL admission must not have issued a remote storage mutation.
An Azure container-create response can be lost, so attempting that mutation and
then discarding its failed constructor is not evidence of storage quiescence.
The restriction does not change ordinary, unsupervised Azure auto-provisioning.
Authentication, mutation tracking, replay observers, and provider transport/retry
configuration use the same constructor path in both modes.

The regression uses the pinned real Azure provider/SDK against a loopback TLS
endpoint. It contrasts ordinary constructor provisioning with supervised zero
create requests, and checks real supervised `Node.Open` through a rejected WAL
bind. These tests do not constitute real Azure or GCS qualification.
