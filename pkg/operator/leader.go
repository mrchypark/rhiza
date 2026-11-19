package operator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	operatorLeaseDuration  = 15 * time.Second
	operatorRenewDeadline  = 10 * time.Second
	operatorRetryPeriod    = 2 * time.Second
	kubernetesMicroTimeFmt = "2006-01-02T15:04:05.000000Z"
)

type operatorLease struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name, Namespace, ResourceVersion string
	} `json:"metadata"`
	Spec struct {
		HolderIdentity       string `json:"holderIdentity"`
		LeaseDurationSeconds int32  `json:"leaseDurationSeconds"`
		AcquireTime          string `json:"acquireTime"`
		RenewTime            string `json:"renewTime"`
	} `json:"spec"`
}

// RunLeaderElection keeps leader active only while this process holds the
// namespace-scoped Lease. Losing the lease cancels reconciliation so a split
// brain cannot continue issuing mutation requests.
func RunLeaderElection(ctx context.Context, kube *Kubernetes, identity, leaseName string, leader func(context.Context) error) error {
	if kube == nil || kube.Namespace == "" || strings.TrimSpace(identity) == "" || len(identity) > 128 || !v1alpha2Name.MatchString(leaseName) || leader == nil {
		return fmt.Errorf("invalid operator leader election configuration")
	}
	base := "/apis/coordination.k8s.io/v1/namespaces/" + kube.Namespace + "/leases"
	path := base + "/" + leaseName
	var leaderCancel context.CancelFunc
	var leaderDone chan error
	defer func() {
		if leaderCancel != nil {
			leaderCancel()
			<-leaderDone
		}
	}()
	lastRenew := time.Time{}
	for {
		acquired, err := renewOperatorLease(ctx, kube, base, path, leaseName, identity)
		if err != nil && leaderCancel == nil {
			log.Printf("operator leader election retry: %v", err)
		}
		if err == nil && acquired {
			lastRenew = time.Now()
			if leaderCancel == nil {
				leaderCtx, cancel := context.WithCancel(ctx)
				leaderCancel = cancel
				leaderDone = make(chan error, 1)
				go func() { leaderDone <- leader(leaderCtx) }()
			}
		} else if leaderCancel != nil && (err != nil || !acquired) && time.Since(lastRenew) >= operatorRenewDeadline {
			leaderCancel()
			leaderCancel = nil
			if err != nil {
				return fmt.Errorf("operator lost leadership: %w", err)
			}
			return fmt.Errorf("operator lost leadership")
		}
		if leaderCancel != nil {
			select {
			case err := <-leaderDone:
				leaderCancel = nil
				return err
			default:
			}
		}
		timer := time.NewTimer(operatorRetryPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case err := <-leaderDone:
			timer.Stop()
			leaderCancel = nil
			return err
		case <-timer.C:
		}
	}
}

func renewOperatorLease(ctx context.Context, kube *Kubernetes, base, path, name, identity string) (bool, error) {
	var lease operatorLease
	err := kube.Get(ctx, path, &lease)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		now := time.Now().UTC().Format(kubernetesMicroTimeFmt)
		want := operatorLease{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		want.Metadata.Name, want.Metadata.Namespace = name, kube.Namespace
		want.Spec.HolderIdentity, want.Spec.LeaseDurationSeconds = identity, int32(operatorLeaseDuration/time.Second)
		want.Spec.AcquireTime, want.Spec.RenewTime = now, now
		if err := kube.Post(ctx, base, want, nil); err != nil {
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if lease.APIVersion != "coordination.k8s.io/v1" || lease.Kind != "Lease" || lease.Metadata.Name != name || lease.Metadata.Namespace != kube.Namespace || lease.Metadata.ResourceVersion == "" {
		return false, fmt.Errorf("invalid operator Lease identity")
	}
	now := time.Now().UTC()
	renew, err := time.Parse(time.RFC3339Nano, lease.Spec.RenewTime)
	if err != nil {
		return false, fmt.Errorf("invalid operator Lease renewTime")
	}
	duration := time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second
	held := lease.Spec.HolderIdentity == identity
	expired := lease.Spec.HolderIdentity == "" || duration <= 0 || now.After(renew.Add(duration))
	if !held && !expired {
		return false, nil
	}
	if held {
		lease.Spec.RenewTime = now.Format(kubernetesMicroTimeFmt)
	} else {
		lease.Spec.HolderIdentity = identity
		lease.Spec.AcquireTime = now.Format(kubernetesMicroTimeFmt)
		lease.Spec.RenewTime = lease.Spec.AcquireTime
	}
	lease.Spec.LeaseDurationSeconds = int32(operatorLeaseDuration / time.Second)
	var written operatorLease
	if err := kube.Put(ctx, path, lease, &written); err != nil {
		return false, err
	}
	if written.APIVersion != "coordination.k8s.io/v1" || written.Kind != "Lease" || written.Metadata.Name != name || written.Metadata.Namespace != kube.Namespace || written.Spec.HolderIdentity != identity || written.Metadata.ResourceVersion == lease.Metadata.ResourceVersion {
		return false, fmt.Errorf("operator Lease readback mismatch")
	}
	return true, nil
}
