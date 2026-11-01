package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func run(args []string) error {
	if len(args) > 0 && args[0] == "serve-managed-sql" {
		return runServe(args[1:])
	}
	if len(args) > 0 && args[0] == "check-ready" {
		return runCheckReady(args[1:])
	}
	if len(args) > 0 && args[0] == "await-bootstrap-plan" {
		return runAwait(args[1:])
	}
	if len(args) == 0 || args[0] != "prepare-identity" {
		return errors.New("expected prepare-identity command")
	}
	flags := flag.NewFlagSet("prepare-identity", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	crUID := flags.String("cr-uid", "", "")
	generation := flags.Int64("generation", -1, "")
	podUID := flags.String("pod-uid", "", "")
	identityPath := flags.String("identity-file", "/var/run/rhiza/identity.json", "")
	messagePath := flags.String("termination-message", "/dev/termination-log", "")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return errors.New("invalid prepare-identity arguments")
	}
	cluster, err := types.ManagedClusterID(*crUID, *generation)
	if err != nil {
		return err
	}
	node, err := types.ManagedNodeID(*podUID)
	if err != nil {
		return err
	}
	if *identityPath == "" || *messagePath == "" || *identityPath == *messagePath {
		return errors.New("distinct identity and termination paths required")
	}
	token, err := prepareIdentityFile(*identityPath, string(cluster), string(node))
	if err != nil {
		return err
	}
	public := rhiza.PeerPublicKey(string(cluster), string(node), token)
	report, err := json.Marshal(struct {
		Version   int             `json:"version"`
		PublicKey rhiza.PublicKey `json:"publicKey"`
	}{1, public})
	if err != nil || len(report) > 256 {
		return errors.New("invalid public identity report")
	}
	identityInfo, err := os.Stat(*identityPath)
	if err != nil {
		return err
	}
	messageInfo, err := os.Stat(*messagePath)
	if err == nil && os.SameFile(identityInfo, messageInfo) {
		return errors.New("termination message aliases identity file")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(*messagePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open termination message: %w", err)
	}
	_, writeErr := f.Write(report)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	return errors.Join(writeErr, f.Close())
}

func runServe(args []string) error {
	flags := flag.NewFlagSet("serve-managed-sql", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	profilePath := flags.String("runtime-profile", operator.V1Alpha2ManagedRuntimeProfilePath, "")
	listen := flags.String("listen", "127.0.0.1:8080", "")
	identity := flags.String("identity-file", operator.V1Alpha2BootstrapIdentityPath, "")
	handoff := flags.String("handoff-file", operator.V1Alpha2BootstrapHandoffPath, "")
	repairHandoff := flags.String("repair-handoff-file", repairHandoffPath, "")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("invalid serve-managed-sql arguments")
	}
	if host, port, err := net.SplitHostPort(*listen); err != nil || net.ParseIP(host) == nil || port != "8080" {
		return errors.New("invalid managed SQL listen address")
	}
	ref := operator.V1Alpha2ClusterRef{Name: os.Getenv("RHIZA_CLUSTER_NAME"), Namespace: os.Getenv("POD_NAMESPACE"), UID: os.Getenv("RHIZA_CLUSTER_UID")}
	pod, podUID := os.Getenv("POD_NAME"), os.Getenv("POD_UID")
	if ref.Name == "" || ref.Namespace == "" || ref.UID == "" || pod == "" || podUID == "" {
		return errors.New("managed execution identity required")
	}
	f, err := os.Open(*profilePath)
	if err != nil {
		return errors.New("managed profile unavailable")
	}
	profile, err := operator.DecodeV1Alpha2RuntimeProfile(f)
	err = errors.Join(err, f.Close())
	if err != nil {
		return errors.New("invalid managed profile")
	}
	kube, err := operator.NewInCluster(ref.Namespace)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := managedConfig(ctx, kube, ref, pod, podUID, *identity, *handoff, profile)
	cleanup := func() error { return nil }
	if errors.Is(err, errLearnerAdmissionPending) {
		cfg, cleanup, err = managedRepairConfig(ctx, kube, ref, pod, podUID, *identity, *handoff, *repairHandoff, profile)
	}
	if err != nil {
		return err
	}
	return errors.Join(serveManaged(ctx, cfg, *listen), cleanup())
}

func runCheckReady(args []string) error {
	flags := flag.NewFlagSet("check-ready", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	profilePath := flags.String("runtime-profile", operator.V1Alpha2ManagedRuntimeProfilePath, "")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("invalid check-ready arguments")
	}
	f, err := os.Open(*profilePath)
	if err != nil {
		return errors.New("managed profile unavailable")
	}
	profile, err := operator.DecodeV1Alpha2RuntimeProfile(f)
	err = errors.Join(err, f.Close())
	if err != nil {
		return errors.New("invalid managed profile")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return checkReady(ctx, client, "http://127.0.0.1:8080/readyz", profile.Storage.AdminToken)
}

func runAwait(args []string) error {
	flags := flag.NewFlagSet("await-bootstrap-plan", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	uid := flags.String("cr-uid", "", "")
	generation := flags.Int64("generation", -1, "")
	podUID := flags.String("pod-uid", "", "")
	ns := flags.String("namespace", os.Getenv("POD_NAMESPACE"), "")
	name := flags.String("cluster-name", os.Getenv("RHIZA_CLUSTER_NAME"), "")
	pod := flags.String("pod-name", os.Getenv("POD_NAME"), "")
	identity := flags.String("identity-file", "/var/lib/rhiza/identity.json", "")
	output := flags.String("handoff-file", "/var/lib/rhiza/bootstrap.json", "")
	repairOutput := flags.String("repair-handoff-file", repairHandoffPath, "")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *ns == "" || *name == "" || *pod == "" || *identity == "" || *output == "" || filepath.Clean(*identity) == filepath.Clean(*output) {
		return errors.New("invalid await-bootstrap-plan arguments")
	}
	kube, err := operator.NewInCluster(*ns)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	return awaitBootstrapWithRepair(ctx, kube, operator.V1Alpha2ClusterRef{Name: *name, Namespace: *ns, UID: *uid}, *generation, *pod, *podUID, *identity, *output, *repairOutput)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
