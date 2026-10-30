package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
)

func run(args []string) error {
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

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
