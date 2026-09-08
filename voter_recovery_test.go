package rhiza_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func TestS3VoterIdentityRejectsLostDiskBeforePeerStart(t *testing.T) {
	endpoint, bucket := os.Getenv("RHIZA_E2E_S3_ENDPOINT"), os.Getenv("RHIZA_E2E_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("RHIZA_E2E_S3_ENDPOINT and RHIZA_E2E_S3_BUCKET are required")
	}
	for _, durability := range []rhiza.ObjectStoreDurability{rhiza.ObjectStoreDurabilityAsync, rhiza.ObjectStoreDurabilityBeforeAck} {
		t.Run(string(durability), func(t *testing.T) {
			peerAddr := freeUDPAddr(t)
			members := []rhiza.Member{
				{ID: "n1", PeerURL: "quic://" + peerAddr, Token: "voter-1"},
				{ID: "n2", PeerURL: "quic://" + freeUDPAddr(t), Token: "voter-2"},
				{ID: "n3", PeerURL: "quic://" + freeUDPAddr(t), Token: "voter-3"},
			}
			originDir := t.TempDir()
			config := rhiza.Config{
				ClusterID: fmt.Sprintf("voter-recovery-%d", time.Now().UnixNano()), NodeID: "n1", DataDir: originDir,
				PeerAddr: peerAddr, Members: members, ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint,
				ObjStoreBucket: bucket, ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
				ObjStoreAccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"), ObjStoreSecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
				ObjStoreDurability: durability,
			}
			voter, err := rhiza.Open(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := voter.Close(); err != nil {
				t.Fatal(err)
			}
			voter, err = rhiza.Open(context.Background(), config)
			if err != nil {
				t.Fatalf("original voter restart: %v", err)
			}
			if err := voter.Close(); err != nil {
				t.Fatal(err)
			}

			blocker, err := net.ListenPacket("udp", peerAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Close()
			lost := config
			lost.DataDir = t.TempDir()
			if _, err := rhiza.Open(context.Background(), lost); !errors.Is(err, rhiza.ErrVoterStateLost) {
				t.Fatalf("fresh voter disk error=%v, want ErrVoterStateLost before peer bind", err)
			}

			qlogDir := filepath.Join(originDir, "qlog")
			for _, entry := range mustReadDir(t, qlogDir) {
				if strings.HasPrefix(entry.Name(), "manifest_") || strings.HasPrefix(entry.Name(), "seg_") {
					if err := os.Remove(filepath.Join(qlogDir, entry.Name())); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := os.Stat(filepath.Join(qlogDir, "voter-identity.json")); err != nil {
				t.Fatalf("test removed voter identity sidecar: %v", err)
			}
			if _, err := rhiza.Open(context.Background(), config); !errors.Is(err, rhiza.ErrVoterStateLost) {
				t.Fatalf("sidecar-only voter disk error=%v, want ErrVoterStateLost", err)
			}
		})
	}
}

func mustReadDir(t testing.TB, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
