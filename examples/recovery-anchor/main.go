package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	objstorecfg "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// readEvidence decodes one trust_state row and closes rows.
func readEvidence(rows *sql.Rows) ([]byte, error) {
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("trust_state: no rows returned")
	}
	var epoch int64
	var token string
	if err := rows.Scan(&epoch, &token); err != nil {
		return nil, fmt.Errorf("scan trust_state: %w", err)
	}
	if rows.Next() {
		return nil, fmt.Errorf("trust_state: expected exactly one row")
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("trust_state rows: %w", err)
	}
	if token == "" {
		return nil, fmt.Errorf("trust_state: empty token")
	}
	if epoch < 1 {
		return nil, fmt.Errorf("trust_state: invalid epoch %d", epoch)
	}
	canonical, err := json.Marshal(struct {
		Epoch int64  `json:"epoch"`
		Token string `json:"token"`
	}{epoch, token})
	if err != nil {
		return nil, fmt.Errorf("marshal evidence: %w", err)
	}
	return canonical, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	namespace := os.Getenv("RHIZA_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	storeCfg := objstorecfg.LoadConfig()
	bucket, err := objstorecfg.NewBucket(storeCfg)
	if err != nil {
		return fmt.Errorf("object store bucket: %w", err)
	}
	defer bucket.Close()

	backend := &operator.KubernetesAnchorBackend{Kube: kube}

	const evidenceFormat = "sql-epoch-token/v1"

	verifier := func(ctx context.Context, req recoveryanchor.Request, rec recoveryanchor.Record) (recoveryanchor.Binding, []byte, error) {
		if rec.PendingWrite {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("anchor still pending")
		}
		if rec.Version != recoveryanchor.Version1 {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("anchor version must be 1, got %d", rec.Version)
		}
		if rec.EvidenceFormat != evidenceFormat {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("schema mismatch: record %q, expected %q", rec.EvidenceFormat, evidenceFormat)
		}

		var targetAnchorHash [32]byte
		if decoded, decErr := hex.DecodeString(req.TargetAnchorHash); decErr != nil || len(decoded) != sha256.Size {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("invalid TargetAnchorHash: must be 64-char hex")
		} else {
			copy(targetAnchorHash[:], decoded)
		}

		sourceStorageID := hashStoreIdentity(storeCfg, req.SourceClusterID)

		if rec.Binding.ClusterID != req.SourceClusterID {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("binding cluster mismatch: record %q, request %q", rec.Binding.ClusterID, req.SourceClusterID)
		}
		if rec.Binding.StorageID != sourceStorageID {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("binding storage mismatch: record %q, computed %q", rec.Binding.StorageID, sourceStorageID)
		}

		if req.SourcePrefix != storagePrefix(storeCfg, req.SourceClusterID) {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("source prefix mismatch: request %q, expected %q", req.SourcePrefix, storagePrefix(storeCfg, req.SourceClusterID))
		}
		if req.TargetPrefix != storagePrefix(storeCfg, req.TargetClusterID) {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("target prefix mismatch: request %q, expected %q", req.TargetPrefix, storagePrefix(storeCfg, req.TargetClusterID))
		}

		var appEvidence []byte
		_, err := recovery.RecoverApplicationEvidence(
			ctx, bucket, req.TargetPrefix,
			recovery.EvidenceOptions{
				ExpectedAnchorHash:    targetAnchorHash,
				ExpectedMembership:    req.TargetMembership,
				ExpectedForkResult:    req.Fork,
				ExpectedSourcePrefix:  req.SourcePrefix,
				ExpectedOperationID:   req.OperationID,
			},
			func(ctx context.Context, mat *recovery.RecoveredMaterializer) error {
				rows, err := mat.Query(ctx, "SELECT epoch, token FROM trust_state WHERE id = 1")
				if err != nil {
					return fmt.Errorf("query trust_state: %w", err)
				}
				canonical, err := readEvidence(rows)
				if err != nil {
					return err
				}
				appEvidence = canonical
				return nil
			},
		)
		if err != nil {
			return recoveryanchor.Binding{}, nil, err
		}
		if len(appEvidence) == 0 {
			return recoveryanchor.Binding{}, nil, fmt.Errorf("no evidence produced")
		}

		targetBinding := recoveryanchor.Binding{
			ClusterID: req.TargetClusterID,
			StorageID: hashStoreIdentity(storeCfg, req.TargetClusterID),
		}
		return targetBinding, appEvidence, nil
	}

	coord := &recoveryanchor.Coordinator{
		Backend:        backend,
		EvidenceFormat: evidenceFormat,
		Verifier:       verifier,
	}

	tokenFile := os.Getenv("ANCHOR_TOKEN_FILE")
	tokenBytes, err := os.ReadFile(tokenFile)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))

	handler := recoveryanchor.NewHandler(coord, token)

	certFile := os.Getenv("ANCHOR_TLS_CERT")
	keyFile := os.Getenv("ANCHOR_TLS_KEY")
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("ANCHOR_TLS_CERT and ANCHOR_TLS_KEY are required")
	}

	server := &http.Server{
		Addr:              ":9191",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	done := make(chan error, 1)
	go func() { done <- server.ListenAndServeTLS(certFile, keyFile) }()
	log.Println("recovery-anchor service listening on :9191 (TLS)")

	select {
	case err := <-done:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
