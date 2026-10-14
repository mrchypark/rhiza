// Test-only application: SQL uses the embedded API, never db.Handler().
// Supports RHIZA_RECOVERY_ANCHOR_ID for external CAS anchor verification.
// Guard rereads every write, reserves pending before db.Execute, clears after.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/quic-go/quic-go"
)

var errAnchorBlocked = errors.New("anchor binding not verified")

func main() {
	if len(os.Args) > 1 && os.Args[1] == "qualification-unix-milli" {
		if len(os.Args) != 2 {
			log.Fatal("invalid Unix millisecond arguments")
		}
		fmt.Println(time.Now().UnixMilli())
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "qualification-archive-head" {
		if len(os.Args) != 2 {
			log.Fatal("invalid archive head arguments")
		}
		head, err := qualificationArchiveHead(os.Stdin)
		if err != nil {
			log.Fatalf("invalid archive head evidence: %v", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(head); err != nil {
			log.Fatalf("archive head output failed: %v", err)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "qualification-quic-probe" {
		if len(os.Args) == 3 && os.Args[2] == "hold" {
			time.Sleep(20 * time.Minute)
			return
		}
		if len(os.Args) != 5 || (os.Args[2] != "connected" && os.Args[2] != "blocked") || !regexp.MustCompile(`^rhiza-voter-[012]$`).MatchString(os.Args[3]) {
			log.Fatal("invalid QUIC probe arguments")
		}
		host, port, err := net.SplitHostPort(os.Args[4])
		if err != nil || net.ParseIP(host) == nil || port != "9090" {
			log.Fatal("literal voter IP and peer port required")
		}
		config, err := rhiza.ConfigFromEnv()
		if err != nil {
			log.Fatal("probe configuration invalid")
		}
		var target rhiza.Member
		for _, member := range config.Members {
			if string(member.ID) == os.Args[3] {
				target = member
			}
		}
		if target.PublicKey == (rhiza.PublicKey{}) {
			log.Fatal("probe peer identity absent")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = probeQUICHandshake(ctx, os.Args[4], string(target.ID), target.PublicKey)
		if os.Args[2] == "connected" && err == nil {
			fmt.Println("QUIC pinned handshake verified; no application stream")
			return
		}
		if os.Args[2] == "blocked" && errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("QUIC context deadline verified")
			return
		}
		log.Fatal("QUIC probe expectation failed")
	}
	if len(os.Args) > 1 && os.Args[1] == "qualification-fixture" {
		if len(os.Args) != 5 {
			log.Fatal("invalid qualification fixture arguments")
		}
		fixture, err := qualificationFixture(os.Args[2], os.Args[3], os.Args[4], rand.Reader)
		if err != nil {
			log.Fatal("qualification fixture generation failed")
		}
		// Only the trusted bootstrap may redirect this credential-bearing output.
		if json.NewEncoder(os.Stdout).Encode(fixture) != nil {
			log.Fatal("qualification fixture output failed")
		}
		return
	}
	config, err := rhiza.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := runApplication(ctx, config); err != nil {
		log.Fatal(err)
	}
}

type qualificationArchiveHeadEvidence struct {
	ConfigID          uint64   `json:"config_id"`
	Base              uint64   `json:"base"`
	BaseRootHash      string   `json:"base_root_hash"`
	BaseStateHash     string   `json:"base_state_hash"`
	BasePrefixHash    string   `json:"base_prefix_hash"`
	RegularCheckpoint bool     `json:"regular_checkpoint"`
	Tip               uint64   `json:"tip"`
	TailHash          [32]byte `json:"tail_hash"`
	TailObject        uint64   `json:"tail_object"`
}

func qualificationArchiveHead(input io.Reader) (qualificationArchiveHeadEvidence, error) {
	const header, crcSize, maxSize = 128, 4, 8388632
	data, err := io.ReadAll(io.LimitReader(input, maxSize+1))
	if err != nil || len(data) < header+crcSize || len(data) > maxSize || string(data[:8]) != "RHZAHD2!" || binary.BigEndian.Uint32(data[8:12]) != uint32(len(data)) {
		return qualificationArchiveHeadEvidence{}, errors.New("invalid archive head header")
	}
	flags := binary.BigEndian.Uint32(data[12:16])
	config, generation := binary.BigEndian.Uint64(data[16:24]), binary.BigEndian.Uint64(data[24:32])
	// The sealed bit fences publication; it is not checkpoint certification.
	if flags&^uint32(1|2|4) != 0 || config == 0 || config != uint64(uint(config)) || generation == 0 {
		return qualificationArchiveHeadEvidence{}, errors.New("invalid sealed archive head identity")
	}
	stored := binary.BigEndian.Uint32(data[len(data)-crcSize:])
	if crc32.Checksum(data[:len(data)-crcSize], crc32.MakeTable(crc32.Castagnoli)) != stored {
		return qualificationArchiveHeadEvidence{}, errors.New("archive head checksum mismatch")
	}
	base, tip := binary.BigEndian.Uint64(data[32:40]), binary.BigEndian.Uint64(data[72:80])
	var prefix, tail [32]byte
	copy(prefix[:], data[40:72])
	copy(tail[:], data[80:112])
	tailObject := binary.BigEndian.Uint64(data[112:120])
	sealLen, payloadLen := binary.BigEndian.Uint32(data[120:124]), binary.BigEndian.Uint32(data[124:128])
	payload := data[header : len(data)-crcSize]
	if sealLen != 0 || int(payloadLen) != len(payload) || tip < base || tailObject > generation ||
		(tip > base) != (tail != [32]byte{}) || (tail == [32]byte{}) != (tailObject == 0) ||
		(base == 0 && (prefix != [32]byte{} || payloadLen != 0 || flags&(1|4) != 0)) ||
		(base > 0 && (prefix == [32]byte{} || payloadLen == 0 || flags&(1|4) == 0)) {
		return qualificationArchiveHeadEvidence{}, errors.New("invalid sealed archive head payload")
	}
	if base == 0 || flags&1 == 0 {
		return qualificationArchiveHeadEvidence{}, errors.New("sealed archive head has no regular checkpoint base")
	}
	var seal quepaxa.CheckpointSeal
	if flags&1 != 0 {
		baseDecisionLen := len(payload)
		if flags&4 != 0 {
			if baseDecisionLen <= sha256.Size || [32]byte(payload[baseDecisionLen-sha256.Size:]) == [32]byte{} {
				return qualificationArchiveHeadEvidence{}, errors.New("invalid sealed archive lineage anchor")
			}
			baseDecisionLen -= sha256.Size
		}
		if baseDecisionLen < 16 {
			return qualificationArchiveHeadEvidence{}, errors.New("truncated sealed archive base")
		}
		valueLen, certificateLen := int(binary.BigEndian.Uint32(payload[8:12])), int(binary.BigEndian.Uint32(payload[12:16]))
		if valueLen == 0 || certificateLen == 0 || valueLen > quepaxa.MaxReplicatedValueBytes ||
			valueLen > baseDecisionLen-16 || certificateLen != baseDecisionLen-16-valueLen ||
			!json.Valid(payload[16+valueLen:baseDecisionLen]) {
			return qualificationArchiveHeadEvidence{}, errors.New("invalid sealed archive base")
		}
		decisionSlot := binary.BigEndian.Uint64(payload[:8])
		decoded, checkpoint, err := quepaxa.DecodeCheckpointSeal(payload[16 : 16+valueLen])
		if err != nil || !checkpoint || decisionSlot <= base || decisionSlot > tip || uint64(decoded.ConfigID) != config ||
			uint64(decoded.Index) != base || decoded.PrefixHash != prefix {
			return qualificationArchiveHeadEvidence{}, errors.New("sealed archive base does not match head")
		}
		seal = decoded
	}
	return qualificationArchiveHeadEvidence{
		ConfigID: config, Base: base, BaseRootHash: hex.EncodeToString(seal.RootHash[:]),
		BaseStateHash: hex.EncodeToString(seal.StateHash[:]), BasePrefixHash: hex.EncodeToString(seal.PrefixHash[:]),
		RegularCheckpoint: true, Tip: tip, TailHash: tail, TailObject: tailObject,
	}, nil
}

func runApplication(ctx context.Context, config rhiza.Config) (result error) {
	role, err := qualificationRole(os.Getenv("RHIZA_ROLE"))
	if err != nil {
		return err
	}
	if role == "object-store" {
		return runObjectStoreReplica(ctx, config)
	}
	db, err := rhiza.Open(context.Background(), config)
	if err != nil {
		return err
	}
	var servers []*http.Server
	identity := shutdownIdentity{Namespace: os.Getenv("RHIZA_NAMESPACE"), Run: os.Getenv("RHIZA_QUALIFICATION_RUN_ID"),
		PodUID: os.Getenv("RHIZA_POD_UID"), NodeID: string(config.NodeID)}
	defer func() {
		result = shutdownHost(servers, db.Close, identity, os.Stdout, result, 10*time.Second, 45*time.Second)
	}()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	identity.Process = hex.EncodeToString(nonce[:])

	anchorID := os.Getenv("RHIZA_RECOVERY_ANCHOR_ID")
	var guard *anchorGuard
	if anchorID != "" {
		ns := os.Getenv("RHIZA_NAMESPACE")
		if ns == "" {
			return errors.New("RHIZA_NAMESPACE required with RHIZA_RECOVERY_ANCHOR_ID")
		}
		kube, err := operator.NewInCluster(ns)
		if err != nil {
			return fmt.Errorf("kubernetes client: %w", err)
		}
		backend := &operator.KubernetesAnchorBackend{Kube: kube}
		guard = newAnchorGuard(backend, anchorID, config.ClusterID)
		log.Printf("anchor guard active: id=%s cluster=%s", anchorID, config.ClusterID)
	}

	management := &http.Server{Addr: config.BindAddr, Handler: db.OperatorHandler(), ReadHeaderTimeout: 5 * time.Second}

	app := http.NewServeMux()
	// Test-harness telemetry only; never expose Config or credentials.
	app.Handle("GET /qualification/object-store", objectStoreCounters(db.ObjectStoreStats))
	app.HandleFunc("GET /host", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"host": "go-embedded"}, nil)
	})
	app.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]bool{"alive": true}, nil)
	})

	app.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if !db.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		respond(w, map[string]bool{"ready": true}, nil)
	})
	app.HandleFunc("POST /sql/execute", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.ExecuteRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if guard != nil {
			snap, err := guard.reserve(r.Context())
			if err != nil {
				respond(w, nil, err)
				return
			}
			result, err := db.Execute(r.Context(), req)
			if err != nil {
				guard.fail(r.Context(), snap)
				respond(w, result, err)
				return
			}
			if clearErr := guard.clear(r.Context(), snap); clearErr != nil {
				respond(w, result, clearErr)
				return
			}
			respond(w, result, nil)
			return
		}
		result, err := db.Execute(r.Context(), req)
		respond(w, result, err)
	})
	app.HandleFunc("POST /sql/query", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.QueryRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		result, err := db.Query(r.Context(), req)
		respond(w, result, err)
	})
	app.HandleFunc("POST /qualification/request-status", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.RequestStatusRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := db.RequestStatus(r.Context(), req)
		respond(w, result, err)
	})
	server := &http.Server{Addr: ":8080", Handler: app, ReadHeaderTimeout: 5 * time.Second}
	servers = []*http.Server{server, management}
	return serveHost(ctx, servers, identity, os.Stdout)
}

func qualificationRole(role string) (string, error) {
	if role == "" || role == "voter" {
		return "voter", nil
	}
	if role == "object-store" {
		return role, nil
	}
	return "", fmt.Errorf("invalid RHIZA_ROLE %q for qualification host", role)
}

func runObjectStoreReplica(ctx context.Context, config rhiza.Config) (result error) {
	replicaConfig := objectStoreReplicaConfig(config)
	var replica *rhiza.ReadReplica
	var err error
	if config.EnableReconfiguration {
		replica, err = rhiza.OpenReadReplicaWithHistory(context.Background(), replicaConfig, rhiza.ReplicaHistoryVerificationConfig{BootstrapMembers: append([]rhiza.Member(nil), config.Members...)})
	} else {
		replica, err = rhiza.OpenReadReplica(context.Background(), replicaConfig)
	}
	if err != nil {
		return err
	}
	identity := shutdownIdentity{Namespace: os.Getenv("RHIZA_NAMESPACE"), Run: os.Getenv("RHIZA_QUALIFICATION_RUN_ID"),
		PodUID: os.Getenv("RHIZA_POD_UID"), NodeID: string(config.NodeID)}
	var servers []*http.Server
	defer func() {
		result = shutdownHost(servers, replica.Close, identity, os.Stdout, result, 10*time.Second, 45*time.Second)
	}()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	identity.Process = hex.EncodeToString(nonce[:])
	servers = []*http.Server{
		{Addr: ":8080", Handler: objectStoreReplicaHandler(replica), ReadHeaderTimeout: 5 * time.Second},
		{Addr: config.BindAddr, Handler: replica.Handler(), ReadHeaderTimeout: 5 * time.Second},
	}
	return serveHost(ctx, servers, identity, os.Stdout)
}

func objectStoreReplicaConfig(config rhiza.Config) rhiza.ReplicaConfig {
	members := make([]rhiza.ReplicaMember, len(config.Members))
	for i, member := range config.Members {
		members[i].ID = member.ID
		if config.EnableReconfiguration {
			members[i].PeerURL = member.PeerURL
			if members[i].PeerURL == "" {
				members[i].PeerURL = member.URL
			}
			members[i].PublicKey = member.PublicKey
		}
	}
	return rhiza.ReplicaConfig{
		ClusterID: config.ClusterID, ReplicaID: string(config.NodeID), DataDir: config.DataDir,
		Members: members, SyncInterval: config.ObjStoreSyncInterval, ObjStoreEndpoint: config.ObjStoreEndpoint,
		ObjStoreBucket: config.ObjStoreBucket, ObjStoreProvider: config.ObjStoreProvider, ObjStoreDir: config.ObjStoreDir,
		ObjStorePrefix: config.ObjStorePrefix, ObjStoreRegion: config.ObjStoreRegion, ObjStoreInsecure: config.ObjStoreInsecure,
		ObjStoreRetries: config.ObjStoreRetries, ObjStoreAccessKey: config.ObjStoreAccessKey,
		ObjStoreSecretKey: config.ObjStoreSecretKey, ObjStoreSessionToken: config.ObjStoreSessionToken,
		ObjStoreServiceAccount: config.ObjStoreServiceAccount, ObjStoreAzureTenantID: config.ObjStoreAzureTenantID,
		ObjStoreAzureClientID: config.ObjStoreAzureClientID, ObjStoreAzureClientSecret: config.ObjStoreAzureClientSecret,
		ObjStoreAzureStorageAccount:    config.ObjStoreAzureStorageAccount,
		ObjStoreAzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
		ObjStoreAzureConnectionString:  config.ObjStoreAzureConnectionString,
		ObjStoreAzureUserAssignedID:    config.ObjStoreAzureUserAssignedID,
		MaxConcurrentReads:             config.MaxConcurrentReads, MaxLongPollReads: config.MaxLongPollReads,
	}
}

func objectStoreReplicaHandler(replica *rhiza.ReadReplica) http.Handler {
	app := http.NewServeMux()
	app.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		status := replica.Status()
		if !replica.Ready() || status.CheckpointBaseSlot == 0 || status.CheckpointRootHash == "" ||
			status.CheckpointStateHash == "" || status.CheckpointPrefixHash == "" ||
			status.ArchiveHeadVersion == "" || status.RecoveryPinID == "" ||
			status.RecoveryPinsCreated != 2 || status.RecoveryPinsReleased != 2 {
			http.Error(w, "certified checkpoint restore not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	app.Handle("GET /qualification/object-store", objectStoreCounters(func() (rhiza.ObjectStoreStats, bool) {
		return replica.ObjectStoreStats(), true
	}))
	app.Handle("/", replica.Handler())
	return app
}

// These markers prove this owned process completed HTTP/DB Close, not that an
// earlier canceled/ambiguous provider request cannot still commit remotely.
type shutdownIdentity struct {
	Namespace string `json:"namespace"`
	Run       string `json:"run"`
	PodUID    string `json:"pod_uid"`
	NodeID    string `json:"node_id"`
	Process   string `json:"process"`
}

func hostMarker(out io.Writer, event string, identity shutdownIdentity, httpDrained, dbClosed bool) error {
	return json.NewEncoder(out).Encode(struct {
		Event string `json:"event"`
		shutdownIdentity
		HTTPDrained bool `json:"http_drained"`
		DBClosed    bool `json:"db_closed"`
	}{event, identity, httpDrained, dbClosed})
}

func serveHost(ctx context.Context, servers []*http.Server, identity shutdownIdentity, out io.Writer) error {
	listeners := make([]net.Listener, 0, len(servers))
	for _, server := range servers {
		listener, err := net.Listen("tcp", server.Addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen: %w", err)
		}
		listeners = append(listeners, listener)
	}
	errorsFromServe := make(chan error, len(servers))
	var serving sync.WaitGroup
	for i, server := range servers {
		serving.Add(1)
		go func() {
			defer serving.Done()
			errorsFromServe <- server.Serve(listeners[i])
		}()
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		serving.Wait()
	}()
	if err := hostMarker(out, "qualification-host-start", identity, false, false); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errorsFromServe:
		return fmt.Errorf("HTTP server stopped before shutdown: %w", err)
	}
}

func closeDatabase(closeDB func() error, limit time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- closeDB() }()
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("owned database Close timed out; outcome unresolved")
	}
}

func shutdownHost(servers []*http.Server, closeDB func() error, identity shutdownIdentity, out io.Writer, cause error, drainLimit, closeLimit time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), drainLimit)
	defer cancel()
	drained := make(chan error, len(servers))
	for _, server := range servers {
		go func() { drained <- server.Shutdown(ctx) }()
	}
	var drainErr error
	for range servers {
		drainErr = errors.Join(drainErr, <-drained)
	}
	if drainErr != nil {
		for _, server := range servers {
			_ = server.Close()
		}
		// Shutdown timeout does not join handlers. Never race DB.Close against
		// such a handler or emit an owned-close success marker.
		markerErr := hostMarker(out, "qualification-host-shutdown-failed", identity, false, false)
		return errors.Join(cause, fmt.Errorf("HTTP drain failed: %w", drainErr), markerErr)
	}
	closeErr := closeDatabase(closeDB, closeLimit)
	event := "qualification-host-shutdown-complete"
	if cause != nil || closeErr != nil {
		event = "qualification-host-shutdown-failed"
	}
	markerErr := hostMarker(out, event, identity, true, closeErr == nil)
	return errors.Join(cause, closeErr, markerErr)
}

// Same TLS13/ALPN/public-key pin as network.Transport. No client credential,
// application stream, database Open, RPC, or object-store access is involved.
func probeQUICHandshake(ctx context.Context, address, nodeID string, expected rhiza.PublicKey) error {
	conn, err := quic.DialAddr(ctx, address, &tls.Config{
		MinVersion: tls.VersionTLS13, NextProtos: []string{sqlpolicy.PeerALPN}, ServerName: nodeID,
		InsecureSkipVerify: true, // Exact peer public-key pin verified below, as in Transport.
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) != 1 {
				return errors.New("probe certificate count mismatch")
			}
			key, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
			if !ok || subtle.ConstantTimeCompare(key, expected[:]) != 1 {
				return errors.New("probe certificate identity mismatch")
			}
			return nil
		},
	}, &quic.Config{HandshakeIdleTimeout: 5 * time.Second, MaxIdleTimeout: 5 * time.Second, MaxIncomingStreams: -1, MaxIncomingUniStreams: -1})
	if err != nil {
		return err
	}
	return conn.CloseWithError(0, "qualification handshake only")
}

// qualificationFixture is test-host-only: no node opens and no remote calls.
func qualificationFixture(namespace, run, owner string, entropy io.Reader) (map[string]any, error) {
	if !regexp.MustCompile(`^[a-f0-9]{8}$`).MatchString(run) || namespace != "rhiza-v0191-"+run ||
		!regexp.MustCompile(`^rhiza-postrelease-`+run+`-[a-f0-9]{40}$`).MatchString(owner) {
		return nil, errors.New("invalid fixture identity")
	}
	tokens := make([]string, 5)
	seen := make(map[string]bool, 5)
	for i := range tokens {
		var secret [32]byte
		if _, err := io.ReadFull(entropy, secret[:]); err != nil {
			return nil, errors.New("fixture entropy unavailable")
		}
		tokens[i] = hex.EncodeToString(secret[:])
		if seen[tokens[i]] {
			return nil, errors.New("fixture entropy repeated")
		}
		seen[tokens[i]] = true
	}
	peers := make(map[string]string, 3)
	members := make([]rhiza.Member, 3)
	for i := range members {
		id := fmt.Sprintf("rhiza-voter-%d", i)
		peers[id] = tokens[i+1]
		members[i] = rhiza.Member{ID: rhiza.NodeID(id), URL: "http://" + id + ".rhiza-peers:8080",
			PeerURL: "quic://" + id + ".rhiza-peers:9090", PublicKey: rhiza.PeerPublicKey("gcs-"+run, id, tokens[i+1])}
	}
	learner := rhiza.Member{ID: "rhiza-learner", URL: "http://rhiza-learner:8080", PeerURL: "quic://rhiza-learner:9090",
		PublicKey: rhiza.PeerPublicKey("gcs-"+run, "rhiza-learner", tokens[4])}
	peerJSON, err := json.Marshal(peers)
	if err != nil {
		return nil, err
	}
	memberJSON, err := json.Marshal(members)
	if err != nil {
		return nil, err
	}
	learnerJSON, err := json.Marshal(learner)
	if err != nil {
		return nil, err
	}
	return map[string]any{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true,
		"metadata": map[string]any{"name": "rhiza-qualification-auth", "namespace": namespace,
			"labels": map[string]string{"chaos.rhiza.io/run": run}, "annotations": map[string]string{"rhiza.dev/auth-owner": owner}},
		"stringData": map[string]string{"RHIZA_ADMIN_TOKEN": tokens[0], "RHIZA_PEER_TOKENS": string(peerJSON),
			"RHIZA_CLUSTER_MEMBERS": string(memberJSON), "learner_token": tokens[4], "learner_member": string(learnerJSON)}}, nil
}

func objectStoreCounters(read func() (rhiza.ObjectStoreStats, bool)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats, ok := read()
		if !ok {
			http.Error(w, "object store unavailable", http.StatusServiceUnavailable)
			return
		}
		// Deliberate allowlist: adding a field to the engine's Stats does not
		// automatically change this test endpoint's disclosure boundary.
		respond(w, map[string]uint64{
			"uploads": stats.Uploads, "gets": stats.Gets, "lists": stats.Lists,
			"heads": stats.Heads, "deletes": stats.Deletes, "failures": stats.Failures,
			"bytes_uploaded": stats.BytesUploaded, "bytes_published": stats.BytesPublished,
			"bytes_downloaded": stats.BytesDownloaded, "http_requests": stats.HTTPRequests,
			"http_request_body_bytes":  stats.HTTPRequestBodyBytes,
			"http_response_body_bytes": stats.HTTPResponseBodyBytes,
			"http_failures":            stats.HTTPFailures, "condition_conflicts": stats.ConditionConflicts,
			"sdk_retries": stats.SDKRetries, "retry_metadata_unknown_requests": stats.RetryMetadataUnknownRequests,
		}, nil)
	})
}

func respond(w http.ResponseWriter, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		value = map[string]string{"error_code": "operation_failed"}
	}
	_ = json.NewEncoder(w).Encode(value)
}

func storageID(provider, endpoint, bucket, prefix, cluster string) string {
	arr := []string{provider, endpoint, bucket, prefix, cluster}
	b, _ := json.Marshal(arr)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type snap struct {
	binding    recoveryanchor.Binding
	generation uint64
	evidence   []byte
}

type anchorGuard struct {
	backend           recoveryanchor.Backend
	anchorID          string
	clusterID         string
	format            string
	expectedStorageID string
}

func newAnchorGuard(b recoveryanchor.Backend, anchorID, clusterID string) *anchorGuard {
	return &anchorGuard{
		backend:           b,
		anchorID:          anchorID,
		clusterID:         clusterID,
		format:            "sql-epoch-token/v1",
		expectedStorageID: storageID("s3", "rhiza-minio:9000", "rhiza", "rhiza", clusterID),
	}
}

func (g *anchorGuard) reserve(ctx context.Context) (*snap, error) {
	record, version, err := g.backend.Read(ctx, g.anchorID)
	if err != nil {
		return nil, errAnchorBlocked
	}
	if record.Version != recoveryanchor.Version1 {
		return nil, errAnchorBlocked
	}
	if record.EvidenceFormat != g.format {
		return nil, errAnchorBlocked
	}
	if record.PendingWrite {
		return nil, errAnchorBlocked
	}
	if record.Transition != nil {
		return nil, errAnchorBlocked
	}
	if len(record.Evidence) == 0 {
		return nil, errAnchorBlocked
	}
	if record.Binding.ClusterID != g.clusterID {
		return nil, errAnchorBlocked
	}
	if record.Binding.StorageID != g.expectedStorageID {
		return nil, errAnchorBlocked
	}
	s := &snap{
		binding:    record.Binding,
		generation: record.Generation,
		evidence:   make([]byte, len(record.Evidence)),
	}
	copy(s.evidence, record.Evidence)
	if err := recoveryanchor.UpdateApplication(ctx, g.backend, g.anchorID,
		version, s.binding, s.generation, s.evidence, true); err != nil {
		return nil, errAnchorBlocked
	}
	return s, nil
}

// clear re-reads backend, verifies binding+gen+Pending=true+!Transition,
// and evidence bytes must match exactly to avoid overwriting concurrent app evidence.
func (g *anchorGuard) clear(ctx context.Context, s *snap) error {
	record, freshVersion, err := g.backend.Read(ctx, g.anchorID)
	if err != nil {
		return errAnchorBlocked
	}
	if record.Binding != s.binding {
		return errAnchorBlocked
	}
	if record.Generation != s.generation {
		return errAnchorBlocked
	}
	if !record.PendingWrite {
		return errAnchorBlocked
	}
	if record.Transition != nil {
		return errAnchorBlocked
	}
	if !bytes.Equal(record.Evidence, s.evidence) {
		return errAnchorBlocked
	}
	return recoveryanchor.UpdateApplication(ctx, g.backend, g.anchorID,
		freshVersion, s.binding, s.generation, s.evidence, false)
}

func (g *anchorGuard) fail(_ context.Context, _ *snap) {}
