package node

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

const (
	logicalRunStateVersion  = 1
	logicalRunStateMaxBytes = 4096
)

type logicalRunState struct {
	Version        int    `json:"version"`
	ClusterID      string `json:"clusterID"`
	NodeID         string `json:"nodeID"`
	AssignedNodeID string `json:"assignedNodeID,omitempty"`
	AssignmentID   string `json:"assignmentID,omitempty"`
	PodUID         string `json:"podUID"`
	RunID          string `json:"runID"`
}

// loadOrCreateLogicalRunState pins a RunID to this local data directory before
// the authority request. A same-Pod process restart reuses that identity; a
// different PodUID cannot inherit it. The authority still validates the
// authenticated workload and exact WAL before allowing continuation.
func loadOrCreateLogicalRunState(config *types.ExecutionConfig, podUID string, local localVoterState) (logicalRunState, bool, error) {
	if config == nil || config.DataDir == "" || config.ClusterID == "" || config.NodeID == "" || podUID == "" {
		return logicalRunState{}, false, fmt.Errorf("fencing: incomplete logical Run identity scope")
	}
	path := filepath.Join(config.DataDir, "logical-run.json")
	state, err := readLogicalRunState(path)
	if err == nil {
		if state.ClusterID != string(config.ClusterID) || state.NodeID != string(config.NodeID) || state.PodUID != podUID {
			return logicalRunState{}, false, fmt.Errorf("fencing: persisted logical Run identity belongs to another workload")
		}
		if local.identity != nil && (local.identity.Cluster != state.ClusterID ||
			(local.identity.Node != state.NodeID && local.identity.Node != state.AssignedNodeID)) {
			return logicalRunState{}, false, fmt.Errorf("fencing: persisted logical Run identity differs from local voter identity")
		}
		return state, local.identity != nil && local.hadWAL, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return logicalRunState{}, false, err
	}
	if local.identity != nil || local.hadWAL {
		return logicalRunState{}, false, fmt.Errorf("%w: existing voter WAL has no persisted logical Run identity", ErrVoterStateLost)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return logicalRunState{}, false, fmt.Errorf("fencing: generate logical RunID: %w", err)
	}
	state = logicalRunState{Version: logicalRunStateVersion, ClusterID: string(config.ClusterID), NodeID: string(config.NodeID), PodUID: podUID, RunID: hex.EncodeToString(nonce[:])}
	data, err := json.Marshal(state)
	if err != nil {
		return logicalRunState{}, false, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return logicalRunState{}, false, fmt.Errorf("persist logical Run identity: %w", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return logicalRunState{}, false, fmt.Errorf("persist logical Run identity: %w", err)
	}
	dir, err := os.Open(config.DataDir)
	if err != nil {
		return logicalRunState{}, false, err
	}
	err = errors.Join(dir.Sync(), dir.Close())
	if err != nil {
		return logicalRunState{}, false, fmt.Errorf("sync logical Run identity directory: %w", err)
	}
	return state, false, nil
}

func persistLogicalRunAssignment(config *types.ExecutionConfig, state logicalRunState, identity fencing.Identity) (logicalRunState, error) {
	if config == nil || state.ClusterID != string(config.ClusterID) || state.NodeID != string(config.NodeID) ||
		identity.SourceClusterID != state.ClusterID || identity.PodUID != state.PodUID || identity.RunID != state.RunID ||
		identity.NodeID == "" || (identity.AssignmentID != "" && !validLogicalAssignmentID(identity.AssignmentID)) {
		return logicalRunState{}, fmt.Errorf("fencing: logical authority assignment differs from persisted Run scope")
	}
	if state.AssignedNodeID != "" && (state.AssignedNodeID != identity.NodeID || state.AssignmentID != identity.AssignmentID) {
		return logicalRunState{}, fmt.Errorf("fencing: logical Run assignment is immutable")
	}
	if identity.NodeID == state.NodeID && identity.AssignmentID == "" {
		return state, nil
	}
	state.AssignedNodeID, state.AssignmentID = identity.NodeID, identity.AssignmentID
	data, err := json.Marshal(state)
	if err != nil {
		return logicalRunState{}, err
	}
	dir, err := os.Open(config.DataDir)
	if err != nil {
		return logicalRunState{}, err
	}
	defer dir.Close()
	tmp, err := os.CreateTemp(config.DataDir, ".logical-run-*.tmp")
	if err != nil {
		return logicalRunState{}, err
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		cleanup()
		return logicalRunState{}, err
	}
	_, writeErr := tmp.Write(append(data, '\n'))
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		cleanup()
		return logicalRunState{}, err
	}
	if err := os.Rename(tmpPath, filepath.Join(config.DataDir, "logical-run.json")); err != nil {
		cleanup()
		return logicalRunState{}, err
	}
	if err := dir.Sync(); err != nil {
		return logicalRunState{}, err
	}
	return state, nil
}

func validLogicalAssignmentID(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readLogicalRunState(path string) (logicalRunState, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return logicalRunState{}, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > logicalRunStateMaxBytes {
		return logicalRunState{}, fmt.Errorf("fencing: invalid persisted logical Run identity file")
	}
	file, err := os.Open(path)
	if err != nil {
		return logicalRunState{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, logicalRunStateMaxBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return logicalRunState{}, err
	}
	if len(data) > logicalRunStateMaxBytes {
		return logicalRunState{}, fmt.Errorf("fencing: persisted logical Run identity exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state logicalRunState
	if decoder.Decode(&state) != nil || decoder.Decode(new(any)) != io.EOF || state.Version != logicalRunStateVersion ||
		state.ClusterID == "" || state.NodeID == "" || state.PodUID == "" || len(state.RunID) != 32 ||
		(state.AssignedNodeID == "") != (state.AssignmentID == "") ||
		(state.AssignedNodeID != "" && !validLogicalAssignmentID(state.AssignmentID)) {
		return logicalRunState{}, fmt.Errorf("fencing: invalid persisted logical Run identity")
	}
	if _, err := hex.DecodeString(state.RunID); err != nil {
		return logicalRunState{}, fmt.Errorf("fencing: invalid persisted logical RunID")
	}
	canonical, _ := json.Marshal(state)
	if !bytes.Equal(bytes.TrimSpace(data), canonical) {
		return logicalRunState{}, fmt.Errorf("fencing: persisted logical Run identity is not canonical")
	}
	return state, nil
}
