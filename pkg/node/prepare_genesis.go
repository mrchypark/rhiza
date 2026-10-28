package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const (
	preparedGenesisMarkerName  = "bootstrap-genesis.json"
	preparedGenesisPendingName = ".bootstrap-genesis.next"
)

type preparedGenesisMarker struct {
	Version        uint                        `json:"version"`
	Phase          string                      `json:"phase"`
	Assignment     fencing.BootstrapAssignment `json:"assignment"`
	AssignmentHash string                      `json:"assignment_hash"`
	BootstrapHash  string                      `json:"bootstrap_hash"`
}

type preparedGenesisExecution struct {
	MarkerNoop       bool
	MaterializerDone bool
	FilesSynced      bool
	CleanupErr       error
}

func preparedGenesisNodeCandidate(config *types.ExecutionConfig) bool {
	if config == nil || config.Learner != nil || config.NodeID == "" {
		return false
	}
	for _, member := range config.Members {
		if member.ID == config.NodeID {
			return true
		}
	}
	return false
}

func validatePreparedGenesisRuntimeConfig(config *types.ExecutionConfig) error {
	if config == nil || config.PreparedPodUID == "" || config.DataDir == "" || !preparedGenesisNodeCandidate(config) {
		return fmt.Errorf("prepared genesis requires its assigned initial voter identity")
	}
	if err := validateVoterMembershipStructure(config); err != nil {
		return err
	}
	marker, exists, err := readPreparedGenesisMarkerForRuntime(config.DataDir)
	if err != nil {
		return err
	}
	if !exists || marker.Phase != "complete" || marker.Assignment.Purpose != fencing.BootstrapInitial ||
		marker.Assignment.PodUID != config.PreparedPodUID || marker.Assignment.NodeID != string(config.NodeID) ||
		marker.Assignment.SourceClusterID != string(config.ClusterID) {
		return fmt.Errorf("prepared genesis marker is missing or mismatched; preserve DataDir")
	}
	want, err := preparedGenesisMarkerFor(config, marker.Assignment)
	if err != nil || !preparedGenesisMarkerMatches(marker, want) || marker.BootstrapHash != recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability)).Membership {
		return fmt.Errorf("prepared genesis marker does not match current public membership; preserve DataDir")
	}
	return nil
}

func readPreparedGenesisMarkerForRuntime(dataDir string) (preparedGenesisMarker, bool, error) {
	var marker preparedGenesisMarker
	name := filepath.Join(dataDir, preparedGenesisMarkerName)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil {
		return marker, true, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return marker, true, fmt.Errorf("bootstrap genesis marker is not a bounded regular file; preserve DataDir")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return marker, true, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return marker, true, fmt.Errorf("invalid bootstrap genesis marker; preserve DataDir: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return marker, true, fmt.Errorf("bootstrap genesis marker has trailing data")
	}
	canonical, err := json.Marshal(marker)
	if err != nil || !bytes.Equal(bytes.TrimSpace(data), canonical) || marker.Version != 1 || marker.Phase != "complete" {
		return marker, true, fmt.Errorf("bootstrap genesis marker is not canonical and complete; preserve DataDir")
	}
	if err := marker.Assignment.Validate(); err != nil {
		return marker, true, fmt.Errorf("invalid bootstrap genesis assignment: %w", err)
	}
	assignmentBytes, err := json.Marshal(marker.Assignment)
	if err != nil {
		return marker, true, err
	}
	assignmentHash := sha256.Sum256(assignmentBytes)
	if marker.AssignmentHash != hex.EncodeToString(assignmentHash[:]) || len(marker.BootstrapHash) != sha256.Size*2 {
		return marker, true, fmt.Errorf("bootstrap genesis marker digest differs; preserve DataDir")
	}
	if _, err := hex.DecodeString(marker.BootstrapHash); err != nil {
		return marker, true, fmt.Errorf("bootstrap genesis marker contains invalid membership digest")
	}
	return marker, true, nil
}

func prepareInitialGenesis(ctx context.Context, config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) (execution preparedGenesisExecution, resultErr error) {
	if err := validateInitialGenesisConfig(config, assignment); err != nil {
		return execution, err
	}
	config.DataDir = filepath.Clean(config.DataDir)
	marker, err := preparedGenesisMarkerFor(config, assignment)
	if err != nil {
		return execution, err
	}
	dataDirInfo, err := os.Lstat(config.DataDir)
	if err == nil && (!dataDirInfo.IsDir() || dataDirInfo.Mode()&os.ModeSymlink != 0) {
		return execution, fmt.Errorf("bootstrap genesis DataDir root is not a real directory; preserve DataDir")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return execution, err
	}
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
			return execution, err
		}
	}
	dataDirInfo, err = os.Lstat(config.DataDir)
	if err != nil {
		return execution, err
	}
	if !dataDirInfo.IsDir() || dataDirInfo.Mode()&os.ModeSymlink != 0 {
		return execution, fmt.Errorf("bootstrap genesis DataDir root is not a real directory; preserve DataDir")
	}
	existing, exists, err := readPreparedGenesisMarkerForAssignment(config.DataDir, marker)
	if err != nil {
		return execution, err
	}
	phase := "fresh-preflight"
	if exists {
		if existing.Phase == "complete" {
			phase = "complete-preflight"
		} else if existing.Phase == "preparing" {
			phase = "preparing"
		} else {
			return execution, fmt.Errorf("bootstrap genesis marker has unknown phase; preserve DataDir")
		}
	} else if _, err := os.Lstat(filepath.Join(config.DataDir, preparedRestoreMarkerName)); err == nil {
		return execution, fmt.Errorf("certified restore marker conflicts with bootstrap genesis; preserve DataDir")
	} else if !errors.Is(err, os.ErrNotExist) {
		return execution, err
	}
	if err := validatePreparedGenesisInventory(config.DataDir, phase); err != nil {
		return execution, err
	}
	if phase != "complete-preflight" {
		if err := materializer.ValidateEmptyGenesisState(filepath.Join(config.DataDir, "sqlite.db"), filepath.Join(config.DataDir, "latticedb", "graph.ltdb")); err != nil {
			return execution, err
		}
	}
	_, lock, err := qlog.Acquire(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		return execution, err
	}
	defer func() {
		releaseErr := preparedRestoreReleaseLock(lock)
		execution.CleanupErr = errors.Join(execution.CleanupErr, releaseErr)
		resultErr = errors.Join(resultErr, releaseErr)
	}()
	if existing, exists, err = readPreparedGenesisMarkerForAssignment(config.DataDir, marker); err != nil {
		return execution, err
	}
	if exists && existing.Phase == "complete" {
		execution.MarkerNoop = true
		return execution, nil
	}
	if exists && existing.Phase != "preparing" {
		return execution, fmt.Errorf("bootstrap genesis marker has unknown phase; preserve DataDir")
	}
	if err := materializer.ValidateEmptyGenesisState(filepath.Join(config.DataDir, "sqlite.db"), filepath.Join(config.DataDir, "latticedb", "graph.ltdb")); err != nil {
		return execution, err
	}
	if !exists {
		if err := writePreparedGenesisMarker(config.DataDir, marker, false); err != nil {
			return execution, fmt.Errorf("record bootstrap genesis intent: %w", err)
		}
	}
	if preparedRestoreCrashBoundary != nil {
		preparedRestoreCrashBoundary("genesis-intent-durable")
	}
	if err := validatePreparedGenesisInventory(config.DataDir, "preparing"); err != nil {
		return execution, err
	}
	material, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 1)
	if err != nil {
		return execution, fmt.Errorf("open initial genesis materializer: %w", err)
	}
	if preparedRestoreCrashBoundary != nil {
		preparedRestoreCrashBoundary("genesis-materializer-open")
	}
	if material.Tip() != 0 {
		resultErr = fmt.Errorf("initial genesis materializer is not empty; preserve DataDir")
	}
	closeErr := material.Close()
	execution.MaterializerDone = closeErr == nil
	execution.CleanupErr = errors.Join(execution.CleanupErr, closeErr)
	resultErr = errors.Join(resultErr, closeErr)
	if resultErr != nil {
		return execution, resultErr
	}
	if err := syncPreparedGenesisFiles(config.DataDir); err != nil {
		execution.CleanupErr = errors.Join(execution.CleanupErr, err)
		return execution, err
	}
	execution.FilesSynced = true
	marker.Phase = "complete"
	if err := writePreparedGenesisMarker(config.DataDir, marker, true); err != nil {
		execution.CleanupErr = errors.Join(execution.CleanupErr, err)
		return execution, fmt.Errorf("commit bootstrap genesis marker: %w", err)
	}
	if preparedRestoreCrashBoundary != nil {
		preparedRestoreCrashBoundary("genesis-complete-marker")
	}
	return execution, nil
}

func validateInitialGenesisConfig(config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) error {
	if config == nil || config.DataDir == "" || config.Local || config.PreparedPodUID == "" ||
		config.PreparedPodUID != assignment.PodUID || string(config.ClusterID) != assignment.SourceClusterID ||
		string(config.NodeID) != assignment.NodeID || assignment.Purpose != fencing.BootstrapInitial {
		return fmt.Errorf("initial genesis requires its exact authorized Pod, node, source, and purpose")
	}
	if err := assignment.Validate(); err != nil {
		return err
	}
	if config.PreparedPodUID != strings.TrimSpace(config.PreparedPodUID) {
		return fmt.Errorf("prepared Pod UID is not canonical")
	}
	if _, err := fencing.ReplacementNodeID(config.PreparedPodUID); err != nil {
		return err
	}
	if err := validateVoterMembershipStructure(config); err != nil {
		return err
	}
	if !config.EnableReconfiguration || len(config.Members) < 2 || config.Learner != nil {
		return fmt.Errorf("initial genesis requires the authorized initial voter membership")
	}
	if assignment.MembershipHash != fencing.BootstrapMembersHash(config.Members) {
		return fmt.Errorf("initial genesis membership differs from its authority assignment")
	}
	configured, err := validateObjectStoreConfig(config)
	if err != nil {
		return err
	}
	if !configured {
		return fmt.Errorf("initial genesis requires shared object storage")
	}
	return nil
}

func preparedGenesisMarkerFor(config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) (preparedGenesisMarker, error) {
	data, err := json.Marshal(assignment)
	if err != nil {
		return preparedGenesisMarker{}, err
	}
	hash := sha256.Sum256(data)
	return preparedGenesisMarker{
		Version: 1, Phase: "preparing", Assignment: assignment,
		AssignmentHash: hex.EncodeToString(hash[:]),
		BootstrapHash:  recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability)).Membership,
	}, nil
}

func preparedGenesisMarkerMatches(got, want preparedGenesisMarker) bool {
	return got.Version == 1 && got.Assignment == want.Assignment && got.AssignmentHash == want.AssignmentHash &&
		got.BootstrapHash == want.BootstrapHash && (got.Phase == "preparing" || got.Phase == "complete")
}

func readPreparedGenesisMarkerForAssignment(dataDir string, expected preparedGenesisMarker) (preparedGenesisMarker, bool, error) {
	var marker preparedGenesisMarker
	name := filepath.Join(dataDir, preparedGenesisMarkerName)
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil {
		return marker, true, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return marker, true, fmt.Errorf("bootstrap genesis marker is not a bounded regular file; preserve DataDir")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return marker, true, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		prefix := genesisMarkerPrefix(expected, "preparing")
		if len(data) <= len(prefix) && bytes.Equal(data, prefix[:len(data)]) {
			return expected, true, nil
		}
		return marker, true, fmt.Errorf("invalid bootstrap genesis marker; preserve DataDir: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return marker, true, fmt.Errorf("bootstrap genesis marker has trailing data")
	}
	canonical, err := json.Marshal(marker)
	if err != nil || !bytes.Equal(bytes.TrimSpace(data), canonical) || !preparedGenesisMarkerMatches(marker, expected) {
		return marker, true, fmt.Errorf("bootstrap genesis marker differs from the authority assignment; preserve DataDir")
	}
	return marker, true, nil
}

func genesisMarkerPrefix(marker preparedGenesisMarker, phase string) []byte {
	marker.Phase = phase
	data, _ := json.Marshal(marker)
	return data
}

func writePreparedGenesisMarker(dataDir string, marker preparedGenesisMarker, complete bool) error {
	name := filepath.Join(dataDir, preparedGenesisMarkerName)
	if complete {
		marker.Phase = "complete"
		current, exists, err := readPreparedGenesisMarkerForAssignment(dataDir, marker)
		if err != nil || !exists || current.Phase != "preparing" {
			return errors.Join(err, fmt.Errorf("bootstrap genesis update lacks matching durable intent; preserve DataDir"))
		}
		return writePreparedGenesisFile(dataDir, preparedGenesisPendingName, name, marker)
	}
	marker.Phase = "preparing"
	data := genesisMarkerPrefix(marker, "preparing")
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if info, err := os.Lstat(name); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > int64(len(data)) {
			return fmt.Errorf("bootstrap genesis intent is not a bounded regular file")
		}
		existing, err := os.ReadFile(name)
		if err != nil || !bytes.Equal(data[:len(existing)], existing) {
			return errors.Join(err, fmt.Errorf("bootstrap genesis intent is not a matching interrupted prefix; preserve DataDir"))
		}
		flags = os.O_WRONLY | os.O_TRUNC
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writePreparedFile(name, flags, data, nil); err != nil {
		return err
	}
	return syncPreparedDirectory(dataDir)
}

func writePreparedGenesisFile(dataDir, pendingName, finalName string, marker preparedGenesisMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	pending := filepath.Join(dataDir, pendingName)
	if info, err := os.Lstat(pending); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > int64(len(data)) {
			return fmt.Errorf("pending bootstrap genesis marker is not a bounded regular file")
		}
		prior, readErr := os.ReadFile(pending)
		if readErr != nil || !bytes.Equal(data[:len(prior)], prior) {
			return errors.Join(readErr, fmt.Errorf("pending bootstrap genesis marker has unknown contents; preserve DataDir"))
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writePreparedFile(pending, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, data, nil); err != nil {
		return err
	}
	if err := os.Rename(pending, finalName); err != nil {
		return err
	}
	return syncPreparedDirectory(dataDir)
}

func validatePreparedGenesisInventory(dataDir, phase string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	allowed := map[string]bool{"qlog": true}
	if phase == "preparing" || phase == "complete-preflight" {
		for _, name := range []string{preparedGenesisMarkerName, preparedGenesisPendingName, "sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "sqlite.db-journal", "latticedb"} {
			allowed[name] = true
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		if !allowed[name] {
			return fmt.Errorf("unknown path %q in bootstrap genesis DataDir; preserve DataDir", name)
		}
		info, err := os.Lstat(filepath.Join(dataDir, name))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink %q in bootstrap genesis DataDir; preserve DataDir", name)
		}
		switch name {
		case "qlog":
			if !info.IsDir() {
				return fmt.Errorf("qlog path is not a directory; preserve DataDir")
			}
			children, err := os.ReadDir(filepath.Join(dataDir, name))
			if err != nil {
				return err
			}
			for _, child := range children {
				childInfo, err := os.Lstat(filepath.Join(dataDir, name, child.Name()))
				if err != nil || child.Name() != "lock.qlog" || !childInfo.Mode().IsRegular() || childInfo.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("unknown or invalid qlog path %q; preserve DataDir", child.Name())
				}
			}
		case preparedGenesisMarkerName, preparedGenesisPendingName:
			if !info.Mode().IsRegular() || info.Size() > 64<<10 {
				return fmt.Errorf("invalid bootstrap genesis marker path %q; preserve DataDir", name)
			}
		case "latticedb":
			if !info.IsDir() {
				return fmt.Errorf("bootstrap genesis path %q is not a directory", name)
			}
			children, err := os.ReadDir(filepath.Join(dataDir, name))
			if err != nil {
				return err
			}
			for _, child := range children {
				if child.Name() != "graph.ltdb" {
					return fmt.Errorf("unknown Graph path %q in bootstrap genesis DataDir; preserve DataDir", child.Name())
				}
				childInfo, err := os.Lstat(filepath.Join(dataDir, name, child.Name()))
				if err != nil || !childInfo.IsDir() || childInfo.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("invalid Graph database directory in bootstrap genesis DataDir")
				}
				files, err := os.ReadDir(filepath.Join(dataDir, name, child.Name()))
				if err != nil {
					return err
				}
				for _, file := range files {
					switch file.Name() {
					case "state.json", "state.json.layout", "state.json.lock", "wal.log", "wal.base", "ids.json":
					default:
						return fmt.Errorf("unknown Graph database file %q; preserve DataDir", file.Name())
					}
					fileInfo, err := os.Lstat(filepath.Join(dataDir, name, child.Name(), file.Name()))
					if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode()&os.ModeSymlink != 0 {
						return fmt.Errorf("invalid Graph database file %q; preserve DataDir", file.Name())
					}
				}
			}
		default:
			if !info.Mode().IsRegular() {
				return fmt.Errorf("bootstrap genesis path %q is not a regular file", name)
			}
		}
	}
	return nil
}

func syncPreparedGenesisFiles(dataDir string) error {
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm", "sqlite.db-journal"} {
		path := filepath.Join(dataDir, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid genesis materializer file %q (mode %s)", name, info.Mode())
		}
		if err := syncGenesisFile(path); err != nil {
			return err
		}
	}
	graphDir := filepath.Join(dataDir, "latticedb", "graph.ltdb")
	entries, err := os.ReadDir(graphDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, entry := range entries {
		filePath := filepath.Join(graphDir, entry.Name())
		if err := syncGenesisFile(filePath); err != nil {
			return err
		}
	}
	if err := syncPreparedDirectory(graphDir); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncPreparedDirectory(filepath.Join(dataDir, "latticedb")); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncPreparedDirectory(dataDir)
}

func syncGenesisFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid genesis materializer file %q (mode %s)", path, info.Mode())
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	return nil
}
