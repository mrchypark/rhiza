package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// identityFile binds a persisted secret to the exact execution identity.
type identityFile struct {
	Version   int    `json:"version"`
	ClusterID string `json:"clusterID"`
	NodeID    string `json:"nodeID"`
	Token     string `json:"token"`
}

func prepareIdentityFile(filename, clusterID, nodeID string) (string, error) {
	return prepareIdentityFileWithHook(filename, clusterID, nodeID, nil)
}

func prepareIdentityFileWithHook(filename, clusterID, nodeID string, hook func(string) error) (token string, resultErr error) {
	if clusterID == "" || nodeID == "" {
		return "", errors.New("cluster and node identity are required")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", err
	}
	want := identityFile{Version: 1, ClusterID: clusterID, NodeID: nodeID, Token: hex.EncodeToString(secret[:])}
	data, err := json.Marshal(want)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(filename), ".rhiza-identity-*")
	if err != nil {
		return "", fmt.Errorf("create temporary identity: %w", err)
	}
	tmp := f.Name()
	defer func() { resultErr = errors.Join(resultErr, os.Remove(tmp)) }()
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return "", fmt.Errorf("persist identity: %w", err)
	}
	if hook != nil {
		if err := hook("before-link"); err != nil {
			return "", err
		}
	}
	if err := os.Link(tmp, filename); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("publish identity: %w", err)
	}
	if hook != nil {
		if err := hook("after-link"); err != nil {
			return "", err
		}
	}
	token, err = readIdentityFile(filename, clusterID, nodeID)
	if err != nil {
		return "", err
	}
	dir, err := os.Open(filepath.Dir(filename))
	if err != nil {
		return "", err
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return "", fmt.Errorf("sync identity directory: %w", err)
	}
	return token, nil
}

func readIdentityFile(filename, clusterID, nodeID string) (string, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > 4096 {
		return "", errors.New("invalid identity file type, permissions, or size")
	}
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		f.Close()
		return "", errors.New("identity file changed while opening")
	}
	dec := json.NewDecoder(io.LimitReader(f, 4097))
	dec.DisallowUnknownFields()
	var got identityFile
	err = dec.Decode(&got)
	if err == nil {
		var extra any
		if e := dec.Decode(&extra); e != io.EOF {
			err = errors.New("trailing identity data")
		}
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return "", fmt.Errorf("invalid identity file: %w", err)
	}
	decoded, err := hex.DecodeString(got.Token)
	if got.Version != 1 || got.ClusterID != clusterID || got.NodeID != nodeID || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != got.Token {
		return "", errors.New("identity binding or token invalid")
	}
	return got.Token, nil
}
