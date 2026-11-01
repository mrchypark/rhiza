package managedbootstrap

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type IdentityFile struct {
	Version   int    `json:"version"`
	ClusterID string `json:"clusterID"`
	NodeID    string `json:"nodeID"`
	Token     string `json:"token"`
}

func ReadIdentityFile(filename, clusterID, nodeID string) (string, error) {
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
	var got IdentityFile
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
