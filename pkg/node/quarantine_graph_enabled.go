//go:build graph

package node

import (
	"fmt"
	"os"
	"time"
)

func quarantineGraph(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return os.Rename(path, fmt.Sprintf("%s.corrupt-%d", path, time.Now().UnixNano()))
}
