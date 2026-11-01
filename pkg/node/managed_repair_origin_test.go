package node

import (
	"context"
	"encoding/json"
	"fmt"
)

func (c *managedTestChecker) ValidateManagedRepairOrigin(_ context.Context, raw []byte) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return fmt.Errorf("invalid test repair origin")
	}
	return nil
}
