//go:build !rhiza_local_testhooks

package network

import (
	"context"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func observeLearnedStreamForTest(quepaxa.NodeID, quepaxa.NodeID, quepaxa.Slot, context.Context) {}
