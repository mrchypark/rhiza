//go:build rhiza_local_testhooks

package network

import (
	"context"
	"sync/atomic"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type learnedStreamEvent struct {
	node, sender quepaxa.NodeID
	slot         quepaxa.Slot
	ctx          context.Context
}

var testLearnedStreamEvents atomic.Pointer[chan learnedStreamEvent]

func observeLearnedStreamForTest(node, sender quepaxa.NodeID, slot quepaxa.Slot, ctx context.Context) {
	if events := testLearnedStreamEvents.Load(); events != nil {
		select {
		case *events <- learnedStreamEvent{node: node, sender: sender, slot: slot, ctx: ctx}:
		default:
		}
	}
}
