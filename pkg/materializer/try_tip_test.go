package materializer

import "testing"

func TestTryTipDoesNotObserveDuringStateReplacement(t *testing.T) {
	m := &Materializer{tip: 4}
	m.mu.Lock()
	if tip, known := m.TryTip(); known || tip != 0 {
		m.mu.Unlock()
		t.Fatalf("busy state must be unknown: tip=%d known=%t", tip, known)
	}
	m.tip = 8
	m.mu.Unlock()
	if tip, known := m.TryTip(); !known || tip != 8 {
		t.Fatalf("published tip=%d known=%t", tip, known)
	}
}
