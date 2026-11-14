//go:build rhiza_local_testhooks

package quepaxa

// TestOnlyHoldSyncLock lets a QUIC integration test create actual Core lock contention.
func (c *Core) TestOnlyHoldSyncLock(entered chan<- struct{}, release <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(entered)
	<-release
}
