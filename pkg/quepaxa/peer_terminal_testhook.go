//go:build rhiza_local_testhooks

package quepaxa

// TestDropTerminalDurability only simulates the post-certification sync window.
func (c *Core) TestDropTerminalDurability(terminal Slot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.durable[terminal] = false
	if c.durableThrough >= terminal {
		c.durableThrough = terminal - 1
	}
}
