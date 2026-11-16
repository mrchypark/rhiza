//go:build rhiza_local_testhooks

package quepaxa

func (c *Core) TestCertifiedDecision(value DecidedValue) (Decision, error) {
	return c.certifiedDecision(value)
}
