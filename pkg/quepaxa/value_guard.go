package quepaxa

import "fmt"

// EncodeCertificateForBudget reuses the actual reference-only certificate
// serializer for startup envelope checks. It neither validates a quorum nor
// certifies a decision, and must never be used as recovery authority.
func EncodeCertificateForBudget(configID uint, decision Decision) ([]byte, error) {
	return encodeCertificate(configID, decision)
}

// checkValue does not infer authority from value bytes. Only an immutable
// validator installed by the owner at construction can authorize a new format.
func (c *Core) checkValue(value []byte) error {
	if c.validateValue != nil {
		// Absolute allocation envelope before invoking the selected parser.
		if len(value) > 256<<10 {
			return fmt.Errorf("profile value exceeds absolute envelope")
		}
		return c.validateValue(value)
	}
	if len(value) > MaxReplicatedValueBytes {
		return fmt.Errorf("QuePaxa value exceeds %d bytes", MaxReplicatedValueBytes)
	}
	return nil
}
