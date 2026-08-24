// Package quepaxa implements the crash-fault-tolerant QuePaxa Algorithm 3
// recorder and Algorithm 4 proposer over a durable QLog.
//
// Core is safe for concurrent proposals. A successful proposal has a strict
// majority certificate and is durably recorded locally before it is returned.
// Membership is fixed for the lifetime of a Core.
package quepaxa
