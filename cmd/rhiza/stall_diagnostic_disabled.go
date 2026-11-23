//go:build !rhiza_stall_diagnostics || !linux

package main

import "context"

func startStallDiagnostics(context.Context) func() { return func() {} }
