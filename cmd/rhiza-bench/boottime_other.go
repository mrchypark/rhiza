//go:build !linux

package main

import "errors"

func bootTimeNS() (int64, error) { return 0, errors.New("boot clock unavailable") }
