//go:build linux

package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBootTimeMatchesProcUptime(t *testing.T) {
	before, err := bootTimeNS()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		t.Fatal(err)
	}
	after, err := bootTimeNS()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 1 {
		t.Fatal("empty /proc/uptime")
	}
	uptime, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		t.Fatal(err)
	}
	ns := int64(uptime * float64(time.Second))
	if ns < before-int64(20*time.Millisecond) || ns > after+int64(20*time.Millisecond) {
		t.Fatalf("boottime %d..%d does not bracket proc uptime %d", before, after, ns)
	}
}
