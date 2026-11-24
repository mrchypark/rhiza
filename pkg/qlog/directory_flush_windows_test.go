package qlog

import (
	"testing"

	"golang.org/x/sys/windows"
)

// The observed API result does not establish power-loss durability.
func TestWindowsDirectoryFlushDiagnostics(t *testing.T) {
	path, err := windows.UTF16PtrFromString(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		access uint32
	}{
		{"read", windows.GENERIC_READ},
		{"write", windows.GENERIC_WRITE},
		{"read_write", windows.GENERIC_READ | windows.GENERIC_WRITE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, openErr := windows.CreateFile(path, tc.access,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
				nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
			if openErr != nil {
				t.Logf("CreateFileW access=%s result=%v; FlushFileBuffers not attempted", tc.name, openErr)
				return
			}
			flushErr := windows.FlushFileBuffers(h)
			t.Logf("CreateFileW access=%s result=success; FlushFileBuffers result=%v", tc.name, flushErr)
			if err := windows.CloseHandle(h); err != nil {
				t.Errorf("CloseHandle: %v", err)
			}
		})
	}
}
