//go:build !goexperiment.arenas

package qlog

type readArena struct{}

func newReadArena() *readArena {
	return &readArena{}
}

func (*readArena) bytes(size int) []byte {
	return make([]byte, size)
}

func (*readArena) free() {}
