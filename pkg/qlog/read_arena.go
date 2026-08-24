//go:build goexperiment.arenas

package qlog

import "arena"

type readArena struct {
	arena *arena.Arena
}

func newReadArena() *readArena {
	return &readArena{arena: arena.NewArena()}
}

func (a *readArena) bytes(size int) []byte {
	return arena.MakeSlice[byte](a.arena, size, size)
}

func (a *readArena) free() {
	a.arena.Free()
}
