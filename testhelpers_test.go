package vmbolt_test

import (
	"encoding/binary"
	"os"
)

// tempfile returns a temporary file path (used by tests that still pass a path
// to Open, even though the pure-memory engine does not read/write it).
func tempfile() string {
	f, err := os.CreateTemp("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

// trunc truncates b to length, or returns b unchanged if it is shorter.
func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

// u64tob converts a uint64 into an 8-byte big-endian slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
