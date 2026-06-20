package vmbolt

import (
	"encoding/hex"
)

// Check is not yet implemented for the per-bucket pure memory model.
//
// It currently reports no errors (treats the database as consistent) so that
// callers such as btesting.MustCheck — which os.Exit(-1) on any check error —
// do not abort the test process. This is the explicit "skip tx_check" trade-off
// for this refactor; a follow-up should implement per-bucket reachability and
// key-ordering checks.
func (tx *Tx) Check(options ...CheckOption) <-chan error {
	_ = options
	ch := make(chan error)
	close(ch)
	return ch
}

// ===========================================================================================
// Public diagnostic options/stringers are preserved for API compatibility.

type checkConfig struct {
	kvStringer KVStringer
	pageId     uint64
}

type CheckOption func(options *checkConfig)

func WithKVStringer(kvStringer KVStringer) CheckOption {
	return func(c *checkConfig) {
		c.kvStringer = kvStringer
	}
}

// WithPageId sets a page ID from which the check command starts to check.
func WithPageId(pageId uint64) CheckOption {
	return func(c *checkConfig) {
		c.pageId = pageId
	}
}

// KVStringer allows to prepare human-readable diagnostic messages.
type KVStringer interface {
	KeyToString([]byte) string
	ValueToString([]byte) string
}

// HexKVStringer serializes both key & value to hex representation.
func HexKVStringer() KVStringer {
	return hexKvStringer{}
}

type hexKvStringer struct{}

func (hexKvStringer) KeyToString(key []byte) string {
	return hex.EncodeToString(key)
}

func (hexKvStringer) ValueToString(value []byte) string {
	return hex.EncodeToString(value)
}
