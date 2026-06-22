package vmbolt

import "github.com/13eholder/vmbolt/internal/common"

// bucketState is the immutable published state of one top-level bucket.
// Once published inside a dbState it is never mutated.
type bucketState struct {
	root  common.Nid
	nodes map[common.Nid]*snapNode
}

// dbState is the immutable, globally-published database view pinned by every
// read transaction at Begin time.
type dbState struct {
	buckets map[string]*bucketState

	// Global node-id allocator state. Nid 0 remains the "unassigned" sentinel.
	nextNid uint64
	freeNid []common.Nid
}
