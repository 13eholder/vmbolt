package vmbolt

// bucketState is the immutable published state of one top-level bucket.
// Once published inside a dbState it is never mutated. The published B+tree is
// structurally shared: branch inodes hold *node child pointers, so unchanged
// subtrees are shared by pointer across generations and a commit publishes a
// new root pointer in O(log N) path-copy work.
type bucketState struct {
	root *node
}

// dbState is the immutable, globally-published database view pinned by every
// read transaction at Begin time.
type dbState struct {
	buckets map[string]*bucketState
}
