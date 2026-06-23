package vmbolt

// inode is one entry in a B+tree node.
// Leaf nodes use key/value (child == nil); branch nodes use key/child
// (value == nil, child != nil). The child pointer makes the published tree a
// self-referential, structurally-shared persistent B+tree: a write copies only
// the root-to-leaf path it touches, and unchanged subtrees are shared by
// pointer across generations.
type inode struct {
	flags uint32
	key   []byte
	value []byte // leaf only
	child *node  // branch only
}

// node is the unified B+tree node. A node is mutable while a write transaction
// builds it and immutable by convention once published in a bucketState (i.e.
// reachable from a published root with the transaction done).
//
// There is no longer a separate immutable/mutable node pair, nor a flat
// Nid->node index: the tree itself is the index (branch inodes hold *node child
// pointers), so commit publishes a new root pointer in O(log N) path-copy work
// instead of rebuilding an O(N) map.
type node struct {
	bucket     *Bucket
	isLeaf     bool
	unbalanced bool
	spilled    bool
	key        []byte
	parent     *node
	inodes     []inode
}
