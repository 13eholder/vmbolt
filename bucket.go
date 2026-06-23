package vmbolt

import (
	"bytes"

	"github.com/13eholder/vmbolt/errors"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2
)

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split nodes are filled.
const DefaultFillPercent = 0.5

// Bucket is a tx-local handle to one top-level B+tree. A Bucket is never shared
// across transactions.
type Bucket struct {
	tx   *Tx
	name string

	rootNode *node          // materialized mutable root (nil until first write)
	dirty    map[*node]bool // set of nodes copied/created in this tx (write tx only)

	// FillPercent sets the threshold for filling nodes when they split. It is
	// non-persisted and must be set per tx.
	FillPercent float64
}

// newBucket creates a tx-local Bucket bound to a top-level bucket name. The
// committed state is resolved on demand from the tx's pinned dbState.
func newBucket(tx *Tx, name string) *Bucket {
	b := &Bucket{
		tx:          tx,
		name:        name,
		FillPercent: DefaultFillPercent,
	}
	if tx.writable {
		b.dirty = make(map[*node]bool)
	}
	return b
}

// baseState returns the committed bucketState this bucket was opened against, or nil.
func (b *Bucket) baseState() *bucketState {
	if b.tx == nil || b.tx.base == nil {
		return nil
	}
	return b.tx.base.buckets[b.name]
}

// rootView returns the root node for read access: the materialized mutable root
// if this write tx has one (read-your-own-writes), otherwise the published root.
func (b *Bucket) rootView() *node {
	if b.rootNode != nil {
		return b.rootNode
	}
	if bs := b.baseState(); bs != nil {
		return bs.root
	}
	return nil
}

// writableRoot returns the root node for write access, materializing (COW) the
// published root on first write.
func (b *Bucket) writableRoot() *node {
	if b.rootNode != nil {
		return b.rootNode
	}
	var root *node
	if bs := b.baseState(); bs != nil && bs.root != nil {
		root = b.copyNode(bs.root)
	} else {
		root = &node{bucket: b, isLeaf: true}
	}
	b.dirty[root] = true
	b.rootNode = root
	return root
}

// copyNode returns a mutable shallow copy of a published (immutable) node: the
// inode slice is copied (so entries can be rebound) but child pointers still
// reference the original children, which are materialized lazily on write.
func (b *Bucket) copyNode(orig *node) *node {
	return &node{
		bucket: b,
		isLeaf: orig.isLeaf,
		key:    orig.key,
		inodes: append([]inode(nil), orig.inodes...),
	}
}

// childForWrite returns the child of parent at idx for write access, COW-ing
// the child into this tx on first touch and relinking the parent's inode at it.
func (b *Bucket) childForWrite(parent *node, idx int) *node {
	child := parent.inodes[idx].child
	if b.dirty[child] {
		return child // already copied in this tx
	}
	c := b.copyNode(child)
	c.parent = parent
	b.dirty[c] = true
	parent.inodes[idx].child = c
	return c
}

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Name returns the bucket's name.
func (b *Bucket) Name() string {
	return b.name
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
func (b *Bucket) Cursor() *Cursor {
	b.tx.stats.IncCursorCount(1)
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0, 8),
	}
}

// Get retrieves the value for a key in the bucket.
func (b *Bucket) Get(key []byte) []byte {
	k, v, _ := b.Cursor().seek(key)
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put sets the value for a key in the bucket.
func (b *Bucket) Put(key []byte, value []byte) (err error) {
	if b.tx.db == nil {
		return errors.ErrTxClosed
	} else if !b.Writable() {
		return errors.ErrTxNotWritable
	} else if len(key) == 0 {
		return errors.ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return errors.ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return errors.ErrValueTooLarge
	}

	newKey := cloneBytes(key)
	newValue := cloneBytes(value)

	c := b.Cursor()
	c.seek(newKey)
	c.node().put(newKey, newKey, newValue, nil, 0)

	return nil
}

// Delete removes a key from the bucket.
func (b *Bucket) Delete(key []byte) (err error) {
	if b.tx.db == nil {
		return errors.ErrTxClosed
	} else if !b.Writable() {
		return errors.ErrTxNotWritable
	}

	c := b.Cursor()
	k, _, _ := c.seek(key)
	if !bytes.Equal(key, k) {
		return nil
	}

	c.node().del(key)
	return nil
}

// ForEach executes a function for each key/value pair in a bucket.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return errors.ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Inspect returns the structure of the bucket.
func (b *Bucket) Inspect() BucketStructure {
	return b.recursivelyInspect([]byte(b.name))
}

func (b *Bucket) recursivelyInspect(name []byte) BucketStructure {
	bs := BucketStructure{Name: string(name)}
	c := b.Cursor()
	for k, _, _ := c.first(); k != nil; k, _, _ = c.next() {
		bs.KeyN++
	}
	return bs
}

// Stats returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	var s BucketStats
	b.forEachNode(func(n *node, depth int) {
		if n.isLeaf {
			s.KeyN += len(n.inodes)
			used := uintptr(nodeHeaderOverheadBytes)
			for i := range n.inodes {
				used += uintptr(nodeInodeOverheadBytes) + uintptr(len(n.inodes[i].key)) + uintptr(len(n.inodes[i].value))
			}
			s.LeafInuse += int(used)
		} else {
			used := uintptr(nodeHeaderOverheadBytes)
			for i := range n.inodes {
				used += uintptr(nodeInodeOverheadBytes) + uintptr(len(n.inodes[i].key))
			}
			s.BranchInuse += int(used)
		}
		if depth+1 > s.Depth {
			s.Depth = depth + 1
		}
	})
	return s
}

// forEachNode iterates over every node in a bucket's current view.
func (b *Bucket) forEachNode(fn func(*node, int)) {
	root := b.rootView()
	if root == nil {
		return
	}
	b.forEachNodeDFS(root, 0, fn)
}

func (b *Bucket) forEachNodeDFS(n *node, depth int, fn func(*node, int)) {
	fn(n, depth)
	if !n.isLeaf {
		for i := range n.inodes {
			b.forEachNodeDFS(n.inodes[i].child, depth+1, fn)
		}
	}
}

// spill finalizes the bucket's dirty tree (split + wire parent pointers).
func (b *Bucket) spill() error {
	if b.rootNode == nil {
		return nil
	}
	if err := b.rootNode.finalize(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()
	return nil
}

// rebalance rebalances every node touched in this tx.
func (b *Bucket) rebalance() {
	for n := range b.dirty {
		n.rebalance()
	}
}

// publishRoot assembles the immutable bucketState to publish for this bucket.
// If the bucket was not written, the existing state is reused; otherwise the
// materialized root (now immutable by convention) is published. Publishing is
// O(1): a new root pointer, with unchanged subtrees shared by pointer.
func (b *Bucket) publishRoot() *bucketState {
	if b.rootNode == nil {
		return b.baseState()
	}
	return &bucketState{root: b.rootNode}
}

// size returns the logical payload size of the bucket's tx view in bytes.
func (b *Bucket) size() int64 {
	root := b.rootView()
	if root == nil {
		return 0
	}
	var sz int64
	var walk func(n *node)
	walk = func(n *node) {
		sz += int64(n.size())
		if !n.isLeaf {
			for i := range n.inodes {
				walk(n.inodes[i].child)
			}
		}
	}
	walk(root)
	return sz
}

// BucketStats records statistics about resources used by a bucket.
type BucketStats struct {
	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchInuse int // bytes actually used for branch data
	LeafInuse   int // bytes actually used for leaf data
}

func (s *BucketStats) Add(other BucketStats) {
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchInuse += other.BranchInuse
	s.LeafInuse += other.LeafInuse
}

// BucketStructure records the structure of a bucket.
type BucketStructure struct {
	Name     string            `json:"name"`
	KeyN     int               `json:"keyN"`
	Children []BucketStructure `json:"buckets,omitempty"`
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
