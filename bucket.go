package vmbolt

import (
	"bytes"
	"fmt"

	"github.com/13eholder/vmbolt/errors"
	"github.com/13eholder/vmbolt/internal/common"
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

	base     *bucketState             // pinned generation: read view / COW base (nil for a brand-new bucket)
	rootNode *workNode                // materialized mutable root (nil until first write)
	dirty    map[common.Nid]*workNode // tx-local mutable node cache (write tx only)
	obsolete map[common.Nid]struct{}  // node ids freed in this tx (write tx only)

	// FillPercent sets the threshold for filling nodes when they split. It is
	// non-persisted and must be set per tx.
	FillPercent float64
}

// newBucket creates a tx-local Bucket bound to an existing committed bucket
// state or a brand-new bucket when base is nil.
func newBucket(tx *Tx, name string, base *bucketState) *Bucket {
	b := &Bucket{
		tx:          tx,
		name:        name,
		base:        base,
		FillPercent: DefaultFillPercent,
	}
	if tx.writable {
		b.dirty = make(map[common.Nid]*workNode)
		b.obsolete = make(map[common.Nid]struct{})
	}
	return b
}

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Name returns the bucket's name.
func (b *Bucket) Name() string {
	return b.name
}

// Root returns the Nid of the bucket's root node. Returns 0 if the bucket has
// no committed root and none has been materialized yet.
func (b *Bucket) Root() common.Nid {
	if b.rootNode != nil {
		return b.rootNode.nid
	}
	if b.base != nil {
		return b.base.root
	}
	return 0
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
	c.node().put(newKey, newKey, newValue, 0, 0)

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
			for _, inode := range n.inodes {
				used += uintptr(nodeInodeOverheadBytes) + uintptr(len(inode.Key())) + uintptr(len(inode.Value()))
			}
			s.LeafInuse += int(used)
		} else {
			used := uintptr(nodeHeaderOverheadBytes)
			for _, inode := range n.inodes {
				used += uintptr(nodeInodeOverheadBytes) + uintptr(len(inode.Key()))
			}
			s.BranchInuse += int(used)
		}
		if depth+1 > s.Depth {
			s.Depth = depth + 1
		}
	})
	return s
}

// forEachNode iterates over every node in a bucket.
func (b *Bucket) forEachNode(fn func(*node, int)) {
	if b.rootNode != nil && b.Root() == 0 {
		fn(b.rootNode.readNodeView(), 0)
		return
	}
	if !b.hasCommittedRoot() {
		return
	}
	b._forEachNode(b.Root(), 0, fn)
}

func (b *Bucket) _forEachNode(nid common.Nid, depth int, fn func(*node, int)) {
	n := b.pageNode(nid)
	if n == nil {
		return
	}
	fn(n, depth)
	if !n.isLeaf {
		for _, inode := range n.inodes {
			b._forEachNode(inode.Nid(), depth+1, fn)
		}
	}
}

// hasCommittedRoot reports whether the bucket has a published root node.
func (b *Bucket) hasCommittedRoot() bool {
	return b.base != nil && b.base.root != 0
}

// node returns a writable node for the given id, materializing one (COW) if
// necessary. This is the write-path entry; the read path uses pageNode.
func (b *Bucket) node(id common.Nid, parent *workNode) *workNode {
	common.Assert(b.dirty != nil, "nodes map expected (write tx)")

	if n := b.dirty[id]; n != nil {
		return n
	}

	var n *workNode
	if !b.hasCommittedRoot() {
		// No committed root yet: use/create the in-memory root (id 0 placeholder).
		if b.rootNode != nil {
			n = b.rootNode
		} else {
			n = &workNode{bucket: b, isLeaf: true}
		}
		n.parent = parent
	} else {
		n = b.nodeForWrite(id, parent)
	}

	if parent == nil {
		b.rootNode = n
	}
	b.dirty[id] = n
	b.tx.stats.IncNodeCount(1)
	return n
}

// nodeForWrite returns a writable copy of a node (COW from the base snapshot).
func (b *Bucket) nodeForWrite(id common.Nid, parent *workNode) *workNode {
	if n, ok := b.dirty[id]; ok {
		return n
	}
	if _, ok := b.obsolete[id]; ok {
		panic(fmt.Sprintf("node %d has been removed", id))
	}
	var orig *snapNode
	if b.base != nil {
		orig = b.base.nodes[id]
	}
	if orig == nil {
		panic(fmt.Sprintf("node %d not found", id))
	}
	n := orig.materializeWorkNode()
	n.parent = parent
	n.bucket = b
	n.children = nil
	n.spilled = false
	n.unbalanced = false
	b.dirty[id] = n
	if parent != nil {
		parent.children = append(parent.children, n)
	}
	return n
}

// pageNode returns a read-only node view (*snapNode) for a logical node id.
// When a bucket has a materialized but unfinalized root, id 0 (and the root's
// id) map to that in-memory root.
func (b *Bucket) pageNode(id common.Nid) *node {
	if b.rootNode != nil && (id == 0 || id == b.rootNode.nid) {
		return b.rootNode.readNodeView()
	}
	if b.dirty != nil {
		if n := b.dirty[id]; n != nil {
			return n.readNodeView()
		}
	}
	if b.obsolete != nil {
		if _, ok := b.obsolete[id]; ok {
			return nil
		}
	}
	if b.base != nil {
		return b.base.nodes[id]
	}
	return nil
}

// allocate returns a new node id for this bucket, reusing freed ids first.
func (b *Bucket) allocate() (common.Nid, error) {
	return b.tx.allocateNid(), nil
}

// spill finalizes the bucket's dirty tree (split + assign node ids).
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

// rebalance rebalances every dirty node in the bucket.
func (b *Bucket) rebalance() {
	for _, n := range b.dirty {
		n.rebalance()
	}
}

// markObsolete records that a node id is freed in this tx. Idempotent.
func (b *Bucket) markObsolete(id common.Nid) {
	if b.obsolete == nil {
		return
	}
	if _, ok := b.obsolete[id]; ok {
		return
	}
	b.obsolete[id] = struct{}{}
	b.tx.freeNid(id)
}

// dropNode removes a node from the tx-local dirty cache and marks it obsolete.
func (b *Bucket) dropNode(id common.Nid) {
	delete(b.dirty, id)
	b.markObsolete(id)
}

// buildPublishedState assembles the immutable bucketState to publish for this
// bucket from its COW base plus frozen dirty workNodes. Clean (unchanged) nodes
// are shared by pointer; only dirtied/obsolete entries differ.
func (b *Bucket) buildPublishedState() *bucketState {
	var baseNodes map[common.Nid]*snapNode
	rootNid := common.Nid(0)
	if b.base != nil {
		baseNodes = b.base.nodes
		rootNid = b.base.root
	}

	var newNodes map[common.Nid]*snapNode
	if len(b.dirty) == 0 && len(b.obsolete) == 0 {
		newNodes = baseNodes
	} else {
		newNodes = make(map[common.Nid]*snapNode, len(baseNodes)+len(b.dirty))
		for nid, n := range baseNodes {
			newNodes[nid] = n
		}
		for nid := range b.obsolete {
			delete(newNodes, nid)
		}
		for nid, n := range b.dirty {
			newNodes[nid] = n.freeze()
		}
	}

	if b.rootNode != nil {
		rootNid = b.rootNode.nid
	}

	return &bucketState{
		root:  rootNid,
		nodes: newNodes,
	}
}

// size returns the logical payload size of the bucket's tx view in bytes.
func (b *Bucket) size() int64 {
	var size int64
	var baseNodes map[common.Nid]*snapNode
	if b.base != nil {
		baseNodes = b.base.nodes
	}
	for id, n := range baseNodes {
		if _, ok := b.obsolete[id]; ok {
			continue
		}
		if _, ok := b.dirty[id]; ok {
			continue
		}
		size += int64(n.size())
	}
	for _, n := range b.dirty {
		size += int64(n.size())
	}
	return size
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
