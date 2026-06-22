package vmbolt

import (
	"sync"

	"github.com/13eholder/vmbolt/internal/common"
)

// node is the immutable node shape published in dbState snapshots.
// It contains only structural data that is safe to share across transactions.
type node = snapNode

type snapNode struct {
	isLeaf bool
	key    []byte
	nid    common.Nid
	inodes common.Inodes
}

func (n *snapNode) size() int {
	if n == nil {
		return 0
	}
	sz := nodeHeaderOverheadBytes
	for i := range n.inodes {
		item := &n.inodes[i]
		sz += nodeInodeOverheadBytes + len(item.Key()) + len(item.Value())
	}
	return sz
}

// workNode is the tx-local mutable node shape.
type workNode struct {
	bucket     *Bucket
	isLeaf     bool
	unbalanced bool
	spilled    bool
	key        []byte
	nid        common.Nid
	parent     *workNode
	children   workNodes
	inodes     common.Inodes
}

// workNodePool recycles transaction-local mutable node shells.
// The pooled structs are reset before reuse; inode slices and key/value bytes
// are intentionally not retained across reuse because they may still be
// referenced by immutable snapNodes.
var workNodePool = sync.Pool{
	New: func() interface{} {
		return &workNode{}
	},
}

func acquireWorkNode() *workNode {
	return workNodePool.Get().(*workNode)
}

func releaseWorkNode(n *workNode) {
	if n == nil {
		return
	}
	// Return the inode slice if it was not transferred to a snapNode by freeze().
	// freeze() sets n.inodes = nil, so a non-nil slice here is safe to recycle.
	if n.inodes != nil {
		releaseInodes(n.inodes)
	}
	// Clear references so the pool does not retain snapNode data or parent graph.
	n.bucket = nil
	n.parent = nil
	n.children = nil
	n.inodes = nil
	n.key = nil
	n.nid = 0
	n.isLeaf = false
	n.unbalanced = false
	n.spilled = false
	workNodePool.Put(n)
}

// maxPooledInodes caps the capacity of Inodes slices kept by inodesPool to
// avoid retaining very large arrays across transactions.
const maxPooledInodes = 256

// inodesPool recycles Inodes slices used by workNodes. Slices that end up
// in immutable snapNodes are not returned (ownership is transferred in freeze()).
var inodesPool = sync.Pool{
	New: func() interface{} {
		return new(common.Inodes)
	},
}

func acquireInodes(length int) common.Inodes {
	if length == 0 {
		return nil
	}
	v := inodesPool.Get()
	if v == nil {
		return make(common.Inodes, length)
	}
	s := *(v.(*common.Inodes))
	if cap(s) < length {
		// Pooled slice is too small; put it back and allocate a new one.
		inodesPool.Put(v)
		return make(common.Inodes, length)
	}
	return s[:length]
}

func releaseInodes(s common.Inodes) {
	if cap(s) > maxPooledInodes {
		return
	}
	// Clear inode headers so the pool does not retain key/value slices.
	for i := range s {
		s[i] = common.Inode{}
	}
	ps := &common.Inodes{}
	*ps = s[:cap(s)]
	inodesPool.Put(ps)
}

func (n *snapNode) materializeWorkNode() *workNode {
	if n == nil {
		return nil
	}

	wn := acquireWorkNode()
	wn.isLeaf = n.isLeaf
	wn.nid = n.nid
	// The parent snapNode is immutable after freeze(); key/value slices can be
	// shared because workNode only reassigns inode pointers (SetKey/SetValue),
	// it never mutates the underlying bytes in place.
	wn.key = n.key
	if len(n.inodes) > 0 {
		wn.inodes = acquireInodes(len(n.inodes))
		copy(wn.inodes, n.inodes)
	} else {
		wn.inodes = nil
	}
	return wn
}

func (n *workNode) freeze() *snapNode {
	if n == nil {
		return nil
	}

	// Transfer inode slice ownership directly to the immutable snapNode.
	// The workNode is discarded after commit/rollback, so sharing the slice
	// avoids one allocation + copy per dirty node.
	inodes := n.inodes
	n.inodes = nil

	return &snapNode{
		isLeaf: n.isLeaf,
		key:    n.key,
		nid:    n.nid,
		inodes: inodes,
	}
}

func (n *workNode) readNodeView() *node {
	if n == nil {
		return nil
	}
	return &snapNode{
		isLeaf: n.isLeaf,
		key:    n.key,
		nid:    n.nid,
		inodes: n.inodes,
	}
}
