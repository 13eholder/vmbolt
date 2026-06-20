package vmbolt

import (
	"bytes"
	"fmt"
	"sort"

	"13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/common"
)

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket
// in lexicographical order.
//
// It has two modes:
//   - tree mode (bucket != nil): walks a single bucket's B+tree (via Bucket.Cursor());
//   - directory mode (bucket == nil, dir set): enumerates the top-level bucket NAMES in
//     sorted order. Produced by Tx.Cursor(); used by snapshot/Hash/defrag-style callers
//     that iterate all top-level buckets deterministically.
type Cursor struct {
	bucket *Bucket
	stack  []elemRef

	// directory mode: a sorted snapshot of top-level bucket names captured when the
	// cursor was created. dirIdx is the current position (-1 / len(dir) => exhausted).
	dir    [][]byte
	dirIdx int
}

func (c *Cursor) isDir() bool { return c.bucket == nil }

// Bucket returns the bucket that this cursor was created from. Returns nil for a
// directory cursor (Tx.Cursor()).
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First moves the cursor to the first item in the bucket and returns its key and value.
func (c *Cursor) First() (key []byte, value []byte) {
	if c.isDir() {
		c.dirIdx = 0
		return c.dirKeyValue()
	}
	common.Assert(c.bucket.tx.db != nil, "tx closed")
	k, v, _ := c.first()
	return k, v
}

func (c *Cursor) first() (key []byte, value []byte, flags uint32) {
	c.stack = c.stack[:0]
	n := c.bucket.pageNode(c.bucket.Root())
	c.stack = append(c.stack, elemRef{node: n, index: 0})
	c.goToFirstElementOnTheStack()

	// If we land on an empty node then move to the next value.
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}
	return c.keyValue()
}

// Last moves the cursor to the last item in the bucket and returns its key and value.
func (c *Cursor) Last() (key []byte, value []byte) {
	if c.isDir() {
		if len(c.dir) == 0 {
			c.dirIdx = 0
			return nil, nil
		}
		c.dirIdx = len(c.dir) - 1
		return c.dirKeyValue()
	}
	common.Assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	n := c.bucket.pageNode(c.bucket.Root())
	ref := elemRef{node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()

	// If this is an empty node we call prev to find the last node that is not empty
	for len(c.stack) > 1 && c.stack[len(c.stack)-1].count() == 0 {
		c.prev()
	}

	if len(c.stack) == 0 {
		return nil, nil
	}
	k, v, _ := c.keyValue()
	return k, v
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
func (c *Cursor) Next() (key []byte, value []byte) {
	if c.isDir() {
		c.dirIdx++
		return c.dirKeyValue()
	}
	common.Assert(c.bucket.tx.db != nil, "tx closed")
	k, v, _ := c.next()
	return k, v
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
func (c *Cursor) Prev() (key []byte, value []byte) {
	if c.isDir() {
		c.dirIdx--
		return c.dirKeyValue()
	}
	common.Assert(c.bucket.tx.db != nil, "tx closed")
	k, v, _ := c.prev()
	return k, v
}

// Seek moves the cursor to a given key using a b-tree search and returns it.
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	if c.isDir() {
		c.dirIdx = sort.Search(len(c.dir), func(i int) bool {
			return bytes.Compare(c.dir[i], seek) >= 0
		})
		return c.dirKeyValue()
	}
	common.Assert(c.bucket.tx.db != nil, "tx closed")

	k, v, _ := c.seek(seek)

	// If we ended up after the last element of a node then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, _ = c.next()
	}
	return k, v
}

// Delete removes the current key/value under the cursor from the bucket.
func (c *Cursor) Delete() error {
	if c.isDir() {
		// A directory cursor yields bucket names, not KV pairs.
		return errors.ErrIncompatibleValue
	}
	if c.bucket.tx.db == nil {
		return errors.ErrTxClosed
	} else if !c.bucket.Writable() {
		return errors.ErrTxNotWritable
	}

	key, _, _ := c.keyValue()
	c.node().del(key)
	return nil
}

// dirKeyValue returns the current directory entry (bucket name) or nil when exhausted.
func (c *Cursor) dirKeyValue() ([]byte, []byte) {
	if c.dirIdx < 0 || c.dirIdx >= len(c.dir) {
		return nil, nil
	}
	return c.dir[c.dirIdx], nil
}

// seek moves the cursor to a given key and returns it.
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	// Start from root node and traverse to correct node.
	c.stack = c.stack[:0]
	c.search(seek, c.bucket.Root())
	return c.keyValue()
}

// first moves the cursor to the first leaf element under the last node in the stack.
func (c *Cursor) goToFirstElementOnTheStack() {
	for {
		// Exit when we hit a leaf node.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding nodes pointing to the first element to the stack.
		nId := ref.node.inodes[ref.index].Nid()
		n := c.bucket.pageNode(nId)
		c.stack = append(c.stack, elemRef{node: n, index: 0})
	}
}

// last moves the cursor to the last leaf element under the last node in the stack.
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf node.
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding nodes pointing to the last element in the stack.
		nId := ref.node.inodes[ref.index].Nid()
		n := c.bucket.pageNode(nId)

		var nextRef = elemRef{node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each node in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		// If we've hit the root node then stop and return.
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf node.
		c.stack = c.stack[:i+1]
		c.goToFirstElementOnTheStack()

		// If this is an empty node then restart and move back up the stack.
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// prev moves the cursor to the previous item in the bucket and returns the key and value.
func (c *Cursor) prev() (key []byte, value []byte, flags uint32) {
	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each node in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		// If we've hit the beginning, we should stop moving the cursor,
		// and stay at the first element, so that users can continue to
		// iterate over the elements in reverse direction by calling `Next`.
		// We should return nil in such case.
		// Refer to https://github.com/etcd-io/vmbolt/issues/733
		if len(c.stack) == 1 {
			c.first()
			return nil, nil, 0
		}
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil, 0
	}

	// Move down the stack to find the last element of the last leaf under this branch.
	c.last()
	return c.keyValue()
}

// search recursively performs a binary search against a given node until it finds a given key.
func (c *Cursor) search(key []byte, nId common.Nid) {
	n := c.bucket.pageNode(nId)
	if n == nil {
		panic(fmt.Sprintf("node %d not found", nId))
	}
	e := elemRef{node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf node then find the specific node.
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	c.searchNode(key, n)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(n.inodes[i].Key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next node.
	c.search(key, n.inodes[index].Nid())
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	n := e.node

	index := sort.Search(len(n.inodes), func(i int) bool {
		return bytes.Compare(n.inodes[i].Key(), key) != -1
	})
	e.index = index
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of node then return nil.
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// Retrieve value from node.
	inode := &ref.node.inodes[ref.index]
	return inode.Key(), inode.Value(), inode.Flags()
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *workNode {
	common.Assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// Start from root and traverse down the hierarchy.
	n := c.bucket.node(c.bucket.Root(), nil)
	for _, ref := range c.stack[:len(c.stack)-1] {
		common.Assert(!n.isLeaf, "expected branch node")
		n = n.childAt(ref.index)
	}
	common.Assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given node.
type elemRef struct {
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf node.
func (r *elemRef) isLeaf() bool {
	return r.node.isLeaf
}

// count returns the number of inodes.
func (r *elemRef) count() int {
	return len(r.node.inodes)
}
