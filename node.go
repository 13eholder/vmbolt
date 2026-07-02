package vmbolt

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/13eholder/vmbolt/internal/common"
)

const (
	defaultMaxNodeSizeBytes  = 128 * 1024
	defaultMinNodeMergeBytes = defaultMaxNodeSizeBytes / 4
	nodeHeaderOverheadBytes  = 32
	nodeInodeOverheadBytes   = 24
	minKeysPerNode           = 2
)

// root returns the top-level node this node is attached to.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the serialized size of the node (used for split threshold).
func (n *node) size() int {
	sz := nodeHeaderOverheadBytes
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += nodeInodeOverheadBytes + len(item.key) + len(item.value)
	}
	return sz
}

// sizeLessThan returns true if the node is less than a given size.
func (n *node) sizeLessThan(v int) bool {
	sz := nodeHeaderOverheadBytes
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += nodeInodeOverheadBytes + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

// childAt returns the child node at a given index, materializing (COW) the
// child into this write transaction if it is still a published (immutable) node.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.childForWrite(n, index)
}

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	return sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value (leaf) or key/child (branch).
func (n *node) put(oldKey, newKey, value []byte, child *node, flags uint32) {
	if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey)
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.child = child
	common.Assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node and clear the tail slot so removed keys,
	// values, or children do not remain reachable through the backing array.
	copy(n.inodes[index:], n.inodes[index+1:])
	n.inodes[len(n.inodes)-1] = inode{}
	n.inodes = n.inodes[:len(n.inodes)-1]

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
func (n *node) split() []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo()
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
func (n *node) splitTwo() (*node, *node) {
	// Ignore the split if the node doesn't have enough keys for two valid
	// nodes or if the node can fit in a single node.
	minKeys := n.minKeys()
	if len(n.inodes) <= (minKeys*2) || n.sizeLessThan(defaultMaxNodeSizeBytes) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(defaultMaxNodeSizeBytes) * fillPercent)

	// Determine split position and sizes of the two nodes.
	splitIndex, _ := n.splitIndex(threshold)

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket}
		n.bucket.dirty[n.parent] = true
	}

	// Create a new node and add it to the parent.
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.bucket.dirty[next] = true

	// Split inodes across two independent slices. Keeping the old backing array
	// would retain references to large values from the other half of the split.
	leftInodes := append([]inode(nil), n.inodes[:splitIndex]...)
	rightInodes := append([]inode(nil), n.inodes[splitIndex:]...)
	for i := range n.inodes {
		n.inodes[i] = inode{}
	}
	n.inodes = leftInodes
	next.inodes = rightInodes

	// Update the statistics.
	n.bucket.tx.stats.IncSplit(1)

	return n, next
}

// splitIndex finds the position where a node will fill a given threshold.
// It returns the index as well as the size of the first node.
// This is only be called from split().
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = nodeHeaderOverheadBytes

	// Loop until we only have the minimum number of keys required for the second node.
	minKeys := n.minKeys()
	for i := 0; i < len(n.inodes)-minKeys; i++ {
		index = i
		inode := n.inodes[i]
		elsize := nodeInodeOverheadBytes + len(inode.key) + len(inode.value)

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeys && sz+elsize > threshold {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// finalize spills the subtree under n: it recursively finalizes copied children,
// splits oversize nodes, and wires parent inodes to point at the finalized
// children. There are no node ids to assign; parent/child linkage is by pointer.
func (n *node) finalize() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// Spill copied children first (descend via inodes.child for nodes that this
	// tx has materialized). Published (shared) children need no work.
	for i := range n.inodes {
		c := n.inodes[i].child
		if c != nil && n.bucket.dirty[c] {
			if err := c.finalize(); err != nil {
				return err
			}
		}
	}

	// Split nodes into appropriate sizes. The first node will always be n.
	var nodes = n.split()
	for _, node := range nodes {
		node.spilled = true

		// Update the parent entry to point at the finalized child.
		if node.parent != nil {
			var key = node.key
			if len(key) == 0 {
				if len(node.inodes) == 0 {
					panic("finalize: node has no inodes")
				}
				key = node.inodes[0].key
			}
			if len(key) == 0 {
				panic(fmt.Sprintf("finalize: zero-length key, inodes=%d, isLeaf=%v", len(node.inodes), node.isLeaf))
			}

			node.parent.put(key, node.inodes[0].key, nil, node, 0)
			node.key = node.inodes[0].key
			common.Assert(len(node.key) > 0, "finalize: zero-length node key")
		}

		// Update the statistics.
		tx.stats.IncSpill(1)
	}

	// If the root node split and created a new root then finalize that as well.
	if n.parent != nil && !n.parent.spilled {
		return n.parent.finalize()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Update statistics.
	n.bucket.tx.stats.IncRebalance(1)

	// Ignore if node is above threshold (25% when FillPercent is set to DefaultFillPercent) and has enough keys.
	var threshold = defaultMinNodeMergeBytes
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move root's child up.
			child := n.childAt(0)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]

			// Reparent all child nodes being moved.
			for _, inode := range n.inodes {
				if inode.child != nil && n.bucket.dirty[inode.child] {
					inode.child.parent = n
				}
			}

			// Detach old child.
			child.parent = nil
		}

		return
	}

	// If node has no keys then just remove it.
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.rebalance()
		return
	}

	common.Assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Merge with right sibling if idx == 0, otherwise left sibling.
	var leftNode, rightNode *node
	var useNextSibling = n.parent.childIndex(n) == 0
	if useNextSibling {
		leftNode = n
		rightNode = n.nextSibling()
	} else {
		leftNode = n.prevSibling()
		rightNode = n
	}

	// If both nodes are too small then merge them.
	// Reparent all child nodes being moved from right to left.
	for _, inode := range rightNode.inodes {
		if inode.child != nil && rightNode.bucket.dirty[inode.child] {
			inode.child.parent = leftNode
		}
	}

	// Copy over inodes from right node to left node and remove right node.
	leftNode.inodes = append(leftNode.inodes, rightNode.inodes...)
	n.parent.del(rightNode.key)
	n.parent.rebalance()
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE {type=%s count=%d}]", typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
		} else {
			warnf("+B %08x -> child=%p", trunc(item.key, 4), item.child)
		}
	}
	warn("")
}
*/
