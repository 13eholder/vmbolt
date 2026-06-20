package vmbolt

import (
	"bytes"
	"fmt"
	"sort"

	"13eholder/vmbolt/internal/common"
)

const (
	defaultMaxNodeSizeBytes  = 32 * 1024
	defaultMinNodeMergeBytes = defaultMaxNodeSizeBytes / 4
	nodeHeaderOverheadBytes  = 32
	nodeInodeOverheadBytes   = 24
	minKeysPerNode           = 2
)

// root returns the top-level node this node is attached to.
func (n *workNode) root() *workNode {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
func (n *workNode) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the serialized size of the node (used for split threshold).
func (n *workNode) size() int {
	sz := nodeHeaderOverheadBytes
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += nodeInodeOverheadBytes + len(item.Key()) + len(item.Value())
	}
	return sz
}

// sizeLessThan returns true if the node is less than a given size.
func (n *workNode) sizeLessThan(v int) bool {
	sz := nodeHeaderOverheadBytes
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += nodeInodeOverheadBytes + len(item.Key()) + len(item.Value())
		if sz >= v {
			return false
		}
	}
	return true
}

// childAt returns the child node at a given index.
func (n *workNode) childAt(index int) *workNode {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].Nid(), n)
}

// childIndex returns the index of a given child node.
func (n *workNode) childIndex(child *workNode) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].Key(), child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *workNode) numChildren() int {
	return len(n.inodes)
}

// nextSibling returns the next node with the same parent.
func (n *workNode) nextSibling() *workNode {
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
func (n *workNode) prevSibling() *workNode {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.
func (n *workNode) put(oldKey, newKey, value []byte, nId common.Nid, flags uint32) {
	if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].Key(), oldKey) != -1 })

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].Key(), oldKey)
	if !exact {
		n.inodes = append(n.inodes, common.Inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.SetFlags(flags)
	inode.SetKey(newKey)
	inode.SetValue(value)
	inode.SetNid(nId)
	common.Assert(len(inode.Key()) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
func (n *workNode) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].Key(), key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].Key(), key) {
		return
	}

	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
func (n *workNode) split() []*workNode {
	var nodes []*workNode

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
func (n *workNode) splitTwo() (*workNode, *workNode) {
	// Ignore the split if the node doesn't have at least enough nodes for
	// two nodes or if the nodes can fit in a single node.
	if len(n.inodes) <= (minKeysPerNode*2) || n.sizeLessThan(defaultMaxNodeSizeBytes) {
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
		n.parent = &workNode{bucket: n.bucket, children: []*workNode{n}}
	}

	// Create a new node and add it to the parent.
	next := &workNode{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	n.bucket.tx.stats.IncSplit(1)

	return n, next
}

// splitIndex finds the position where a node will fill a given threshold.
// It returns the index as well as the size of the first node.
// This is only be called from split().
func (n *workNode) splitIndex(threshold int) (index, sz int) {
	sz = nodeHeaderOverheadBytes

	// Loop until we only have the minimum number of keys required for the second node.
	for i := 0; i < len(n.inodes)-minKeysPerNode; i++ {
		index = i
		inode := n.inodes[i]
		elsize := nodeInodeOverheadBytes + len(inode.Key()) + len(inode.Value())

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeysPerNode && sz+elsize > threshold {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// finalize assigns logical ids to materialized nodes and updates parent links.
func (n *workNode) finalize() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].finalize(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	var nodes = n.split()
	for _, node := range nodes {
		if node.nid == 0 {
			nid, err := n.bucket.allocate()
			if err != nil {
				return err
			}
			// The node was tracked under the placeholder key 0 (unfinalized
			// root); move it to its real id so it is frozen exactly once.
			delete(n.bucket.dirty, 0)
			node.nid = nid
		}
		node.spilled = true

		n.bucket.dirty[node.nid] = node

		// Update the parent entry to point at the finalized child.
		if node.parent != nil {
			var key = node.key
			if len(key) == 0 {
				if len(node.inodes) == 0 {
					panic(fmt.Sprintf("finalize: node %d has no inodes", node.nid))
				}
				key = node.inodes[0].Key()
			}
			if len(key) == 0 {
				panic(fmt.Sprintf("finalize: zero-length key for node %d, inodes=%d, isLeaf=%v", node.nid, len(node.inodes), node.isLeaf))
			}

			node.parent.put(key, node.inodes[0].Key(), nil, node.nid, 0)
			node.key = node.inodes[0].Key()
			common.Assert(len(node.key) > 0, "finalize: zero-length node key")
		}

		// Update the statistics.
		tx.stats.IncSpill(1)
	}

	// If the root node split and created a new root then finalize that as well.
	if n.parent != nil && n.parent.nid == 0 {
		n.children = nil
		return n.parent.finalize()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *workNode) rebalance() {
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
			child := n.bucket.node(n.inodes[0].Nid(), n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			for _, inode := range n.inodes {
				if child, ok := n.bucket.dirty[inode.Nid()]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			n.bucket.dropNode(child.nid)
		}

		return
	}

	// If node has no keys then just remove it.
	if n.numChildren() == 0 {
		oldID := n.nid
		n.parent.del(n.key)
		n.parent.removeChild(n)
		n.bucket.dropNode(oldID)
		n.nid = 0
		n.parent.rebalance()
		return
	}

	common.Assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Merge with right sibling if idx == 0, otherwise left sibling.
	var leftNode, rightNode *workNode
	var useNextSibling = n.parent.childIndex(n) == 0
	if useNextSibling {
		leftNode = n
		rightNode = n.nextSibling()
	} else {
		leftNode = n.prevSibling()
		rightNode = n
	}

	// If both nodes are too small then merge them.
	// Reparent all child nodes being moved.
	oldRightID := rightNode.nid
	for _, inode := range rightNode.inodes {
		if child, ok := n.bucket.dirty[inode.Nid()]; ok {
			child.parent.removeChild(child)
			child.parent = leftNode
			child.parent.children = append(child.parent.children, child)
		}
	}

	// Copy over inodes from right node to left node and remove right node.
	leftNode.inodes = append(leftNode.inodes, rightNode.inodes...)
	n.parent.del(rightNode.key)
	n.parent.removeChild(rightNode)
	n.bucket.dropNode(oldRightID)

	// Either this node or the sibling node was deleted from the parent so rebalance it.
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
func (n *workNode) removeChild(target *workNode) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.nid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> nid=%d", trunc(item.key, 4), item.nid)
		}
	}
	warn("")
}
*/

type workNodes []*workNode

func (s workNodes) Len() int      { return len(s) }
func (s workNodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s workNodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].Key(), s[j].inodes[0].Key()) == -1
}
