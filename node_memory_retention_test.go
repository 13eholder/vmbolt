package vmbolt

import "testing"

func TestNodeDeleteDropsRemovedInodeReference(t *testing.T) {
	n := &node{
		isLeaf: true,
		inodes: []inode{
			{key: []byte("a"), value: []byte("value-a")},
			{key: []byte("b"), value: []byte("value-b")},
			{key: []byte("c"), value: []byte("value-c")},
		},
	}

	n.del([]byte("b"))

	if got, want := len(n.inodes), 2; got != want {
		t.Fatalf("unexpected inode count after delete: got=%d want=%d", got, want)
	}
	if got := string(n.inodes[0].key); got != "a" {
		t.Fatalf("unexpected first key after delete: %q", got)
	}
	if got := string(n.inodes[1].key); got != "c" {
		t.Fatalf("unexpected second key after delete: %q", got)
	}

	backing := n.inodes[:cap(n.inodes)]
	for i := len(n.inodes); i < cap(n.inodes); i++ {
		if backing[i].key != nil || backing[i].value != nil || backing[i].child != nil {
			t.Fatalf("deleted inode reference retained at backing slot %d: key=%q valueLen=%d child=%p",
				i, backing[i].key, len(backing[i].value), backing[i].child)
		}
	}
}

func TestNodeSplitDoesNotRetainRightHalfInLeftBackingArray(t *testing.T) {
	n := newLargeValueLeafNodeForMemoryTest(t, 6, defaultMaxNodeSizeBytes/3)

	left, right := n.splitTwo()
	if right == nil {
		t.Fatal("expected oversized node to split")
	}
	if left != n {
		t.Fatalf("split returned unexpected left node: got=%p want=%p", left, n)
	}

	backing := left.inodes[:cap(left.inodes)]
	for i := len(left.inodes); i < cap(left.inodes); i++ {
		if backing[i].key != nil || backing[i].value != nil || backing[i].child != nil {
			t.Fatalf("right-half inode reference retained by left backing slot %d: key=%q valueLen=%d child=%p",
				i, backing[i].key, len(backing[i].value), backing[i].child)
		}
	}
}

func TestNodeSplitAllowsOversizeLeafWithThreeLargeValues(t *testing.T) {
	n := newLargeValueLeafNodeForMemoryTest(t, 3, defaultMaxNodeSizeBytes/2+1)

	if n.sizeLessThan(defaultMaxNodeSizeBytes) {
		t.Fatalf("test setup error: node size=%d should exceed max node size=%d", n.size(), defaultMaxNodeSizeBytes)
	}

	_, right := n.splitTwo()
	if right == nil {
		t.Fatalf("oversized leaf with %d keys did not split; size=%d max=%d",
			len(n.inodes), n.size(), defaultMaxNodeSizeBytes)
	}
}

func newLargeValueLeafNodeForMemoryTest(t *testing.T, count, valueSize int) *node {
	t.Helper()

	tx := &Tx{}
	b := &Bucket{
		tx:          tx,
		FillPercent: DefaultFillPercent,
		dirty:       make(map[*node]bool),
	}
	tx.bctx = map[string]*Bucket{"test": b}

	n := &node{
		bucket: b,
		isLeaf: true,
		inodes: make([]inode, count),
	}
	b.rootNode = n
	b.dirty[n] = true

	for i := range n.inodes {
		n.inodes[i] = inode{
			key:   []byte{byte('a' + i)},
			value: make([]byte, valueSize),
		}
	}

	return n
}
