package vmbolt

import (
	"fmt"
	"testing"
)

// leafFlag is a stand-in for the legacy page leaf flag used only by these
// node unit tests (the engine no longer interprets page flags).
const leafFlag uint32 = 0x02

// pbsTestBucket builds a minimal Bucket/tx scaffolding for node unit tests.
// The B+tree mechanics only need tx.stats, FillPercent, and a dirty set
// (splitTwo registers new parent/sibling nodes in it).
func pbsTestBucket() *Bucket {
	return &Bucket{
		tx:          &Tx{db: &DB{}},
		FillPercent: DefaultFillPercent,
		dirty:       make(map[*node]bool),
	}
}

// Ensure that a node can insert a key/value.
func TestNode_put(t *testing.T) {
	n := &node{inodes: make([]inode, 0), bucket: pbsTestBucket()}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), nil, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), nil, 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), nil, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), nil, leafFlag)

	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "1" {
		t.Fatalf("exp=<bar,1>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "baz" || string(v) != "2" {
		t.Fatalf("exp=<baz,2>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "foo" || string(v) != "3" {
		t.Fatalf("exp=<foo,3>; got=<%s,%s>", k, v)
	}
	if n.inodes[2].flags != leafFlag {
		t.Fatalf("not a leaf: %d", n.inodes[2].flags)
	}
}

// Ensure that a node can split into appropriate subgroups.
func TestNode_split(t *testing.T) {
	n := &node{inodes: make([]inode, 0), bucket: pbsTestBucket()}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		val := []byte("01234567012345670123456701234567")
		n.put(key, key, val, nil, 0)
	}

	nodes := n.split()
	if len(nodes) <= 1 {
		t.Fatalf("expected split into multiple nodes, got=%d", len(nodes))
	}
}

// Ensure that a node with the minimum number of inodes just returns a single node.
func TestNode_split_MinKeys(t *testing.T) {
	n := &node{inodes: make([]inode, 0), bucket: pbsTestBucket()}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), nil, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), nil, 0)

	nodes := n.split()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got=%d", len(nodes))
	}
}

// Ensure that a node that has keys that all fit within maxNodeBytes just returns one leaf.
func TestNode_split_SinglePage(t *testing.T) {
	n := &node{inodes: make([]inode, 0), bucket: pbsTestBucket()}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), nil, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), nil, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), nil, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), nil, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), nil, 0)

	nodes := n.split()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got=%d", len(nodes))
	}
}
