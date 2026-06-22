package vmbolt

import (
	"fmt"
	"testing"

	"github.com/13eholder/vmbolt/internal/common"
)

// leafFlag is a stand-in for the legacy page leaf flag used only by these
// workNode unit tests (the engine no longer interprets page flags).
const leafFlag uint32 = 0x02

// pbsTestBucket builds a minimal Bucket/tx scaffolding for workNode unit tests.
// The B+tree mechanics only need tx.stats and FillPercent.
func pbsTestBucket() *Bucket {
	return &Bucket{tx: &Tx{db: &DB{}}, FillPercent: DefaultFillPercent}
}

// Ensure that a node can insert a key/value.
func TestNode_put(t *testing.T) {
	n := &workNode{inodes: make(common.Inodes, 0), bucket: pbsTestBucket()}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), 0, 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), 0, leafFlag)

	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].Key(), n.inodes[0].Value(); string(k) != "bar" || string(v) != "1" {
		t.Fatalf("exp=<bar,1>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].Key(), n.inodes[1].Value(); string(k) != "baz" || string(v) != "2" {
		t.Fatalf("exp=<baz,2>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].Key(), n.inodes[2].Value(); string(k) != "foo" || string(v) != "3" {
		t.Fatalf("exp=<foo,3>; got=<%s,%s>", k, v)
	}
	if n.inodes[2].Flags() != leafFlag {
		t.Fatalf("not a leaf: %d", n.inodes[2].Flags())
	}
}

// Ensure that a node can split into appropriate subgroups.
func TestNode_split(t *testing.T) {
	n := &workNode{inodes: make(common.Inodes, 0), bucket: pbsTestBucket()}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		val := []byte("01234567012345670123456701234567")
		n.put(key, key, val, 0, 0)
	}

	nodes := n.split()
	if len(nodes) <= 1 {
		t.Fatalf("expected split into multiple nodes, got=%d", len(nodes))
	}
}

// Ensure that a node with the minimum number of inodes just returns a single node.
func TestNode_split_MinKeys(t *testing.T) {
	n := &workNode{inodes: make(common.Inodes, 0), bucket: pbsTestBucket()}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)

	nodes := n.split()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got=%d", len(nodes))
	}
}

// Ensure that a node that has keys that all fit within maxNodeBytes just returns one leaf.
func TestNode_split_SinglePage(t *testing.T) {
	n := &workNode{inodes: make(common.Inodes, 0), bucket: pbsTestBucket()}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	nodes := n.split()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got=%d", len(nodes))
	}
}
