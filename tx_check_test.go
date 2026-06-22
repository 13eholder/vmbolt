package vmbolt

import (
	"strings"
	"testing"

	"github.com/13eholder/vmbolt/internal/common"
)

// These are whitebox tests for the structural Check validators
// (checkBucket / checkDBState). Each case constructs a deliberately malformed
// immutable node graph and asserts the validator catches it; the well-formed
// cases must yield zero errors.

// --- test builders ---

// leafSn builds a leaf snapNode from already-sorted key/value pairs.
func leafSn(nid common.Nid, kvs ...[2]string) *snapNode {
	inodes := make([]common.Inode, len(kvs))
	for i, kv := range kvs {
		inodes[i] = common.NewInode(0, 0, []byte(kv[0]), []byte(kv[1]))
	}
	return &snapNode{nid: nid, isLeaf: true, inodes: inodes}
}

// branchSn builds a branch snapNode whose separator keys equal each child's
// first key (the vmbolt convention enforced by finalize()).
func branchSn(nid common.Nid, children ...*snapNode) *snapNode {
	inodes := make([]common.Inode, len(children))
	for i, c := range children {
		inodes[i] = common.NewInode(0, c.nid, c.inodes[0].Key(), nil)
	}
	return &snapNode{nid: nid, isLeaf: false, inodes: inodes}
}

func bucketStateFrom(root common.Nid, nodes ...*snapNode) *bucketState {
	m := make(map[common.Nid]*snapNode, len(nodes))
	for _, n := range nodes {
		m[n.nid] = n
	}
	return &bucketState{root: root, nodes: m}
}

// --- helpers ---

func assertNoCheckErrs(t *testing.T, errs []error) {
	t.Helper()
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %d: %v", len(errs), errs)
	}
}

func assertCheckErrContaining(t *testing.T, errs []error, substr string) {
	t.Helper()
	for _, e := range errs {
		if strings.Contains(e.Error(), substr) {
			return
		}
	}
	t.Fatalf("expected an error containing %q, got %d error(s): %v", substr, len(errs), errs)
}

// --- Level B: per-bucket tree ---

func TestCheck_BucketWellFormed(t *testing.T) {
	// Two-level tree: root branch -> two leaves.
	left := leafSn(2, [2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"})
	right := leafSn(3, [2]string{"m", "4"}, [2]string{"z", "5"})
	root := branchSn(1, left, right)
	bs := bucketStateFrom(1, root, left, right)
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_BucketSingleLeafRoot(t *testing.T) {
	n := leafSn(1, [2]string{"a", "1"}, [2]string{"b", "2"})
	bs := bucketStateFrom(1, n)
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_BucketEmptyRoot(t *testing.T) {
	// A bucket emptied of all keys keeps a leaf root with zero inodes.
	n := &snapNode{nid: 1, isLeaf: true}
	bs := bucketStateFrom(1, n)
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_DanglingChild(t *testing.T) {
	root := &snapNode{nid: 1, isLeaf: false, inodes: []common.Inode{
		common.NewInode(0, 99, []byte("a"), nil), // child 99 absent
	}}
	bs := bucketStateFrom(1, root)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "missing child")
}

func TestCheck_OrphanNode(t *testing.T) {
	left := leafSn(2, [2]string{"a", "1"})
	root := branchSn(1, left)
	orphan := leafSn(7, [2]string{"x", "9"}) // unreachable
	bs := bucketStateFrom(1, root, left, orphan)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "unreachable")
}

func TestCheck_CycleSharedChild(t *testing.T) {
	// Root points at itself: the node is reachable via two "parents".
	root := &snapNode{nid: 1, isLeaf: false, inodes: []common.Inode{
		common.NewInode(0, 1, []byte("a"), nil),
	}}
	bs := bucketStateFrom(1, root)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "multiple parents")
}

func TestCheck_UnsortedKeys(t *testing.T) {
	n := leafSn(1, [2]string{"a", "1"}, [2]string{"c", "3"}, [2]string{"b", "2"})
	bs := bucketStateFrom(1, n)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "not strictly ascending")
}

func TestCheck_SeparatorMismatch(t *testing.T) {
	child := leafSn(2, [2]string{"a", "1"})
	root := &snapNode{nid: 1, isLeaf: false, inodes: []common.Inode{
		common.NewInode(0, 2, []byte("WRONG"), nil), // separator != child first key "a"
	}}
	bs := bucketStateFrom(1, root, child)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "separator")
}

func TestCheck_LeafInodeWithChildNid(t *testing.T) {
	n := &snapNode{nid: 1, isLeaf: true, inodes: []common.Inode{
		common.NewInode(0, 5, []byte("a"), []byte("1")), // leaf inode carrying a child nid
	}}
	bs := bucketStateFrom(1, n)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "non-zero child nid")
}

func TestCheck_BranchInodeWithZeroNid(t *testing.T) {
	n := &snapNode{nid: 1, isLeaf: false, inodes: []common.Inode{
		common.NewInode(0, 0, []byte("a"), nil), // branch inode with no child pointer
	}}
	bs := bucketStateFrom(1, n)
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "zero child nid")
}

func TestCheck_RootMissingFromNodes(t *testing.T) {
	bs := &bucketState{root: 1, nodes: map[common.Nid]*snapNode{}}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "root nid")
}

func TestCheck_EmptyBucketWithOrphanNodes(t *testing.T) {
	n := leafSn(1, [2]string{"a", "1"})
	bs := &bucketState{root: 0, nodes: map[common.Nid]*snapNode{1: n}}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "orphan nodes")
}

func TestCheck_NodeSelfNidMismatch(t *testing.T) {
	// Node keyed by 9 in the map but its self.nid is 2. This invariant is
	// validated at Level A (checkDBState), which scans every bucket's nodes.
	n := leafSn(2, [2]string{"a", "1"})
	bs := &bucketState{root: 9, nodes: map[common.Nid]*snapNode{9: n}}
	s := &dbState{
		buckets: map[string]*bucketState{"x": bs},
		nextNid: 10,
	}
	assertCheckErrContaining(t, checkDBState(s), "self.nid")
}

// --- Level A: dbState / global nid allocator ---

func TestCheck_DBStateWellFormed(t *testing.T) {
	left := leafSn(2, [2]string{"a", "1"})
	root := branchSn(1, left)
	bs := bucketStateFrom(1, root, left)
	s := &dbState{
		buckets: map[string]*bucketState{"x": bs},
		nextNid: 3,
	}
	assertNoCheckErrs(t, checkDBState(s))
}

func TestCheck_DuplicateNidAcrossBuckets(t *testing.T) {
	b1 := bucketStateFrom(2, leafSn(2, [2]string{"a", "1"}))
	b2 := bucketStateFrom(2, leafSn(2, [2]string{"z", "9"})) // same nid 2
	s := &dbState{
		buckets: map[string]*bucketState{"x": b1, "y": b2},
		nextNid: 10,
	}
	assertCheckErrContaining(t, checkDBState(s), "owned by two buckets")
}

func TestCheck_FreeNidLive(t *testing.T) {
	bs := bucketStateFrom(2, leafSn(2, [2]string{"a", "1"}))
	s := &dbState{
		buckets: map[string]*bucketState{"x": bs},
		nextNid: 10,
		freeNid: []common.Nid{2}, // 2 is live
	}
	assertCheckErrContaining(t, checkDBState(s), "both free and live")
}

func TestCheck_NextNidHighWater(t *testing.T) {
	bs := bucketStateFrom(5, leafSn(5, [2]string{"a", "1"}))
	s := &dbState{
		buckets: map[string]*bucketState{"x": bs},
		nextNid: 5, // live nid 5 >= nextNid 5
	}
	assertCheckErrContaining(t, checkDBState(s), ">= nextNid")
}

func TestCheck_DuplicateFreeNid(t *testing.T) {
	s := &dbState{
		buckets: map[string]*bucketState{},
		nextNid: 10,
		freeNid: []common.Nid{3, 3},
	}
	assertCheckErrContaining(t, checkDBState(s), "duplicate free nid")
}

func TestCheck_NilBucketState(t *testing.T) {
	s := &dbState{
		buckets: map[string]*bucketState{"x": nil},
		nextNid: 1,
	}
	assertCheckErrContaining(t, checkDBState(s), "nil state")
}
