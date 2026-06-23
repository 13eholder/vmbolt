package vmbolt

import (
	"strings"
	"testing"
)

// These are whitebox tests for the structural Check validators
// (checkBucket / checkDBState) over the unified *node tree. Each case
// constructs a deliberately malformed published tree and asserts the validator
// catches it; the well-formed cases must yield zero errors.

// --- test builders ---

// leafNode builds a leaf node from already-sorted key/value pairs.
func leafNode(kvs ...[2]string) *node {
	inodes := make([]inode, len(kvs))
	for i, kv := range kvs {
		inodes[i] = inode{key: []byte(kv[0]), value: []byte(kv[1])}
	}
	return &node{isLeaf: true, inodes: inodes}
}

// branchNode builds a branch node whose separator keys equal each child's
// first key (the vmbolt convention enforced by finalize()).
func branchNode(children ...*node) *node {
	inodes := make([]inode, len(children))
	for i, c := range children {
		inodes[i] = inode{key: c.inodes[0].key, child: c}
	}
	return &node{isLeaf: false, inodes: inodes}
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
	left := leafNode([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"})
	right := leafNode([2]string{"m", "4"}, [2]string{"z", "5"})
	root := branchNode(left, right)
	bs := &bucketState{root: root}
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_BucketSingleLeafRoot(t *testing.T) {
	bs := &bucketState{root: leafNode([2]string{"a", "1"}, [2]string{"b", "2"})}
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_BucketEmptyRoot(t *testing.T) {
	// A bucket emptied of all keys has a nil published root.
	bs := &bucketState{root: nil}
	assertNoCheckErrs(t, checkBucket("b", bs, newCheckConfig(nil)))
}

func TestCheck_DanglingChild(t *testing.T) {
	// A branch inode with no child pointer.
	root := &node{isLeaf: false, inodes: []inode{{key: []byte("a"), child: nil}}}
	bs := &bucketState{root: root}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "nil child")
}

func TestCheck_CycleSharedChild(t *testing.T) {
	// Root points at itself: the node is reachable via two parents.
	root := &node{isLeaf: false, inodes: []inode{{key: []byte("a"), child: nil}}}
	root.inodes[0].child = root // cycle
	bs := &bucketState{root: root}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "multiple parents")
}

func TestCheck_UnsortedKeys(t *testing.T) {
	bs := &bucketState{root: leafNode([2]string{"a", "1"}, [2]string{"c", "3"}, [2]string{"b", "2"})}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "not strictly ascending")
}

func TestCheck_SeparatorMismatch(t *testing.T) {
	child := leafNode([2]string{"a", "1"})
	root := &node{isLeaf: false, inodes: []inode{{key: []byte("WRONG"), child: child}}}
	bs := &bucketState{root: root}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "separator")
}

func TestCheck_LeafInodeWithChild(t *testing.T) {
	other := leafNode([2]string{"x", "9"})
	n := &node{isLeaf: true, inodes: []inode{{key: []byte("a"), value: []byte("1"), child: other}}}
	bs := &bucketState{root: n}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "non-nil child")
}

func TestCheck_BranchInodeWithValue(t *testing.T) {
	child := leafNode([2]string{"a", "1"})
	root := &node{isLeaf: false, inodes: []inode{{key: []byte("a"), value: []byte("oops"), child: child}}}
	bs := &bucketState{root: root}
	assertCheckErrContaining(t, checkBucket("b", bs, newCheckConfig(nil)), "carries a value")
}

// --- Level A: dbState ---

func TestCheck_DBStateWellFormed(t *testing.T) {
	left := leafNode([2]string{"a", "1"})
	bs := &bucketState{root: branchNode(left)}
	s := &dbState{buckets: map[string]*bucketState{"x": bs}}
	assertNoCheckErrs(t, checkDBState(s))
}

func TestCheck_NilBucketState(t *testing.T) {
	s := &dbState{buckets: map[string]*bucketState{"x": nil}}
	assertCheckErrContaining(t, checkDBState(s), "nil state")
}
