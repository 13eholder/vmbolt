package vmbolt

import (
	"testing"

	"13eholder/vmbolt/internal/common"
)

// TestBucketHandle_AllocFreeRecycle exercises the per-bucket node-id allocator:
// ids carry the bucket's prefix, freed ids are reused (LIFO), and the monotonic
// counter only advances when the freelist is empty.
func TestBucketHandle_AllocFreeRecycle(t *testing.T) {
	h := &bucketHandle{id: common.BucketId(7)}

	n0 := h.allocNode()
	n1 := h.allocNode()
	if n0.NodeId() != 0 || n1.NodeId() != 1 {
		t.Fatalf("expected node ids 0,1; got %d,%d", n0.NodeId(), n1.NodeId())
	}
	if n0.BucketOf() != 7 || n1.BucketOf() != 7 {
		t.Fatalf("nid bucket prefix wrong: %d,%d", n0.BucketOf(), n1.BucketOf())
	}

	// Free node 0, then allocate: the freed id must come back (LIFO).
	h.freeNode(0)
	n2 := h.allocNode()
	if n2.NodeId() != 0 {
		t.Fatalf("expected recycled node id 0; got %d", n2.NodeId())
	}

	// Freelist empty again: next alloc advances the counter (2, not reused).
	n3 := h.allocNode()
	if n3.NodeId() != 2 {
		t.Fatalf("expected fresh node id 2; got %d", n3.NodeId())
	}
}
