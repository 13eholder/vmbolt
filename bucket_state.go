package vmbolt

import (
	"sync/atomic"

	"13eholder/vmbolt/internal/common"
)

// bucketState is the immutable, published per-bucket generation. Readers observe
// it via bucketHandle.state.Load(); a write transaction builds a fresh one and
// Stores it atomically. Once published it is never mutated.
type bucketState struct {
	id    common.BucketId
	root  common.Nid               // full Nid of the root node (0 while a new bucket is unfinalized)
	nodes map[common.Nid]*snapNode // this bucket's B+tree, keyed by full Nid
}

// bucketHandle is the long-lived, DB-level handle for one top-level bucket. It
// is the entry stored in DB.buckets[name]. It carries:
//   - state: an atomic pointer to the current published bucketState (lock-free reads);
//   - nextNodeId / freeNodeIds: writer-private node-id allocation state for the
//     low-48 NodeId space.
//
// The allocation counters are only ever mutated by the single in-flight writer
// (the DB holds a global write lock), so they require no synchronization of
// their own; a write tx snapshots them on first touch to allow clean rollback.
type bucketHandle struct {
	id   common.BucketId
	name string

	state atomic.Pointer[bucketState]

	// cohort, if non-nil, makes this bucket a member of an atomic group: its
	// published state then lives in cohort.state's snapshot (NOT in state), and
	// readers observe all members of the cohort at one consistent generation.
	// nil (the default) keeps the bucket published independently via state.
	cohort *bucketCohort

	// Writer-private node-id allocator (low-48 NodeId space of this bucket).
	nextNodeId  uint64
	freeNodeIds []uint64 // LIFO stack of recyclable node ids
}

// bucketCohort is an atomic publication group. Member buckets share a single
// atomic.Pointer[cohortSnapshot]: a reader that loads it observes every member
// at one consistent generation (jointly atomic), while non-member buckets keep
// being published independently. This lets etcd keep its `meta`+`key` buckets
// jointly consistent (consistent_index vs data) without re-coupling the rest.
type bucketCohort struct {
	state atomic.Pointer[cohortSnapshot]
}

// cohortSnapshot is the immutable, jointly-published set of member states.
type cohortSnapshot struct {
	members map[string]*bucketState
}

// publishedStateOf returns the bucket's currently-published state: from its
// cohort snapshot if it is a cohort member, else from its own atomic pointer.
// For a cohort member not yet present in a published snapshot (just adopted or
// freshly created this tx), it falls back to its own pointer.
func publishedStateOf(h *bucketHandle) *bucketState {
	if h.cohort != nil {
		if snap := h.cohort.state.Load(); snap != nil {
			if s, ok := snap.members[h.name]; ok {
				return s
			}
		}
		return h.state.Load()
	}
	return h.state.Load()
}

// allocNode returns a fresh Nid for this bucket, reusing a freed node id when
// one is available (LIFO, cache-friendly) and otherwise advancing the monotonic
// counter. The returned Nid always carries this bucket's id in its high 16 bits.
func (h *bucketHandle) allocNode() common.Nid {
	if n := len(h.freeNodeIds); n > 0 {
		node := h.freeNodeIds[n-1]
		h.freeNodeIds = h.freeNodeIds[:n-1]
		return common.MakeNid(h.id, node)
	}
	node := h.nextNodeId
	h.nextNodeId++
	return common.MakeNid(h.id, node)
}

// freeNode returns a bucket-local node id to the freelist for reuse by a later
// allocation in the same bucket. node is the low-48 NodeId (Nid.NodeId()).
func (h *bucketHandle) freeNode(node uint64) {
	h.freeNodeIds = append(h.freeNodeIds, node)
}
