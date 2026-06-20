package vmbolt

import (
	"errors"
	"fmt"

	berrors "13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/common"
)

// Cohort is an opaque handle to an atomic bucket group created by DB.NewCohort.
// Buckets assigned to the same Cohort (via Tx.AssignCohort) are published and
// observed atomically together: a reader never sees one member at a newer
// generation than another. Buckets not in any cohort keep being published
// independently.
//
// Use case: etcd stores consistent_index in a `meta` bucket and the MVCC tree in
// a `key` bucket, and requires them to advance atomically. Putting just those two
// in a cohort gives the guarantee without re-coupling every other bucket.
type Cohort struct{ c *bucketCohort }

// NewCohort creates a new, empty atomic bucket group.
func (db *DB) NewCohort() *Cohort {
	return &Cohort{c: &bucketCohort{}}
}

// AssignCohort makes name a member of cohort c: it creates the bucket if it does
// not exist, or adopts the existing bucket into the cohort. All members of a
// cohort are published and observed atomically. Must be called in a write tx.
//
// A bucket can only belong to one cohort; assigning an already-cohort bucket to
// a different cohort is an error. Assigning to the same cohort it already has is
// a no-op that returns the bucket.
func (tx *Tx) AssignCohort(name []byte, c *Cohort) (*Bucket, error) {
	if tx.db == nil {
		return nil, berrors.ErrTxClosed
	}
	if !tx.writable {
		return nil, berrors.ErrTxNotWritable
	}
	if c == nil {
		return nil, errors.New("vmbolt: nil cohort")
	}
	if len(name) == 0 {
		return nil, berrors.ErrBucketNameRequired
	}
	key := string(name)

	// Already created in this tx?
	if h := tx.created[key]; h != nil {
		if h.cohort != nil && h.cohort != c.c {
			return nil, fmt.Errorf("vmbolt: bucket %q already in a different cohort", key)
		}
		if h.cohort != c.c {
			h.cohort = c.c
			tx.markCohortDirty(c.c)
		}
		return tx.openHandle(key, h), nil
	}

	// Already committed?
	tx.db.bucketsMu.RLock()
	h := tx.db.buckets[key]
	tx.db.bucketsMu.RUnlock()
	if h != nil {
		if h.cohort != nil && h.cohort != c.c {
			return nil, fmt.Errorf("vmbolt: bucket %q already in a different cohort", key)
		}
		if h.cohort != c.c {
			// Adopt an independent bucket: carry its current state into the
			// cohort snapshot at commit.
			cur := h.state.Load()
			h.cohort = c.c
			if cur != nil {
				if tx.adoptedStates == nil {
					tx.adoptedStates = map[string]*bucketState{}
				}
				tx.adoptedStates[key] = cur
			}
			tx.markCohortDirty(c.c)
		}
		return tx.openHandle(key, h), nil
	}

	// Create a new bucket as a cohort member.
	id := tx.db.allocBucketId()
	nh := &bucketHandle{id: id, name: key, cohort: c.c}
	b := &Bucket{
		tx:          tx,
		name:        key,
		handle:      nh,
		dirty:       make(map[common.Nid]*workNode),
		obsolete:    make(map[common.Nid]struct{}),
		FillPercent: DefaultFillPercent,
	}
	b.rootNode = &workNode{bucket: b, isLeaf: true}
	if tx.created == nil {
		tx.created = make(map[string]*bucketHandle)
	}
	tx.created[key] = nh
	delete(tx.deleted, key)
	tx.markCohortDirty(c.c)
	tx.putBctx(key, b)
	return b, nil
}

// openHandle returns a tx-local Bucket bound to an existing (committed or
// tx-created) handle, caching it in tx.bctx. Shared tail of Bucket() and
// AssignCohort().
func (tx *Tx) openHandle(key string, h *bucketHandle) *Bucket {
	if b := tx.bctx[key]; b != nil {
		return b
	}
	b := newBucketForHandle(tx, key, h)
	tx.putBctx(key, b)
	return b
}

// pinCohort returns the cohortSnapshot this tx has pinned for c, loading and
// caching it on first access so all members are read from one consistent
// generation.
func (tx *Tx) pinCohort(c *bucketCohort) *cohortSnapshot {
	if tx.cohortSnaps != nil {
		if s, ok := tx.cohortSnaps[c]; ok {
			return s
		}
	}
	s := c.state.Load()
	if tx.cohortSnaps == nil {
		tx.cohortSnaps = map[*bucketCohort]*cohortSnapshot{}
	}
	tx.cohortSnaps[c] = s
	return s
}

// markCohortDirty records that cohort c must have its snapshot rebuilt at commit
// (a member was touched, created, adopted, or deleted in this tx).
func (tx *Tx) markCohortDirty(c *bucketCohort) {
	if tx.dirtyCohorts == nil {
		tx.dirtyCohorts = map[*bucketCohort]struct{}{}
	}
	tx.dirtyCohorts[c] = struct{}{}
}
