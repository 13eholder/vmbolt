package vmbolt

import (
	"fmt"

	berrors "13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/common"
)

// CommitGroup is an opaque handle to an atomic bucket publication group created
// by DB.NewCommitGroup. Buckets created into the same CommitGroup are published
// and observed atomically together.
type CommitGroup struct{ g *bucketCommitGroup }

// BucketOptions controls how a top-level bucket is created.
type BucketOptions struct {
	CommitGroup *CommitGroup
}

// NewCommitGroup creates a new, empty atomic bucket group.
func (db *DB) NewCommitGroup() *CommitGroup {
	return &CommitGroup{g: &bucketCommitGroup{}}
}

// CreateBucketWithOptions creates a new top-level bucket with explicit options.
func (tx *Tx) CreateBucketWithOptions(name []byte, options *BucketOptions) (*Bucket, error) {
	if tx.db == nil {
		return nil, berrors.ErrTxClosed
	}
	if !tx.writable {
		return nil, berrors.ErrTxNotWritable
	}
	if len(name) == 0 {
		return nil, berrors.ErrBucketNameRequired
	}
	key := string(name)
	if _, deleted := tx.deleted[key]; !deleted {
		if tx.created[key] != nil || tx.bucketExistsCommitted(key) {
			return nil, berrors.ErrBucketExists
		}
	}

	group := commitGroupOf(options)
	id := tx.db.allocBucketId()
	h := &bucketHandle{id: id, name: key, commitGroup: group}
	b := &Bucket{
		tx:          tx,
		name:        key,
		handle:      h,
		dirty:       make(map[common.Nid]*workNode),
		obsolete:    make(map[common.Nid]struct{}),
		FillPercent: DefaultFillPercent,
	}
	b.rootNode = &workNode{bucket: b, isLeaf: true}
	if tx.created == nil {
		tx.created = make(map[string]*bucketHandle)
	}
	tx.created[key] = h
	delete(tx.deleted, key)
	if group != nil {
		tx.markCommitGroupDirty(group)
	}
	tx.putBctx(key, b)
	return b, nil
}

// CreateBucketIfNotExistsWithOptions creates a bucket when absent and validates
// the existing bucket's commit-group membership when present.
func (tx *Tx) CreateBucketIfNotExistsWithOptions(name []byte, options *BucketOptions) (*Bucket, error) {
	if tx.db == nil {
		return nil, berrors.ErrTxClosed
	}
	if !tx.writable {
		return nil, berrors.ErrTxNotWritable
	}
	if len(name) == 0 {
		return nil, berrors.ErrBucketNameRequired
	}
	if b := tx.Bucket(name); b != nil {
		if err := ensureCommitGroupMatch(string(name), b.handle, options); err != nil {
			return nil, err
		}
		return b, nil
	}
	return tx.CreateBucketWithOptions(name, options)
}

func commitGroupOf(options *BucketOptions) *bucketCommitGroup {
	if options == nil || options.CommitGroup == nil {
		return nil
	}
	return options.CommitGroup.g
}

func ensureCommitGroupMatch(name string, h *bucketHandle, options *BucketOptions) error {
	if options == nil {
		return nil
	}
	if h.commitGroup != commitGroupOf(options) {
		return fmt.Errorf("vmbolt: bucket %q already exists with a different commit group", name)
	}
	return nil
}

// pinCommitGroup returns the group snapshot this tx has pinned for g, loading
// and caching it on first access so all its members are read from one
// consistent generation.
func (tx *Tx) pinCommitGroup(g *bucketCommitGroup) *commitGroupSnapshot {
	if tx.commitGroupSnaps != nil {
		if s, ok := tx.commitGroupSnaps[g]; ok {
			return s
		}
	}
	s := g.state.Load()
	if tx.commitGroupSnaps == nil {
		tx.commitGroupSnaps = map[*bucketCommitGroup]*commitGroupSnapshot{}
	}
	tx.commitGroupSnaps[g] = s
	return s
}

// markCommitGroupDirty records that g must have its snapshot rebuilt at commit.
func (tx *Tx) markCommitGroupDirty(g *bucketCommitGroup) {
	if tx.dirtyCommitGroups == nil {
		tx.dirtyCommitGroups = map[*bucketCommitGroup]struct{}{}
	}
	tx.dirtyCommitGroups[g] = struct{}{}
}
