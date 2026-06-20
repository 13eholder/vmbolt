package vmbolt

import (
	"os"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	berrors "13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/common"
)

// Tx represents a read-only or read/write transaction on the database.
//
// In the per-bucket model a Tx does not hold a global snapshot. Instead it
// lazily opens a per-bucket context (*Bucket) on first access. A read Tx pins
// each touched bucket's current generation (per-bucket snapshot isolation); a
// write Tx accumulates dirty workNodes per touched bucket and publishes each
// touched bucket independently at commit. There is deliberately no
// cross-bucket atomicity.
type Tx struct {
	writable bool
	managed  bool
	db       *DB
	id       common.Txid

	// Per-bucket tx-local contexts, created lazily by Bucket/CreateBucket/etc.
	bctx map[string]*Bucket

	// Pending bucket-directory mutations, applied at commit (build-all-then-publish-all).
	created map[string]*bucketHandle
	deleted map[string]struct{}

	// Cohort bookkeeping. cohortSnaps pins (per read tx) one cohortSnapshot per
	// cohort so all its members are read from one generation. dirtyCohorts /
	// adoptedStates drive write-tx cohort (re)publishing.
	cohortSnaps   map[*bucketCohort]*cohortSnapshot
	dirtyCohorts  map[*bucketCohort]struct{}
	adoptedStates map[string]*bucketState // existing independent buckets adopted into a cohort this tx

	stats          TxStats
	commitHandlers []func()
}

// ID returns the transaction id.
func (tx *Tx) ID() int {
	if tx == nil {
		return -1
	}
	return int(tx.id)
}

// DB returns a reference to the database that created the transaction.
func (tx *Tx) DB() *DB {
	return tx.db
}

// Writable returns whether the transaction can perform write operations.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Stats retrieves a copy of the current transaction statistics.
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Size returns the logical payload size of the whole database view in bytes
// (the sum of every bucket's node-content estimate, using the tx-local view for
// buckets touched by this tx and the committed generation for the rest). It is
// an in-memory estimate, not a persisted file size.
func (tx *Tx) Size() int64 {
	var size int64
	seen := make(map[string]bool, len(tx.bctx))

	// Committed buckets.
	tx.db.bucketsMu.RLock()
	type committed struct {
		name string
		h    *bucketHandle
	}
	committedBuckets := make([]committed, 0, len(tx.db.buckets))
	for n, h := range tx.db.buckets {
		committedBuckets = append(committedBuckets, committed{n, h})
	}
	tx.db.bucketsMu.RUnlock()
	for _, c := range committedBuckets {
		seen[c.name] = true
		if b := tx.bctx[c.name]; b != nil {
			size += b.size() // tx-local view (dirty/base/obsolete)
			continue
		}
		if st := publishedStateOf(c.h); st != nil {
			for _, sn := range st.nodes {
				size += int64(sn.size())
			}
		}
	}
	// Buckets created in this tx (not yet in db.buckets).
	for name, b := range tx.bctx {
		if seen[name] {
			continue
		}
		size += b.size()
	}
	return size
}

// Inspect returns the structure of the database: a synthetic "root" whose
// children are the top-level buckets. (There is no real root bucket in the
// per-bucket model; this aggregates for diagnostic compatibility.)
func (tx *Tx) Inspect() BucketStructure {
	root := BucketStructure{Name: "root"}
	_ = tx.ForEach(func(name []byte, b *Bucket) error {
		root.Children = append(root.Children, b.Inspect())
		return nil
	})
	return root
}

// Cursor returns a directory cursor over the top-level bucket NAMES in sorted
// (lexicographic) order. It yields (bucketName, nil) per entry; use tx.Bucket(name)
// to open the bucket itself. This replaces the legacy root-bucket cursor and keeps
// enumeration deterministic (required by Hash/snapshot/defrag-style callers).
func (tx *Tx) Cursor() *Cursor {
	if tx.db == nil {
		return &Cursor{}
	}
	// Snapshot the bucket-name set (committed ∪ created − deleted) and sort it.
	nameSet := map[string]struct{}{}
	tx.db.bucketsMu.RLock()
	for n := range tx.db.buckets {
		nameSet[n] = struct{}{}
	}
	tx.db.bucketsMu.RUnlock()
	for n := range tx.created {
		nameSet[n] = struct{}{}
	}
	for n := range tx.deleted {
		delete(nameSet, n)
	}
	names := make([]string, 0, len(nameSet))
	for n := range nameSet {
		names = append(names, n)
	}
	sort.Strings(names)
	dir := make([][]byte, len(names))
	for i, n := range names {
		dir[i] = []byte(n)
	}
	return &Cursor{dir: dir, dirIdx: -1}
}

// Bucket retrieves a top-level bucket by name, opening a tx-local context for
// it. For a read Tx the bucket's current generation is pinned (per-bucket
// snapshot isolation); for a write Tx a dirty set is initialized and the
// bucket's allocator state is snapshotted for rollback. Returns nil if the
// bucket does not exist.
func (tx *Tx) Bucket(name []byte) *Bucket {
	if tx.db == nil {
		return nil
	}
	key := string(name)
	if b := tx.bctx[key]; b != nil {
		return b
	}
	if _, ok := tx.deleted[key]; ok {
		return nil
	}
	if h := tx.created[key]; h != nil {
		b := newBucketForHandle(tx, key, h)
		tx.putBctx(key, b)
		return b
	}
	tx.db.bucketsMu.RLock()
	h := tx.db.buckets[key]
	tx.db.bucketsMu.RUnlock()
	if h == nil {
		return nil
	}
	b := newBucketForHandle(tx, key, h)
	tx.putBctx(key, b)
	return b
}

// CreateBucket creates a new top-level bucket and returns a tx-local handle to
// it. The bucket is usable immediately within this tx; it is registered in the
// DB directory at commit.
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
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
	id := tx.db.allocBucketId()
	h := &bucketHandle{id: id, name: key}
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
	delete(tx.deleted, key) // a delete-then-recreate cancels the deletion
	tx.putBctx(key, b)
	return b, nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
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
		return b, nil
	}
	return tx.CreateBucket(name)
}

// DeleteBucket deletes a top-level bucket. The whole tree is discarded at
// commit; no per-node freeing is needed.
func (tx *Tx) DeleteBucket(name []byte) error {
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	if !tx.writable {
		return berrors.ErrTxNotWritable
	}
	key := string(name)
	if tx.created[key] != nil {
		// Created and deleted in the same tx: cancel the creation.
		delete(tx.created, key)
		delete(tx.bctx, key)
		return nil
	}
	if !tx.bucketExistsCommitted(key) {
		return berrors.ErrBucketNotFound
	}
	if tx.deleted == nil {
		tx.deleted = make(map[string]struct{})
	}
	tx.deleted[key] = struct{}{}
	delete(tx.bctx, key)
	return nil
}

// MoveBucket is not supported in the flat per-bucket model: buckets are all
// top-level and indexed directly by name.
func (tx *Tx) MoveBucket(child []byte, src *Bucket, dst *Bucket) error {
	_ = child
	_ = src
	_ = dst
	return berrors.ErrNestedBucketsUnsupported
}

// ForEach executes a function for each top-level bucket.
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	tx.db.bucketsMu.RLock()
	names := make([]string, 0, len(tx.db.buckets))
	for n := range tx.db.buckets {
		names = append(names, n)
	}
	tx.db.bucketsMu.RUnlock()
	for _, n := range names {
		if _, ok := tx.deleted[n]; ok {
			continue
		}
		if err := fn([]byte(n), tx.Bucket([]byte(n))); err != nil {
			return err
		}
	}
	for n := range tx.created {
		if err := fn([]byte(n), tx.Bucket([]byte(n))); err != nil {
			return err
		}
	}
	return nil
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// bucketExistsCommitted reports whether a bucket name exists in the DB
// directory (ignoring this tx's pending create/delete).
func (tx *Tx) bucketExistsCommitted(key string) bool {
	tx.db.bucketsMu.RLock()
	_, ok := tx.db.buckets[key]
	tx.db.bucketsMu.RUnlock()
	return ok
}

func (tx *Tx) putBctx(key string, b *Bucket) {
	if tx.bctx == nil {
		tx.bctx = make(map[string]*Bucket)
	}
	tx.bctx[key] = b
}

// Commit publishes each touched bucket. Independent buckets are published one
// atomic Store each (per-bucket snapshot isolation). Buckets that share a cohort
// (see Tx.AssignCohort) are published together via one cohort-snapshot Store, so
// cohort members become visible atomically — no reader can see one member at a
// newer generation than another.
//
// Pipeline:
//  1. rebalance + spill (finalize) every touched bucket, assigning node ids;
//  2. build each bucket's new immutable bucketState;
//  3. publish under bucketsMu: independent buckets per-handle, dirty cohorts as
//     rebuilt joint snapshots, plus pending create/delete.
//
// Steps 1-2 perform no publication, so a failure leaves nothing published and
// the tx rolls back cleanly.
func (tx *Tx) Commit() (err error) {
	txId := tx.ID()
	lg := tx.db.Logger()
	if lg != discardLogger {
		lg.Debugf("Committing transaction %d", txId)
		defer func() {
			if err != nil {
				lg.Errorf("Committing transaction failed: %v", err)
			} else {
				lg.Debugf("Committing transaction %d successfully", txId)
			}
		}()
	}

	common.Assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	if !tx.writable {
		return berrors.ErrTxNotWritable
	}

	// Step 1: rebalance then spill each touched bucket.
	startTime := time.Now()
	for _, b := range tx.bctx {
		b.rebalance()
	}
	if tx.stats.GetRebalance() > 0 {
		tx.stats.IncRebalanceTime(time.Since(startTime))
	}

	startTime = time.Now()
	for _, b := range tx.bctx {
		if err = b.spill(); err != nil {
			lg.Errorf("spilling bucket %q failed: %v", b.name, err)
			tx.rollback()
			return err
		}
	}
	tx.stats.IncSpillTime(time.Since(startTime))

	// Step 2: build the new published state for each touched bucket.
	type pending struct {
		handle *bucketHandle
		state  *bucketState
		isNew  bool
	}
	pendings := make([]pending, 0, len(tx.bctx))
	for _, b := range tx.bctx {
		_, isNew := tx.created[b.name]
		pendings = append(pendings, pending{
			handle: b.handle,
			state:  b.buildPublishedState(),
			isNew:  isNew,
		})
	}

	// Step 3: publish under the directory lock.
	//   - touched INDEPENDENT buckets: per-handle atomic Store;
	//   - each dirty cohort: rebuild its jointly-published snapshot (members at
	//     their new state if touched/adopted this tx, else carried forward) and
	//     Store it once, so all members become visible atomically together.
	newStates := make(map[string]*bucketState, len(pendings)+len(tx.adoptedStates))
	for _, p := range pendings {
		newStates[p.handle.name] = p.state
	}
	for name, st := range tx.adoptedStates {
		if _, ok := newStates[name]; !ok {
			newStates[name] = st
		}
	}
	// Any touched/adopted cohort member must trigger its cohort's joint republish.
	for _, p := range pendings {
		if p.handle.cohort != nil {
			tx.markCohortDirty(p.handle.cohort)
		}
	}

	tx.db.bucketsMu.Lock()
	// 3a. register created buckets.
	for _, p := range pendings {
		if p.isNew {
			tx.db.buckets[p.handle.name] = p.handle
		}
	}
	// 3b. apply deletes; mark any cohort dirty so its snapshot drops the member.
	for name := range tx.deleted {
		if h := tx.db.buckets[name]; h != nil {
			if h.cohort != nil {
				tx.markCohortDirty(h.cohort)
			}
			delete(tx.db.buckets, name)
			tx.db.freeBucketId(h.id)
		}
	}
	// 3c. publish independent touched buckets.
	for _, p := range pendings {
		if p.handle.cohort == nil {
			p.handle.state.Store(p.state)
		}
	}
	// 3d. publish dirty cohorts: rebuild each from its current members.
	for c := range tx.dirtyCohorts {
		members := make(map[string]*bucketState)
		for name, h := range tx.db.buckets {
			if h.cohort != c {
				continue
			}
			if st := newStates[name]; st != nil {
				members[name] = st
			} else {
				members[name] = publishedStateOf(h) // carry untouched member forward
			}
		}
		c.state.Store(&cohortSnapshot{members: members})
	}
	tx.db.bucketsMu.Unlock()

	// Finalize the transaction.
	tx.close()

	// Execute commit handlers now that the locks have been removed.
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates.
func (tx *Tx) Rollback() error {
	common.Assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	tx.rollback()
	return nil
}

// rollback rolls back the transaction (internal, panic/commit-failure/user
// Rollback path). It restores writer-private allocator state so ids allocated
// during this tx are discarded, reclaims BucketIds for created-then-rolled-back
// buckets, and then closes.
func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	// Only a writable tx mutates (and therefore needs to restore) the
	// writer-private allocator state. Read txs never snapshot it, so restoring
	// on their (mandatory) rollback would clobber the live counters with zeros.
	if tx.writable {
		for _, b := range tx.bctx {
			if b.handle == nil {
				continue
			}
			b.handle.nextNodeId = b.snapNextNode
			b.handle.freeNodeIds = b.snapFreeIds
		}
		// Reclaim BucketIds for buckets created then rolled back (never published).
		for _, h := range tx.created {
			tx.db.freeBucketId(h.id)
		}
		// Undo cohort adoptions: an adopted bucket's cohort was set but never
		// published (no commit), so detach it to avoid a set-but-orphaned cohort.
		for name := range tx.adoptedStates {
			if h := tx.db.buckets[name]; h != nil {
				h.cohort = nil
			}
		}
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.rwtx = nil

		// Return transaction-local workNode shells to the pool. On commit their
		// inodes were already transferred to snapNodes by freeze(), so only the
		// shells are recycled.
		for _, b := range tx.bctx {
			for _, n := range b.dirty {
				releaseWorkNode(n)
			}
		}

		// Merge statistics.
		if tx.db.stats != nil {
			tx.db.statlock.Lock()
			tx.db.stats.FreePageN = 0
			tx.db.stats.PendingPageN = 0
			tx.db.stats.FreeAlloc = 0
			tx.db.stats.FreelistInuse = 0
			tx.db.stats.TxStats.add(&tx.stats)
			tx.db.statlock.Unlock()
		}

		tx.db.rwlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	// Clear all references.
	tx.db = nil
	tx.bctx = nil
	tx.created = nil
	tx.deleted = nil
	tx.cohortSnaps = nil
	tx.dirtyCohorts = nil
	tx.adoptedStates = nil
}

// Copy writes the entire database to a writer as a BMSP snapshot (see snapshot.go).
func (tx *Tx) Copy(w errorWriter) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo writes the entire database to a writer as a BMSP snapshot. It traverses
// top-level buckets in sorted name order (via Tx.Cursor) and, within each, keys
// in sorted order, so the output is deterministic and round-trips with Restore.
func (tx *Tx) WriteTo(w errorWriter) (n int64, err error) {
	return tx.serializeSnapshot(w)
}

// CopyFile writes the entire database to a file at path as a BMSP snapshot.
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = tx.WriteTo(f)
	return err
}

type errorWriter interface {
	Write(p []byte) (n int, err error)
}

// TxStats represents statistics about the actions performed by the transaction.
type TxStats struct {
	PageCount     int64
	PageAlloc     int64
	CursorCount   int64
	NodeCount     int64
	NodeDeref     int64
	Rebalance     int64
	RebalanceTime time.Duration
	Split         int64
	Spill         int64
	SpillTime     time.Duration
	Write         int64
	WriteTime     time.Duration
}

func (s *TxStats) add(other *TxStats) {
	s.IncPageCount(other.GetPageCount())
	s.IncPageAlloc(other.GetPageAlloc())
	s.IncCursorCount(other.GetCursorCount())
	s.IncNodeCount(other.GetNodeCount())
	s.IncNodeDeref(other.GetNodeDeref())
	s.IncRebalance(other.GetRebalance())
	s.IncRebalanceTime(other.GetRebalanceTime())
	s.IncSplit(other.GetSplit())
	s.IncSpill(other.GetSpill())
	s.IncSpillTime(other.GetSpillTime())
	s.IncWrite(other.GetWrite())
	s.IncWriteTime(other.GetWriteTime())
}

// Sub calculates and returns the difference between two sets of transaction stats.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.GetPageCount() - other.GetPageCount()
	diff.PageAlloc = s.GetPageAlloc() - other.GetPageAlloc()
	diff.CursorCount = s.GetCursorCount() - other.GetCursorCount()
	diff.NodeCount = s.GetNodeCount() - other.GetNodeCount()
	diff.NodeDeref = s.GetNodeDeref() - other.GetNodeDeref()
	diff.Rebalance = s.GetRebalance() - other.GetRebalance()
	diff.RebalanceTime = s.GetRebalanceTime() - other.GetRebalanceTime()
	diff.Split = s.GetSplit() - other.GetSplit()
	diff.Spill = s.GetSpill() - other.GetSpill()
	diff.SpillTime = s.GetSpillTime() - other.GetSpillTime()
	diff.Write = s.GetWrite() - other.GetWrite()
	diff.WriteTime = s.GetWriteTime() - other.GetWriteTime()
	return diff
}

func (s *TxStats) GetPageCount() int64 {
	return atomic.LoadInt64(&s.PageCount)
}

func (s *TxStats) IncPageCount(delta int64) int64 {
	return atomic.AddInt64(&s.PageCount, delta)
}

func (s *TxStats) GetPageAlloc() int64 {
	return atomic.LoadInt64(&s.PageAlloc)
}

func (s *TxStats) IncPageAlloc(delta int64) int64 {
	return atomic.AddInt64(&s.PageAlloc, delta)
}

func (s *TxStats) GetCursorCount() int64 {
	return atomic.LoadInt64(&s.CursorCount)
}

func (s *TxStats) IncCursorCount(delta int64) int64 {
	return atomic.AddInt64(&s.CursorCount, delta)
}

func (s *TxStats) GetNodeCount() int64 {
	return atomic.LoadInt64(&s.NodeCount)
}

func (s *TxStats) IncNodeCount(delta int64) int64 {
	return atomic.AddInt64(&s.NodeCount, delta)
}

func (s *TxStats) GetNodeDeref() int64 {
	return atomic.LoadInt64(&s.NodeDeref)
}

func (s *TxStats) IncNodeDeref(delta int64) int64 {
	return atomic.AddInt64(&s.NodeDeref, delta)
}

func (s *TxStats) GetRebalance() int64 {
	return atomic.LoadInt64(&s.Rebalance)
}

func (s *TxStats) IncRebalance(delta int64) int64 {
	return atomic.AddInt64(&s.Rebalance, delta)
}

func (s *TxStats) GetRebalanceTime() time.Duration {
	return atomicLoadDuration(&s.RebalanceTime)
}

func (s *TxStats) IncRebalanceTime(delta time.Duration) time.Duration {
	return atomicAddDuration(&s.RebalanceTime, delta)
}

func (s *TxStats) GetSplit() int64 {
	return atomic.LoadInt64(&s.Split)
}

func (s *TxStats) IncSplit(delta int64) int64 {
	return atomic.AddInt64(&s.Split, delta)
}

func (s *TxStats) GetSpill() int64 {
	return atomic.LoadInt64(&s.Spill)
}

func (s *TxStats) IncSpill(delta int64) int64 {
	return atomic.AddInt64(&s.Spill, delta)
}

func (s *TxStats) GetSpillTime() time.Duration {
	return atomicLoadDuration(&s.SpillTime)
}

func (s *TxStats) IncSpillTime(delta time.Duration) time.Duration {
	return atomicAddDuration(&s.SpillTime, delta)
}

func (s *TxStats) GetWrite() int64 {
	return atomic.LoadInt64(&s.Write)
}

func (s *TxStats) IncWrite(delta int64) int64 {
	return atomic.AddInt64(&s.Write, delta)
}

func (s *TxStats) GetWriteTime() time.Duration {
	return atomicLoadDuration(&s.WriteTime)
}

func (s *TxStats) IncWriteTime(delta time.Duration) time.Duration {
	return atomicAddDuration(&s.WriteTime, delta)
}

func atomicAddDuration(ptr *time.Duration, du time.Duration) time.Duration {
	return time.Duration(atomic.AddInt64((*int64)(unsafe.Pointer(ptr)), int64(du)))
}

func atomicLoadDuration(ptr *time.Duration) time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(unsafe.Pointer(ptr))))
}
