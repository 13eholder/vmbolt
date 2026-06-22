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
// Every transaction pins one globally consistent dbState at Begin time. A
// write tx mutates tx-local buckets and publishes one new dbState atomically
// at commit.
type Tx struct {
	writable bool
	managed  bool
	db       *DB

	id   uint64
	base *dbState

	// Per-bucket tx-local contexts, created lazily by Bucket/CreateBucket/etc.
	bctx map[string]*Bucket

	// Pending bucket-directory mutations.
	created map[string]struct{}
	deleted map[string]struct{}

	// Writer-private global nid allocator snapshot.
	nextNid  uint64
	freeNids []common.Nid

	stats          TxStats
	commitHandlers []func()
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

// Size returns the logical payload size of the whole database view in bytes.
func (tx *Tx) Size() int64 {
	var size int64
	seen := make(map[string]bool, len(tx.bctx))

	for name, st := range tx.base.buckets {
		seen[name] = true
		if b := tx.bctx[name]; b != nil {
			size += b.size()
			continue
		}
		for _, sn := range st.nodes {
			size += int64(sn.size())
		}
	}
	for name, b := range tx.bctx {
		if seen[name] {
			continue
		}
		size += b.size()
	}
	return size
}

// Inspect returns the structure of the database: a synthetic "root" whose
// children are the top-level buckets.
func (tx *Tx) Inspect() BucketStructure {
	root := BucketStructure{Name: "root"}
	_ = tx.ForEach(func(name []byte, b *Bucket) error {
		root.Children = append(root.Children, b.Inspect())
		return nil
	})
	return root
}

// Cursor returns a directory cursor over the top-level bucket names in sorted order.
func (tx *Tx) Cursor() *Cursor {
	if tx.db == nil {
		return &Cursor{}
	}
	nameSet := map[string]struct{}{}
	for n := range tx.base.buckets {
		nameSet[n] = struct{}{}
	}
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

// Bucket retrieves a top-level bucket by name, opening a tx-local context for it.
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
	st := tx.base.buckets[key]
	if st == nil {
		return nil
	}
	b := newBucket(tx, key, st)
	tx.putBctx(key, b)
	return b
}

// CreateBucket creates a new top-level bucket and returns a tx-local handle to it.
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
		if _, created := tx.created[key]; created || tx.bucketExistsCommitted(key) {
			return nil, berrors.ErrBucketExists
		}
	}

	b := newBucket(tx, key, nil)
	b.rootNode = &workNode{bucket: b, isLeaf: true}
	if tx.created == nil {
		tx.created = make(map[string]struct{})
	}
	tx.created[key] = struct{}{}
	delete(tx.deleted, key)
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

// DeleteBucket deletes a top-level bucket.
func (tx *Tx) DeleteBucket(name []byte) error {
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	if !tx.writable {
		return berrors.ErrTxNotWritable
	}
	key := string(name)
	if _, ok := tx.created[key]; ok {
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

// ForEach executes a function for each top-level bucket.
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	if tx.db == nil {
		return berrors.ErrTxClosed
	}
	names := make([]string, 0, len(tx.base.buckets)+len(tx.created))
	for n := range tx.base.buckets {
		if _, deleted := tx.deleted[n]; deleted {
			continue
		}
		names = append(names, n)
	}
	for n := range tx.created {
		names = append(names, n)
	}
	sort.Strings(names)
	seen := map[string]struct{}{}
	for _, n := range names {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
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

// bucketExistsCommitted reports whether a bucket name exists in the transaction base state.
func (tx *Tx) bucketExistsCommitted(key string) bool {
	_, ok := tx.base.buckets[key]
	return ok
}

func (tx *Tx) putBctx(key string, b *Bucket) {
	if tx.bctx == nil {
		tx.bctx = make(map[string]*Bucket)
	}
	tx.bctx[key] = b
}

func (tx *Tx) allocateNid() common.Nid {
	if n := len(tx.freeNids); n > 0 {
		id := tx.freeNids[n-1]
		tx.freeNids = tx.freeNids[:n-1]
		return id
	}
	id := common.Nid(tx.nextNid)
	tx.nextNid++
	return id
}

func (tx *Tx) freeNid(id common.Nid) {
	if id == 0 {
		return
	}
	tx.freeNids = append(tx.freeNids, id)
}

// Commit publishes one new globally consistent state.
func (tx *Tx) Commit() (err error) {
	lg := tx.db.Logger()
	if lg != discardLogger {
		lg.Debugf("Committing transaction %d", tx.id)
		defer func() {
			if err != nil {
				lg.Errorf("Committing transaction failed: %v", err)
			} else {
				lg.Debugf("Committing transaction %d successfully", tx.id)
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

	newBuckets := make(map[string]*bucketState, len(tx.base.buckets)+len(tx.created))
	for name, st := range tx.base.buckets {
		newBuckets[name] = st
	}
	for name := range tx.deleted {
		delete(newBuckets, name)
	}
	for name, b := range tx.bctx {
		if _, deleted := tx.deleted[name]; deleted {
			continue
		}
		newBuckets[name] = b.buildPublishedState()
	}

	tx.db.state.Store(&dbState{
		buckets: newBuckets,
		nextNid: tx.nextNid,
		freeNid: append([]common.Nid(nil), tx.freeNids...),
	})

	tx.close()

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

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.rwtx = nil

		for _, b := range tx.bctx {
			for _, n := range b.dirty {
				releaseWorkNode(n)
			}
		}

		if tx.db.stats != nil {
			tx.db.statlock.Lock()
			tx.db.stats.TxStats.add(&tx.stats)
			tx.db.statlock.Unlock()
		}

		tx.db.rwlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	tx.db = nil
	tx.base = nil
	tx.bctx = nil
	tx.created = nil
	tx.deleted = nil
	tx.freeNids = nil
}

// Copy writes the entire database to a writer as a BMSP snapshot (see snapshot.go).
func (tx *Tx) Copy(w errorWriter) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo writes the entire database to a writer as a BMSP snapshot.
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
