package vmbolt

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	berrors "13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/common"
)

// DB represents a collection of top-level buckets held in memory.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchDelay time.Duration

	logger Logger

	path     string
	pageSize int
	opened   bool
	rwtx     *Tx
	stats    *Stats

	// Per-bucket directory: name -> bucketHandle. Reads look it up under
	// bucketsMu.RLock; create/delete mutate it under the write side (which the
	// single in-flight writer already holds via rwlock). Each handle owns an
	// atomic.Pointer[bucketState] for lock-free, per-bucket publish.
	bucketsMu     sync.RWMutex
	buckets       map[string]*bucketHandle
	txid          atomic.Uint64 // global monotonic transaction counter (Tx.ID)
	nextBucketId  uint16        // writer-private 16-bit BucketId allocator (starts at 1)
	freeBucketIds []uint16      // LIFO of recycled BucketIds

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex // Allows only one writer at a time.
	statlock sync.RWMutex

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open creates and opens a database at the given path with a given file mode.
// If the file does not exist then it will be created automatically with a given file mode.
// Passing in nil options will cause Bolt to open the database with the default options.
func Open(path string, mode os.FileMode, options *Options) (db *DB, err error) {
	db = &DB{
		opened: true,
	}

	// Set default options if no options are provided.
	if options == nil {
		options = DefaultOptions
	}
	db.readOnly = options.ReadOnly

	// Set default values for later DB operations.
	db.MaxBatchSize = common.DefaultMaxBatchSize
	db.MaxBatchDelay = common.DefaultMaxBatchDelay

	if !options.NoStatistics {
		db.stats = new(Stats)
	}

	if options.Logger == nil {
		db.logger = getDiscardLogger()
	} else {
		db.logger = options.Logger
	}

	lg := db.Logger()
	if lg != discardLogger {
		lg.Infof("Opening db file (%s) with options: %s", path, options)
		defer func() {
			if err != nil {
				lg.Errorf("Opening vmbolt db (%s) failed: %v", path, err)
			} else {
				lg.Infof("Opening vmbolt db (%s) successfully", path)
			}
		}()
	}

	if db.pageSize = options.PageSize; db.pageSize == 0 {
		db.pageSize = common.DefaultPageSize
	}

	db.path = path

	// Initialize the database.
	if err = db.init(); err != nil {
		_ = db.close()
		return nil, err
	}

	// If the path holds a BMSP snapshot, rehydrate from it. This adds no
	// per-commit persistence: the snapshot is only written on demand (via
	// WriteTo/CopyFile). A non-BMSP/missing file is ignored (start empty).
	if path != "" {
		if rerr := db.maybeRestore(path); rerr != nil {
			_ = db.close()
			return nil, rerr
		}
	}

	// Mark the database as opened and return.
	return db, nil
}

// maybeRestore opens path and, if it begins with the BMSP magic, restores the
// database from it. Missing files and non-BMSP files are silently ignored.
func (db *DB) maybeRestore(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return nil // missing file: start empty
	}
	defer f.Close()
	var magic [4]byte
	if _, err := f.Read(magic[:]); err != nil {
		return nil // unreadable / empty: start empty
	}
	if string(magic[:]) != snapshotMagic {
		return nil // not our snapshot format: start empty
	}
	if _, err := f.Seek(0, 0); err != nil {
		return fmt.Errorf("vmbolt: seek snapshot %q: %w", path, err)
	}
	if err := db.Restore(f); err != nil {
		return fmt.Errorf("vmbolt: restore snapshot %q: %w", path, err)
	}
	return nil
}

// init creates the empty per-bucket directory. Pure memory mode starts with
// no buckets and no reserved node ids; buckets (and their BucketIds) are
// allocated on demand via CreateBucket.
func (db *DB) init() error {
	db.buckets = make(map[string]*bucketHandle)
	// BucketId 0 is reserved: Nid 0 is the "unassigned" sentinel, so a bucket
	// whose id was 0 would alias it. Start allocation at 1.
	db.nextBucketId = 1
	return nil
}

// Close releases all database resources.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false
	db.buckets = nil
	db.path = ""

	return nil
}

// Begin starts a new transaction.
func (db *DB) Begin(writable bool) (t *Tx, err error) {
	if lg := db.Logger(); lg != discardLogger {
		lg.Debugf("Starting a new transaction [writable: %t]", writable)
		defer func() {
			if err != nil {
				lg.Errorf("Starting a new transaction [writable: %t] failed: %v", writable, err)
			} else {
				lg.Debugf("Starting a new transaction [writable: %t] successfully", writable)
			}
		}()
	}

	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) Logger() Logger {
	if db == nil || db.logger == nil {
		return getDiscardLogger()
	}
	return db.logger
}

func (db *DB) beginTx() (*Tx, error) {
	// Exit if the database is not open yet.
	if !db.opened {
		return nil, berrors.ErrDatabaseNotOpen
	}

	// Read transactions lazily pin each touched bucket's current generation on
	// first access (per-bucket snapshot isolation). There is no global snapshot.
	t := &Tx{db: db, id: common.Txid(db.txid.Load())}

	// Update the transaction stats.
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.TxN++
		db.stats.OpenTxN++
		db.statlock.Unlock()
	}

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, berrors.ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// A single global writer serializes all updates; buckets are published
	// independently at commit time.
	db.rwlock.Lock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{db: db, writable: true, id: common.Txid(db.txid.Add(1))}
	db.rwtx = t
	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Merge statistics.
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.OpenTxN--
		db.stats.TxStats.add(&tx.stats)
		db.statlock.Unlock()
	}
}

// Update executes a function within the context of a read-write managed transaction.
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Rollback()
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except concurrent Batch calls can be combined into a single Bolt
// transaction.
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Stats retrieves ongoing performance stats for the database.
func (db *DB) Stats() Stats {
	var s Stats
	if db.stats != nil {
		db.statlock.RLock()
		s = *db.stats
		db.statlock.RUnlock()
	}
	return s
}

// Info returns information about the database.
func (db *DB) Info() *Info {
	return &Info{0, db.pageSize}
}

// allocBucketId returns a fresh 16-bit BucketId for a new bucket, reusing a
// recycled id (LIFO) when available. Writer-private: only called under rwlock.
func (db *DB) allocBucketId() common.BucketId {
	if n := len(db.freeBucketIds); n > 0 {
		id := db.freeBucketIds[n-1]
		db.freeBucketIds = db.freeBucketIds[:n-1]
		return common.BucketId(id)
	}
	id := db.nextBucketId
	if id == 0 {
		// nextBucketId wrapped past 65535 back to 0. BucketId 0 is reserved, and
		// if the freelist is also empty the BucketId space (65535) is exhausted.
		panic("vmbolt: BucketId space exhausted (max 65535 buckets)")
	}
	db.nextBucketId++
	return common.BucketId(id)
}

// freeBucketId returns a BucketId to the pool for reuse. Writer-private.
func (db *DB) freeBucketId(id common.BucketId) {
	db.freeBucketIds = append(db.freeBucketIds, uint16(id))
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// PageSize overrides the default page size used for size/statistics estimates.
	PageSize int

	// Logger is the logger used for vmbolt.
	Logger Logger

	// NoStatistics turns off statistics collection.
	NoStatistics bool
}

func (o *Options) String() string {
	if o == nil {
		return "{}"
	}
	return fmt.Sprintf("{ReadOnly: %t, PageSize: %d, Logger: %p, NoStatistics: %t}",
		o.ReadOnly, o.PageSize, o.Logger, o.NoStatistics)
}

// DefaultOptions represent the options used if nil options are passed into Open().
var DefaultOptions = &Options{}

// Stats represents statistics about the database.
type Stats struct {
	// Put `TxStats` at the first field to ensure it's 64-bit aligned.
	TxStats TxStats // global, ongoing stats.

	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions
}

// Sub calculates and returns the difference between two sets of database stats.
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

type Info struct {
	Data     uintptr
	PageSize int
}
