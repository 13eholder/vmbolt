package vmbolt

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	berrors "github.com/13eholder/vmbolt/errors"
	"github.com/13eholder/vmbolt/internal/common"
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

	opened atomic.Bool
	stats  *Stats

	// state is the single publication point for the whole database view.
	state atomic.Pointer[dbState]

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex // Allows only one writer at a time.
	statlock sync.RWMutex

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return "vmbolt.DB"
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return "vmbolt.DB"
}

// Open creates and opens a database at the given path with a given file mode.
// If the file does not exist then it will be created automatically with a given file mode.
// Passing in nil options will cause Bolt to open the database with the default options.
func Open(path string, mode os.FileMode, options *Options) (db *DB, err error) {
	db = &DB{}

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

	// Initialize the database.
	if err = db.init(); err != nil {
		_ = db.close()
		return nil, err
	}
	// Mark the DB as open before any write path runs: maybeRestore() below
	// calls Restore -> Update -> Begin(true), which checks db.opened.
	db.opened.Store(true)

	// If the path holds a BMSP snapshot, rehydrate from it. This adds no
	// per-commit persistence: the snapshot is only written on demand (via
	// WriteTo/CopyFile). A non-BMSP/missing file is ignored (start empty).
	if path != "" {
		if rerr := db.maybeRestore(path); rerr != nil {
			_ = db.close()
			return nil, rerr
		}
	}

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

// init creates the empty database state. Pure memory mode starts with no
// buckets and no allocated node ids. Nid 0 remains reserved.
func (db *DB) init() error {
	db.state.Store(&dbState{
		buckets: map[string]*bucketState{},
		nextNid: 1,
	})
	return nil
}

// Close releases all database resources.
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened.Load() {
		return nil
	}

	db.opened.Store(false)
	db.state.Store(nil)

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
	if !db.opened.Load() {
		return nil, berrors.ErrDatabaseNotOpen
	}
	base := db.state.Load()
	if base == nil {
		return nil, berrors.ErrDatabaseNotOpen
	}

	t := &Tx{
		db:   db,
		base: base,
	}

	if db.stats != nil {
		db.statlock.Lock()
		db.stats.TxN++
		db.stats.OpenTxN++
		db.statlock.Unlock()
	}

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	if db.readOnly {
		return nil, berrors.ErrDatabaseReadOnly
	}

	db.rwlock.Lock()

	if !db.opened.Load() {
		db.rwlock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	base := db.state.Load()
	if base == nil {
		db.rwlock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	t := &Tx{
		db:       db,
		writable: true,
		base:     base,
		nextNid:  base.nextNid,
		freeNids: append([]common.Nid(nil), base.freeNid...),
	}
	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
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

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true
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

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true
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
		db.batch = &batch{db: db}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
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
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			c.err <- trySolo
			continue retry
		}

		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

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
	return &Info{0}
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// Logger is the logger used for vmbolt.
	Logger Logger

	// NoStatistics turns off statistics collection.
	NoStatistics bool
}

func (o *Options) String() string {
	if o == nil {
		return "{}"
	}
	return fmt.Sprintf("{ReadOnly: %t, Logger: %p, NoStatistics: %t}",
		o.ReadOnly, o.Logger, o.NoStatistics)
}

// DefaultOptions represent the options used if nil options are passed into Open().
var DefaultOptions = &Options{}

// Stats represents statistics about the database.
type Stats struct {
	// Put `TxStats` at the first field to ensure it's 64-bit aligned.
	TxStats TxStats // global, ongoing stats.

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
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

type Info struct {
	Data uintptr
}
