package vmbolt_test

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	bolt "13eholder/vmbolt"
	berrors "13eholder/vmbolt/errors"
	"13eholder/vmbolt/internal/btesting"
)

// TestTx_Check_ReadOnly tests consistency checking on a ReadOnly database.
// TestTx_Check runs the structural consistency check over a well-formed
// database populated across multiple buckets (each large enough to split into a
// multi-level B+tree) and asserts it reports no errors.
func TestTx_Check(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"alpha", "beta", "gamma"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 500; i++ {
				k := []byte(fmt.Sprintf("key-%04d", i))
				if err := b.Put(k, []byte("value")); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertNoCheckErrors(t, db)
}

// TestTx_Check_AfterDeletions verifies the check stays clean once deletions
// have forced rebalancing/merging into a new published-consistent state.
func TestTx_Check_AfterDeletions(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < 1000; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("k-%04d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertNoCheckErrors(t, db)
}

// TestTx_Check_Concurrent verifies that concurrent Check calls on the same
// database are safe and each reports no errors. The published snapshot is
// immutable, so checks neither race against each other nor reload state.
func TestTx_Check_Concurrent(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("c"))
		if err != nil {
			return err
		}
		for i := 0; i < 300; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k-%d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	errc := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errc <- checkErrors(db)
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// checkErrors opens a read-only transaction, drains tx.Check, and returns the
// first error encountered (or nil).
func checkErrors(db *btesting.DB) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for err := range tx.Check() {
		return err
	}
	return nil
}

func assertNoCheckErrors(t *testing.T, db *btesting.DB) {
	t.Helper()
	if err := checkErrors(db); err != nil {
		t.Fatalf("unexpected check error: %v", err)
	}
}

// Ensure that committing a closed transaction returns an error.
func TestTx_Commit_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.CreateBucket([]byte("foo")); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != berrors.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that rolling back a closed transaction returns an error.
func TestTx_Rollback_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != berrors.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that committing a read-only transaction returns an error.
func TestTx_Commit_ErrTxNotWritable(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != berrors.ErrTxNotWritable {
		t.Fatal(err)
	}
	// Close the view transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
}

// Ensure that a transaction can enumerate top-level buckets. In the per-bucket
// model the directory is a map, so enumeration order is unspecified; we check
// the set of names instead. Buckets created within this tx are visible via
// tx.ForEach before commit.
func TestTx_ForEachBuckets(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.CreateBucket([]byte("woojits")); err != nil {
			t.Fatal(err)
		}

		seen := map[string]bool{}
		if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			seen[string(name)] = true
			return nil
		}); err != nil {
			return err
		}
		if !seen["widgets"] || !seen["woojits"] || len(seen) != 2 {
			t.Fatalf("unexpected bucket set: %v", seen)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTx_CreateBucket_ErrTxNotWritable(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.View(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("foo"))
		if err != berrors.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that creating a bucket on a closed transaction returns an error.
func TestTx_CreateBucket_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.CreateBucket([]byte("foo")); err != berrors.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a Tx can retrieve a bucket.
func TestTx_Bucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTx_Get_NotFound(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if b.Get([]byte("no_such_key")) != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can be created and retrieved.
func TestTx_CreateBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Read the bucket through a separate transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestTx_CreateBucketIfNotExists(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create bucket.
		if b, err := tx.CreateBucketIfNotExists([]byte("widgets")); err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}

		// Create bucket again.
		if b, err := tx.CreateBucketIfNotExists([]byte("widgets")); err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected bucket")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Read the bucket through a separate transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("widgets")) == nil {
			t.Fatal("expected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure transaction returns an error if creating an unnamed bucket.
func TestTx_CreateBucketIfNotExists_ErrBucketNameRequired(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte{}); err != berrors.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}

		if _, err := tx.CreateBucketIfNotExists(nil); err != berrors.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket cannot be created twice.
func TestTx_CreateBucket_ErrBucketExists(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Create the same bucket again.
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != berrors.ErrBucketExists {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket is created with a non-blank name.
func TestTx_CreateBucket_ErrBucketNameRequired(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket(nil); err != berrors.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can be deleted.
func TestTx_DeleteBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket and add a value.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Delete the bucket and make sure we can't get the value.
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		if tx.Bucket([]byte("widgets")) != nil {
			t.Fatal("unexpected bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		// Create the bucket again and make sure there's not a phantom value.
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatalf("unexpected phantom value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a bucket on a closed transaction returns an error.
func TestTx_DeleteBucket_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteBucket([]byte("foo")); err != berrors.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTx_DeleteBucket_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.View(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("foo")); err != berrors.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that nothing happens when deleting a bucket that doesn't exist.
func TestTx_DeleteBucket_NotFound(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != berrors.ErrBucketNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that no error is returned when a tx.ForEach function does not return
// an error.
func TestTx_ForEach_NoError(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}

		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that an error is returned when a tx.ForEach function returns an error.
func TestTx_ForEach_WithError(t *testing.T) {
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}

		marker := errors.New("marker")
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return marker
		}); err != marker {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that Tx commit handlers are called after a transaction successfully commits.
func TestTx_OnCommit(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var x int
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	} else if x != 3 {
		t.Fatalf("unexpected x: %d", x)
	}
}

// Ensure that Tx commit handlers are NOT called after a transaction rolls back.
func TestTx_OnCommit_Rollback(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var x int
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return errors.New("rollback this commit")
	}); err == nil || err.Error() != "rollback this commit" {
		t.Fatalf("unexpected error: %s", err)
	} else if x != 0 {
		t.Fatalf("unexpected x: %d", x)
	}
}

// Ensure that the database can be copied to a file path.
func TestTx_CopyFile(t *testing.T) {
	t.Skip("not applicable to pure memory mode")
	db := btesting.MustCreateDB(t)

	path := tempfile()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(path, 0600)
	}); err != nil {
		t.Fatal(err)
	}

	db2, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := db2.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte("widgets")).Get([]byte("foo")); !bytes.Equal(v, []byte("bar")) {
			t.Fatalf("unexpected value: %v", v)
		}
		if v := tx.Bucket([]byte("widgets")).Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

type failWriterError struct{}

func (failWriterError) Error() string {
	return "error injected for tests"
}

type failWriter struct {
	// fail after this many bytes
	After int
}

func (f *failWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	if n > f.After {
		n = f.After
		err = failWriterError{}
	}
	f.After -= n
	return n, err
}

// Ensure that Copy handles write errors right.
func TestTx_CopyFile_Error_Meta(t *testing.T) {
	t.Skip("not applicable to pure memory mode")
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Copy(&failWriter{})
	}); err == nil || err.Error() != "meta 0 copy: error injected for tests" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Ensure that Copy handles write errors right.
func TestTx_CopyFile_Error_Normal(t *testing.T) {
	t.Skip("not applicable to pure memory mode")
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Copy(&failWriter{12 * 1024})
	}); err == nil || err.Error() != "error injected for tests" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestTx_Rollback ensures there is no error on tx rollback.
func TestTx_Rollback(t *testing.T) {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Error starting tx: %v", err)
	}
	bucket := []byte("mybucket")
	if _, err := tx.CreateBucket(bucket); err != nil {
		t.Fatalf("Error creating bucket: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Error on commit: %v", err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatalf("Error starting tx: %v", err)
	}
	b := tx.Bucket(bucket)
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Error on put: %v", err)
	}
	// Imagine there is an error and tx needs to be rolled-back
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Error on rollback: %v", err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatalf("Error starting tx: %v", err)
	}
	b = tx.Bucket(bucket)
	if v := b.Get([]byte("k")); v != nil {
		t.Fatalf("Value for k should not have been stored")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Error on rollback: %v", err)
	}
}

func TestTxStats_GetAndIncAtomically(t *testing.T) {
	var stats bolt.TxStats

	stats.IncCursorCount(3)
	assert.Equal(t, int64(3), stats.GetCursorCount())

	stats.IncNodeCount(100)
	assert.Equal(t, int64(100), stats.GetNodeCount())

	stats.IncNodeDeref(101)
	assert.Equal(t, int64(101), stats.GetNodeDeref())

	stats.IncRebalance(1000)
	assert.Equal(t, int64(1000), stats.GetRebalance())

	stats.IncRebalanceTime(1001 * time.Second)
	assert.Equal(t, 1001*time.Second, stats.GetRebalanceTime())

	stats.IncSplit(10000)
	assert.Equal(t, int64(10000), stats.GetSplit())

	stats.IncSpill(10001)
	assert.Equal(t, int64(10001), stats.GetSpill())

	stats.IncSpillTime(10001 * time.Second)
	assert.Equal(t, 10001*time.Second, stats.GetSpillTime())

	stats.IncWrite(100000)
	assert.Equal(t, int64(100000), stats.GetWrite())

	stats.IncWriteTime(100001 * time.Second)
	assert.Equal(t, 100001*time.Second, stats.GetWriteTime())

	assert.Equal(t,
		bolt.TxStats{
			CursorCount:   3,
			NodeCount:     100,
			NodeDeref:     101,
			Rebalance:     1000,
			RebalanceTime: 1001 * time.Second,
			Split:         10000,
			Spill:         10001,
			SpillTime:     10001 * time.Second,
			Write:         100000,
			WriteTime:     100001 * time.Second,
		},
		stats,
	)
}

func TestTxStats_Sub(t *testing.T) {
	statsA := bolt.TxStats{
		CursorCount:   3,
		NodeCount:     100,
		NodeDeref:     101,
		Rebalance:     1000,
		RebalanceTime: 1001 * time.Second,
		Split:         10000,
		Spill:         10001,
		SpillTime:     10001 * time.Second,
		Write:         100000,
		WriteTime:     100001 * time.Second,
	}

	statsB := bolt.TxStats{
		CursorCount:   4,
		NodeCount:     101,
		NodeDeref:     102,
		Rebalance:     1001,
		RebalanceTime: 1002 * time.Second,
		Split:         11001,
		Spill:         11002,
		SpillTime:     11002 * time.Second,
		Write:         110001,
		WriteTime:     110010 * time.Second,
	}

	diff := statsB.Sub(&statsA)
	assert.Equal(t, int64(1), diff.GetCursorCount())
	assert.Equal(t, int64(1), diff.GetNodeCount())
	assert.Equal(t, int64(1), diff.GetNodeDeref())
	assert.Equal(t, int64(1), diff.GetRebalance())
	assert.Equal(t, time.Second, diff.GetRebalanceTime())
	assert.Equal(t, int64(1001), diff.GetSplit())
	assert.Equal(t, int64(1001), diff.GetSpill())
	assert.Equal(t, 1001*time.Second, diff.GetSpillTime())
	assert.Equal(t, int64(10001), diff.GetWrite())
	assert.Equal(t, 10009*time.Second, diff.GetWriteTime())
}
