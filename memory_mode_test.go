package vmbolt

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

)

func TestMemoryMode_BasicCRUD(t *testing.T) {

	db, err := Open("memory-test.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Create bucket and put keys.
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("hello"), []byte("world"))
	})
	require.NoError(t, err)

	// Read back.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		v := b.Get([]byte("hello"))
		require.Equal(t, "world", string(v))
		return nil
	})
	require.NoError(t, err)

	// Update.
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		return b.Put([]byte("hello"), []byte("memory"))
	})
	require.NoError(t, err)

	// Read updated.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		v := b.Get([]byte("hello"))
		require.Equal(t, "memory", string(v))
		return nil
	})
	require.NoError(t, err)

	// Delete.
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		return b.Delete([]byte("hello"))
	})
	require.NoError(t, err)

	// Verify deletion.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		v := b.Get([]byte("hello"))
		require.Nil(t, v)
		return nil
	})
	require.NoError(t, err)
}

func TestMemoryMode_Cursor(t *testing.T) {

	db, err := Open("memory-cursor.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Insert ordered keys.
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("cursor"))
		if err != nil {
			return err
		}
		for i := byte(0); i < 10; i++ {
			if err := b.Put([]byte{i}, []byte{i + 10}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Iterate forward.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("cursor"))
		c := b.Cursor()
		var count int
		for k, v := c.First(); k != nil; k, v = c.Next() {
			require.Equal(t, byte(count), k[0])
			require.Equal(t, byte(count+10), v[0])
			count++
		}
		require.Equal(t, 10, count)
		return nil
	})
	require.NoError(t, err)
}

func TestMemoryMode_TxRollback(t *testing.T) {

	db, err := Open("memory-rollback.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Put initial value.
	err = db.Update(func(tx *Tx) error {
		b, _ := tx.CreateBucket([]byte("tx"))
		return b.Put([]byte("k"), []byte("v1"))
	})
	require.NoError(t, err)

	// Start a write tx and modify, then rollback.
	tx, err := db.Begin(true)
	require.NoError(t, err)
	b := tx.Bucket([]byte("tx"))
	require.NoError(t, b.Put([]byte("k"), []byte("v2")))
	require.NoError(t, tx.Rollback())

	// Verify original value still present.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("tx"))
		require.Equal(t, "v1", string(b.Get([]byte("k"))))
		return nil
	})
	require.NoError(t, err)
}

func TestMemoryMode_NoFileLeftBehind(t *testing.T) {

	path := "memory-no-leak.db"
	_ = os.Remove(path)

	db, err := Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// In pure memory mode no file should be created.
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "memory mode should not create a file")
}

// TestMemoryMode_MVCCIsolation verifies that a long-running read transaction
// does not see modifications made by a concurrent write transaction.
func TestMemoryMode_MVCCIsolation(t *testing.T) {

	db, err := Open("memory-mvcc.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Insert initial value.
	err = db.Update(func(tx *Tx) error {
		b, _ := tx.CreateBucket([]byte("mvcc"))
		return b.Put([]byte("key"), []byte("v1"))
	})
	require.NoError(t, err)

	// Start a read-only transaction and keep it open.
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer func() { _ = readTx.Rollback() }()

	// Verify initial value in read tx.
	b := readTx.Bucket([]byte("mvcc"))
	require.Equal(t, "v1", string(b.Get([]byte("key"))))

	// Perform a write transaction that updates the value.
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("mvcc"))
		return b.Put([]byte("key"), []byte("v2"))
	})
	require.NoError(t, err)

	// The old read transaction should still see v1.
	require.Equal(t, "v1", string(b.Get([]byte("key"))))

	// A new read transaction should see v2.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("mvcc"))
		require.Equal(t, "v2", string(b.Get([]byte("key"))))
		return nil
	})
	require.NoError(t, err)
}

func TestMemoryMode_PutCopiesValue(t *testing.T) {
	db, err := Open("memory-put-copies-value.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	value := []byte("world")
	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		return b.Put([]byte("hello"), value)
	})
	require.NoError(t, err)

	copy(value, []byte("xxxxx"))

	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		require.Equal(t, "world", string(b.Get([]byte("hello"))))
		return nil
	})
	require.NoError(t, err)
}

// TestMemoryMode_PageReuseIsolation verifies that when pages are freed and
// reused by a write transaction, old read transactions still see stable data.
func TestMemoryMode_PageReuseIsolation(t *testing.T) {

	db, err := Open("memory-reuse.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Create bucket and fill with enough data to cause page splits.
	err = db.Update(func(tx *Tx) error {
		b, _ := tx.CreateBucket([]byte("data"))
		for i := 0; i < 1000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%08d", i)), []byte("filler-data-here")); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Start a read transaction.
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer func() { _ = readTx.Rollback() }()

	// Capture a cursor snapshot in the old read tx.
	b := readTx.Bucket([]byte("data"))
	c := b.Cursor()
	firstKey, firstVal := c.First()
	require.NotNil(t, firstKey)
	require.Equal(t, "filler-data-here", string(firstVal))

	// Delete all keys and insert new ones in a write transaction.
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.First() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		for i := 0; i < 1000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("new-%08d", i)), []byte("new-data-here-new")); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// The old read transaction should still see the old data.
	require.Equal(t, "filler-data-here", string(firstVal))
	// Re-position cursor and verify old data is still readable.
	c2 := b.Cursor()
	k, v := c2.First()
	require.NotNil(t, k)
	require.Equal(t, "filler-data-here", string(v))
}

// TestMemoryMode_ConcurrentWriters verifies that only one write transaction
// proceeds at a time.
func TestMemoryMode_ConcurrentWriters(t *testing.T) {

	db, err := Open("memory-concurrent.db", 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("counter"))
		return err
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 10
	numIterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				err := db.Update(func(tx *Tx) error {
					b := tx.Bucket([]byte("counter"))
					key := []byte(fmt.Sprintf("g%d", id))
					return b.Put(key, []byte(fmt.Sprintf("%d", j)))
				})
				require.NoError(t, err)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent writers timed out")
	}

	// Verify all writes succeeded.
	err = db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("counter"))
		for i := 0; i < numGoroutines; i++ {
			key := []byte(fmt.Sprintf("g%d", i))
			v := b.Get(key)
			require.NotNil(t, v)
			require.Equal(t, fmt.Sprintf("%d", numIterations-1), string(v))
		}
		return nil
	})
	require.NoError(t, err)
}
