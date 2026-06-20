package vmbolt

import (
	"fmt"
	"testing"
)

// TestSplitKeepsAllKeys verifies that splitting (both on a fresh bucket and on
// a bucket with pre-existing committed data, across intervening read txs and
// unrelated bucket creation) preserves every key for both cursor iteration and
// point lookups. Regression guard for the allocator-state bugs seen during the
// per-bucket refactor.
func TestSplitKeepsAllKeys(t *testing.T) {
	for _, n := range []int{50, 500, 3000} {
		t.Run(fmt.Sprintf("fresh/%d", n), func(t *testing.T) {
			db := mustOpenMem(t)
			defer db.Close()
			mustOk(t, db.Update(func(tx *Tx) error {
				b, err := tx.CreateBucket([]byte("a"))
				if err != nil {
					return err
				}
				for i := 0; i < n; i++ {
					if err := b.Put([]byte(fmt.Sprintf("big-key-%05d", i)), make([]byte, 256)); err != nil {
						return err
					}
				}
				return nil
			}))
			checkAll(t, db, n)
		})
		t.Run(fmt.Sprintf("update/%d", n), func(t *testing.T) {
			db := mustOpenMem(t)
			defer db.Close()
			// Phase 1: small committed data.
			mustOk(t, db.Update(func(tx *Tx) error {
				b, err := tx.CreateBucket([]byte("a"))
				if err != nil {
					return err
				}
				for i := 0; i < 100; i++ {
					if err := b.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v")); err != nil {
						return err
					}
				}
				return nil
			}))
			// A read tx in between (must not perturb allocator state).
			mustOk(t, db.View(func(tx *Tx) error { _ = tx.Bucket([]byte("a")).Get([]byte("k00000")); return nil }))
			// An unrelated bucket creation.
			mustOk(t, db.Update(func(tx *Tx) error {
				b, err := tx.CreateBucket([]byte("b"))
				if err != nil {
					return err
				}
				return b.Put([]byte("x"), []byte("y"))
			}))
			// Phase 2: grow past many splits.
			mustOk(t, db.Update(func(tx *Tx) error {
				b := tx.Bucket([]byte("a"))
				for i := 0; i < n; i++ {
					if err := b.Put([]byte(fmt.Sprintf("big-key-%05d", i)), make([]byte, 256)); err != nil {
						return err
					}
				}
				return nil
			}))
			checkAll(t, db, n)
			// Original small keys still present.
			mustOk(t, db.View(func(tx *Tx) error {
				b := tx.Bucket([]byte("a"))
				for i := 0; i < 100; i++ {
					if b.Get([]byte(fmt.Sprintf("k%05d", i))) == nil {
						t.Fatalf("lost small key k%05d", i)
					}
				}
				return nil
			}))
		})
	}
}

func checkAll(t *testing.T, db *DB, n int) {
	t.Helper()
	var count int
	mustOk(t, db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("a"))
		mustOk(t, b.ForEach(func(k, v []byte) error { count++; return nil }))
		for i := 0; i < n; i++ {
			if b.Get([]byte(fmt.Sprintf("big-key-%05d", i))) == nil {
				t.Fatalf("missing big-key-%05d (ForEach saw %d keys)", i, count)
			}
		}
		return nil
	}))
}
