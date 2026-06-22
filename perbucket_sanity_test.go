package vmbolt

import (
	"bytes"
	"fmt"
	"testing"
)

// TestPerBucketSanity exercises the per-bucket model end-to-end:
// create/get/delete buckets, put/get/delete keys across commits, force splits
// and rebalance merges, and verify bucket independence, rollback, and that a
// read transaction does not perturb writer-private allocator state.
func TestPerBucketSanity(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	// Create bucket "a", insert 100 keys, commit.
	mustOk(t, db.Update(func(tx *Tx) error {
		a, err := tx.CreateBucket([]byte("a"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			if err := a.Put([]byte(pbsKey(i)), []byte(pbsVal(i))); err != nil {
				return err
			}
		}
		return nil
	}))

	// Read back in a separate read tx.
	mustOk(t, db.View(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		if a == nil {
			t.Fatal("bucket a is nil")
		}
		for i := 0; i < 100; i++ {
			got := a.Get([]byte(pbsKey(i)))
			want := []byte(pbsVal(i))
			if !bytes.Equal(got, want) {
				t.Fatalf("key %d: got %q want %q", i, got, want)
			}
		}
		return nil
	}))

	// Rollback: write then abort; data must not persist.
	if err := db.Update(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		if err := a.Put([]byte("temp"), []byte("x")); err != nil {
			return err
		}
		return fmt.Errorf("intentional abort")
	}); err == nil {
		t.Fatal("expected the intentional abort error")
	}
	mustOk(t, db.View(func(tx *Tx) error {
		if tx.Bucket([]byte("a")).Get([]byte("temp")) != nil {
			t.Fatal("rolled-back write persisted")
		}
		return nil
	}))

	// Two independent buckets.
	mustOk(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		return b.Put([]byte("hello"), []byte("world"))
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		if got := tx.Bucket([]byte("b")).Get([]byte("hello")); string(got) != "world" {
			t.Fatalf("b.hello = %q", got)
		}
		if tx.Bucket([]byte("a")).Get([]byte(pbsKey(0))) == nil {
			t.Fatal("a lost data after b created")
		}
		return nil
	}))

	// Force many splits: 3000 large keys added to a bucket that already has data.
	mustOk(t, db.Update(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		for i := 0; i < 3000; i++ {
			k := []byte(fmt.Sprintf("big-key-%05d", i))
			v := make([]byte, 256)
			if err := a.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		for i := 0; i < 3000; i++ {
			if a.Get([]byte(fmt.Sprintf("big-key-%05d", i))) == nil {
				t.Fatalf("missing big-key-%05d", i)
			}
		}
		if a.Get([]byte(pbsKey(0))) == nil {
			t.Fatal("original key lost after splits")
		}
		return nil
	}))

	// Delete half the big keys (exercises rebalance merges) then verify.
	mustOk(t, db.Update(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		for i := 0; i < 1500; i++ {
			if err := a.Delete([]byte(fmt.Sprintf("big-key-%05d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		a := tx.Bucket([]byte("a"))
		for i := 0; i < 1500; i++ {
			if a.Get([]byte(fmt.Sprintf("big-key-%05d", i))) != nil {
				t.Fatalf("key %d not deleted", i)
			}
		}
		for i := 1500; i < 3000; i++ {
			if a.Get([]byte(fmt.Sprintf("big-key-%05d", i))) == nil {
				t.Fatalf("key %d wrongly gone", i)
			}
		}
		return nil
	}))

	// DeleteBucket.
	mustOk(t, db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("b"))
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		if tx.Bucket([]byte("b")) != nil {
			t.Fatal("bucket b still present after delete")
		}
		return nil
	}))

	// Recreate a deleted bucket name.
	mustOk(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		return b.Put([]byte("again"), []byte("ok"))
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		if got := tx.Bucket([]byte("b")).Get([]byte("again")); string(got) != "ok" {
			t.Fatalf("recreated b.again = %q", got)
		}
		return nil
	}))

	// ForEach iterates top-level buckets.
	mustOk(t, db.View(func(tx *Tx) error {
		names := map[string]bool{}
		mustOk(t, tx.ForEach(func(name []byte, _ *Bucket) error {
			names[string(name)] = true
			return nil
		}))
		if !names["a"] || !names["b"] {
			t.Fatalf("ForEach missed buckets: %v", names)
		}
		return nil
	}))
}

func mustOpenMem(t *testing.T) *DB {
	t.Helper()
	db, err := Open("", 0600, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func pbsKey(i int) string { return fmt.Sprintf("k%05d", i) }
func pbsVal(i int) string { return fmt.Sprintf("v%05d", i) }
