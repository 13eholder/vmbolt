package vmbolt

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// TestCommitGroup_BasicCRUD creates two grouped buckets, writes/reads them, and
// checks they resolve through the group snapshot.
func TestCommitGroup_BasicCRUD(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	g := db.NewCommitGroup()
	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.CreateBucketWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g})
		if err != nil {
			return err
		}
		key, err := tx.CreateBucketWithOptions([]byte("key"), &BucketOptions{CommitGroup: g})
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("ci"), []byte("42")); err != nil {
			return err
		}
		return key.Put([]byte("k1"), []byte("v1"))
	}))

	mustOk(t, db.View(func(tx *Tx) error {
		meta := tx.Bucket([]byte("meta"))
		key := tx.Bucket([]byte("key"))
		if string(meta.Get([]byte("ci"))) != "42" {
			t.Fatalf("meta.ci = %q", meta.Get([]byte("ci")))
		}
		if string(key.Get([]byte("k1"))) != "v1" {
			t.Fatalf("key.k1 = %q", key.Get([]byte("k1")))
		}

		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		hk := tx.db.buckets["key"]
		tx.db.bucketsMu.RUnlock()
		if hm.commitGroup == nil || hm.commitGroup != hk.commitGroup {
			t.Fatalf("meta/key not in same commit group: %p vs %p", hm.commitGroup, hk.commitGroup)
		}
		snap := hm.commitGroup.state.Load()
		if snap == nil || len(snap.members) != 2 {
			t.Fatalf("commit-group snapshot members = %d (want 2)", groupLen(snap))
		}
		return nil
	}))

	// A third, independent bucket is NOT in the group.
	mustOk(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("solo"))
		if err != nil {
			return err
		}
		return b.Put([]byte("x"), []byte("y"))
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		tx.db.bucketsMu.RLock()
		hs := tx.db.buckets["solo"]
		tx.db.bucketsMu.RUnlock()
		if hs.commitGroup != nil {
			t.Fatal("solo bucket should be independent (no commit group)")
		}
		return nil
	}))
}

func groupLen(s *commitGroupSnapshot) int {
	if s == nil {
		return -1
	}
	return len(s.members)
}

// TestCommitGroup_JointAtomicVisibility drives a writer that advances meta.v and
// key.v together, and a concurrent reader that asserts they are always equal.
func TestCommitGroup_JointAtomicVisibility(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	g := db.NewCommitGroup()
	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.CreateBucketWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g})
		if err != nil {
			return err
		}
		key, err := tx.CreateBucketWithOptions([]byte("key"), &BucketOptions{CommitGroup: g})
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("v"), []byte("0")); err != nil {
			return err
		}
		return key.Put([]byte("v"), []byte("0"))
	}))

	const N = 3000
	var mismatches int64
	var stop atomic.Bool

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 1; i <= N; i++ {
			ver := []byte(strconv.Itoa(i))
			_ = db.Update(func(tx *Tx) error {
				if err := tx.Bucket([]byte("meta")).Put([]byte("v"), ver); err != nil {
					return err
				}
				return tx.Bucket([]byte("key")).Put([]byte("v"), ver)
			})
		}
		stop.Store(true)
	}()

	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.View(func(tx *Tx) error {
				mv := tx.Bucket([]byte("meta")).Get([]byte("v"))
				kv := tx.Bucket([]byte("key")).Get([]byte("v"))
				if mv != nil && kv != nil && !bytes.Equal(mv, kv) {
					atomic.AddInt64(&mismatches, 1)
				}
				return nil
			})
		}
	}()

	wg.Wait()
	if mismatches != 0 {
		t.Fatalf("commit group observed %d inconsistent reads (meta.v != key.v)", mismatches)
	}
}

// TestCommitGroup_ExistingBucketCannotJoinGroup verifies there is no runtime
// adopt path: a committed independent bucket cannot later be recreated into a
// commit group.
func TestCommitGroup_ExistingBucketCannotJoinGroup(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.CreateBucket([]byte("meta"))
		if err != nil {
			return err
		}
		return meta.Put([]byte("ci"), []byte("7"))
	}))

	g := db.NewCommitGroup()
	err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucketIfNotExistsWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g})
		return err
	})
	if err == nil {
		t.Fatal("expected group mismatch error when trying to join an existing bucket to a commit group")
	}

	mustOk(t, db.View(func(tx *Tx) error {
		if string(tx.Bucket([]byte("meta")).Get([]byte("ci"))) != "7" {
			t.Fatal("meta.ci changed after rejected adopt")
		}
		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		tx.db.bucketsMu.RUnlock()
		if hm.commitGroup != nil {
			t.Fatal("independent bucket unexpectedly joined a commit group")
		}
		return nil
	}))
}

// TestCommitGroup_CreateBucketIfNotExists_MatchingGroup returns the existing
// bucket when the requested group matches.
func TestCommitGroup_CreateBucketIfNotExists_MatchingGroup(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	g := db.NewCommitGroup()
	mustOk(t, db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucketWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g}); err != nil {
			return err
		}
		b, err := tx.CreateBucketIfNotExistsWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g})
		if err != nil {
			return err
		}
		return b.Put([]byte("ci"), []byte("9"))
	}))

	mustOk(t, db.View(func(tx *Tx) error {
		if string(tx.Bucket([]byte("meta")).Get([]byte("ci"))) != "9" {
			t.Fatal("meta.ci write lost")
		}
		return nil
	}))
}

// TestCommitGroup_DeleteMember deletes one grouped bucket and verifies the other
// is unaffected and the group snapshot no longer carries the deleted member.
func TestCommitGroup_DeleteMember(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	g := db.NewCommitGroup()
	mustOk(t, db.Update(func(tx *Tx) error {
		if _, err := tx.CreateBucketWithOptions([]byte("meta"), &BucketOptions{CommitGroup: g}); err != nil {
			return err
		}
		if _, err := tx.CreateBucketWithOptions([]byte("key"), &BucketOptions{CommitGroup: g}); err != nil {
			return err
		}
		return tx.Bucket([]byte("meta")).Put([]byte("ci"), []byte("1"))
	}))

	mustOk(t, db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("key"))
	}))

	mustOk(t, db.View(func(tx *Tx) error {
		if tx.Bucket([]byte("key")) != nil {
			t.Fatal("key bucket should be gone")
		}
		if string(tx.Bucket([]byte("meta")).Get([]byte("ci"))) != "1" {
			t.Fatal("meta data lost after deleting grouped sibling")
		}
		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		tx.db.bucketsMu.RUnlock()
		if snap := hm.commitGroup.state.Load(); snap != nil {
			if _, ok := snap.members["key"]; ok {
				t.Fatal("deleted member still in commit-group snapshot")
			}
			if len(snap.members) != 1 {
				t.Fatalf("commit-group members after delete = %d (want 1)", len(snap.members))
			}
		}
		return nil
	}))
}
