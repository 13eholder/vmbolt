package vmbolt

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// TestCohort_BasicCRUD creates two cohort members, writes/reads them, and checks
// they resolve through the cohort snapshot.
func TestCohort_BasicCRUD(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	ag := db.NewCohort()
	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.AssignCohort([]byte("meta"), ag)
		if err != nil {
			return err
		}
		key, err := tx.AssignCohort([]byte("key"), ag)
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
		// Both handles must share the same cohort, and the cohort snapshot must
		// carry both members.
		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		hk := tx.db.buckets["key"]
		tx.db.bucketsMu.RUnlock()
		if hm.cohort == nil || hm.cohort != hk.cohort {
			t.Fatalf("meta/key not in same cohort: %p vs %p", hm.cohort, hk.cohort)
		}
		snap := hm.cohort.state.Load()
		if snap == nil || len(snap.members) != 2 {
			t.Fatalf("cohort snapshot members = %d (want 2)", mapLen(snap))
		}
		return nil
	}))

	if s := mustSize(t, db); s <= 0 {
		t.Fatalf("size = %d, want > 0", s)
	}

	// A third, independent bucket is NOT in the cohort.
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
		if hs.cohort != nil {
			t.Fatal("solo bucket should be independent (no cohort)")
		}
		return nil
	}))
}

func mapLen(s *cohortSnapshot) int {
	if s == nil {
		return -1
	}
	return len(s.members)
}

// TestCohort_JointAtomicVisibility drives a writer that advances meta.ci and
// key.maxv together, and a concurrent reader that asserts they are always equal.
// With the cohort they share one atomic publication point, so a reader can never
// observe one member at a newer generation than the other.
func TestCohort_JointAtomicVisibility(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	ag := db.NewCohort()
	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.AssignCohort([]byte("meta"), ag)
		if err != nil {
			return err
		}
		key, err := tx.AssignCohort([]byte("key"), ag)
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
		t.Fatalf("cohort observed %d inconsistent reads (meta.v != key.v)", mismatches)
	}
}

// TestCohort_AdoptExisting creates two independent buckets, then adopts them
// into a cohort, and verifies data survives + they become jointly published.
func TestCohort_AdoptExisting(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	// Independent buckets with data.
	mustOk(t, db.Update(func(tx *Tx) error {
		meta, err := tx.CreateBucket([]byte("meta"))
		if err != nil {
			return err
		}
		key, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("ci"), []byte("7")); err != nil {
			return err
		}
		return key.Put([]byte("k"), []byte("v"))
	}))

	// Adopt both into a cohort in one tx.
	ag := db.NewCohort()
	mustOk(t, db.Update(func(tx *Tx) error {
		if _, err := tx.AssignCohort([]byte("meta"), ag); err != nil {
			return err
		}
		_, err := tx.AssignCohort([]byte("key"), ag)
		return err
	}))

	// Data survives adoption and reads route through the cohort.
	mustOk(t, db.View(func(tx *Tx) error {
		if string(tx.Bucket([]byte("meta")).Get([]byte("ci"))) != "7" {
			t.Fatal("meta.ci lost on adopt")
		}
		if string(tx.Bucket([]byte("key")).Get([]byte("k"))) != "v" {
			t.Fatal("key.k lost on adopt")
		}
		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		hk := tx.db.buckets["key"]
		tx.db.bucketsMu.RUnlock()
		if hm.cohort == nil || hm.cohort != hk.cohort {
			t.Fatal("adopted buckets not in same cohort")
		}
		if snap := hm.cohort.state.Load(); snap == nil || len(snap.members) != 2 {
			t.Fatalf("cohort members after adopt = %d", mapLen(snap))
		}
		return nil
	}))

	// Further writes go through the cohort.
	mustOk(t, db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("ci"), []byte("8"))
	}))
	mustOk(t, db.View(func(tx *Tx) error {
		if string(tx.Bucket([]byte("meta")).Get([]byte("ci"))) != "8" {
			t.Fatal("post-adopt write lost")
		}
		return nil
	}))
}

// TestCohort_DeleteMember deletes one cohort member and verifies the other is
// unaffected and the cohort snapshot no longer carries the deleted one.
func TestCohort_DeleteMember(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	ag := db.NewCohort()
	mustOk(t, db.Update(func(tx *Tx) error {
		if _, err := tx.AssignCohort([]byte("meta"), ag); err != nil {
			return err
		}
		if _, err := tx.AssignCohort([]byte("key"), ag); err != nil {
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
			t.Fatal("meta data lost after deleting cohort sibling")
		}
		tx.db.bucketsMu.RLock()
		hm := tx.db.buckets["meta"]
		tx.db.bucketsMu.RUnlock()
		if snap := hm.cohort.state.Load(); snap != nil {
			if _, ok := snap.members["key"]; ok {
				t.Fatal("deleted member still in cohort snapshot")
			}
			if len(snap.members) != 1 {
				t.Fatalf("cohort members after delete = %d (want 1)", len(snap.members))
			}
		}
		return nil
	}))
}
