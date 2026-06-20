package vmbolt

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// crcHash mirrors etcd's backend.Hash: CRC32 over sorted bucket names + sorted
// (key,value) pairs. Used to verify snapshot determinism and round-trip fidelity.
func crcHash(t *testing.T, db *DB) uint32 {
	t.Helper()
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	mustOk(t, db.View(func(tx *Tx) error {
		c := tx.Cursor()
		for name, _ := c.First(); name != nil; name, _ = c.Next() {
			b := tx.Bucket(name)
			if b == nil {
				t.Fatalf("bucket %q vanished during hash", name)
			}
			h.Write(name)
			mustOk(t, b.ForEach(func(k, v []byte) error {
				h.Write(k)
				h.Write(v)
				return nil
			}))
		}
		return nil
	}))
	return h.Sum32()
}

// TestTxDirCursor_Sorted verifies Tx.Cursor enumerates top-level bucket names in
// sorted order (etcd's Hash/snapshot/defrag depend on determinism here).
func TestTxDirCursor_Sorted(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	want := []string{"alpha", "bravo", "charlie", "delta"}
	// Create out of order.
	mustOk(t, db.Update(func(tx *Tx) error {
		for _, n := range []string{"charlie", "alpha", "delta", "bravo"} {
			if _, err := tx.CreateBucket([]byte(n)); err != nil {
				return err
			}
		}
		return nil
	}))

	var got []string
	mustOk(t, db.View(func(tx *Tx) error {
		c := tx.Cursor()
		for name, _ := c.First(); name != nil; name, _ = c.Next() {
			got = append(got, string(name))
		}
		return nil
	}))
	if !sort.StringsAreSorted(got) {
		t.Fatalf("cursor not sorted: %v", got)
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d buckets, got %d (%v)", len(want), len(got), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("bucket[%d]: got %q want %q", i, got[i], want[i])
		}
	}

	// Seek lands on the right name.
	mustOk(t, db.View(func(tx *Tx) error {
		c := tx.Cursor()
		k, _ := c.Seek([]byte("brump"))
		if string(k) != "charlie" {
			t.Fatalf("Seek(brump) = %q, want charlie", k)
		}
		return nil
	}))

	// Empty DB cursor yields nothing immediately.
	db2 := mustOpenMem(t)
	defer db2.Close()
	mustOk(t, db2.View(func(tx *Tx) error {
		k, _ := tx.Cursor().First()
		if k != nil {
			t.Fatalf("empty db cursor.First() = %q, want nil", k)
		}
		return nil
	}))
}

// TestSnapshot_RoundTrip writes a DB with multi-bucket data (some forcing
// splits) to a buffer via WriteTo, restores it into a fresh DB, and verifies the
// CRC hash matches and key/values are intact.
func TestSnapshot_RoundTrip(t *testing.T) {
	src := mustOpenMem(t)
	defer src.Close()

	mustOk(t, src.Update(func(tx *Tx) error {
		for _, name := range []string{"zebra", "apple", "mango"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			n := 100
			if name == "mango" {
				n = 3000 // force several splits / multi-level tree
			}
			for i := 0; i < n; i++ {
				k := []byte(bytefmt("key-%05d", i))
				v := make([]byte, 64)
				if err := b.Put(k, v); err != nil {
					return err
				}
			}
			if err := b.SetSequence(uint64(len(name))); err != nil {
				return err
			}
		}
		return nil
	}))

	wantHash := crcHash(t, src)
	wantSize := mustSize(t, src)
	if wantSize <= 0 {
		t.Fatalf("pre-snapshot size = %d, want > 0", wantSize)
	}

	var buf bytes.Buffer
	mustOk(t, src.View(func(tx *Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	}))
	if buf.Len() == 0 {
		t.Fatal("WriteTo produced no bytes")
	}

	// Restore into a fresh DB.
	dst := mustOpenMem(t)
	defer dst.Close()
	mustOk(t, dst.Restore(bytes.NewReader(buf.Bytes())))

	if got := crcHash(t, dst); got != wantHash {
		t.Fatalf("hash mismatch after round-trip: got %d want %d", got, wantHash)
	}
	// Size() is a structural estimate that depends on tree shape (fill/split
	// points); src (FillPercent 0.5) and dst (Restore uses 0.9) legitimately
	// differ in node count, so only assert it stays positive, not equal.
	if got := mustSize(t, dst); got <= 0 {
		t.Fatalf("restored size = %d, want > 0", got)
	}

	// Spot-check sequence + a key.
	mustOk(t, dst.View(func(tx *Tx) error {
		if got := tx.Bucket([]byte("mango")).Sequence(); got != uint64(len("mango")) {
			t.Fatalf("mango sequence = %d, want %d", got, len("mango"))
		}
		if v := tx.Bucket([]byte("mango")).Get([]byte(bytefmt("key-%05d", 2999))); v == nil {
			t.Fatal("missing mango key-02999 after restore")
		}
		return nil
	}))
}

// TestSnapshot_AutoRestoreOnOpen verifies Open(path) rehydrates from a BMSP file
// written by CopyFile.
func TestSnapshot_AutoRestoreOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.snap")

	src := mustOpenMem(t)
	defer src.Close()
	mustOk(t, src.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("persisted"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}))
	mustOk(t, src.View(func(tx *Tx) error { return tx.CopyFile(path, 0600) }))

	// Reopen the path: should auto-restore.
	dst, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatalf("Open restored db: %v", err)
	}
	defer dst.Close()
	mustOk(t, dst.View(func(tx *Tx) error {
		if got := tx.Bucket([]byte("persisted")).Get([]byte("k")); string(got) != "v" {
			t.Fatalf("auto-restore lost data: got %q", got)
		}
		return nil
	}))

	// Opening a NON-BMSP file (e.g. random bytes) must not error (start empty).
	junk := filepath.Join(dir, "junk")
	if err := os.WriteFile(junk, []byte("not a bolt db"), 0600); err != nil {
		t.Fatal(err)
	}
	jdb, err := Open(junk, 0600, nil)
	if err != nil {
		t.Fatalf("Open junk file errored: %v", err)
	}
	defer jdb.Close()
	mustOk(t, jdb.View(func(tx *Tx) error {
		if tx.Bucket([]byte("persisted")) != nil {
			t.Fatal("junk file should not have restored a bucket")
		}
		return nil
	}))
}

// TestTx_Size ensures Size() reflects the whole DB (not just touched buckets).
func TestTx_Size(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	// Empty DB: a fresh read tx reports a small/zero size.
	var empty int64
	mustOk(t, db.View(func(tx *Tx) error { empty = tx.Size(); return nil }))

	mustOk(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("big"))
		if err != nil {
			return err
		}
		for i := 0; i < 2000; i++ {
			if err := b.Put([]byte(bytefmt("k%05d", i)), make([]byte, 128)); err != nil {
				return err
			}
		}
		return nil
	}))

	// A read tx that touches NO buckets must still see the full size.
	var full int64
	mustOk(t, db.View(func(tx *Tx) error { full = tx.Size(); return nil }))
	if full <= empty {
		t.Fatalf("Size() did not grow after inserts: empty=%d full=%d", empty, full)
	}
}

func mustSize(t *testing.T, db *DB) int64 {
	t.Helper()
	var sz int64
	mustOk(t, db.View(func(tx *Tx) error { sz = tx.Size(); return nil }))
	return sz
}

func bytefmt(format string, i int) string {
	return fmt.Sprintf(format, i)
}
