package vmbolt

import "testing"

func TestView_GlobalSnapshotAcrossBuckets(t *testing.T) {
	db := mustOpenMem(t)
	defer func() { _ = db.Close() }()

	if err := db.Update(func(tx *Tx) error {
		meta, err := tx.CreateBucket([]byte("meta"))
		if err != nil {
			return err
		}
		key, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		if err := meta.Put([]byte("rev"), []byte("1")); err != nil {
			return err
		}
		return key.Put([]byte("k"), []byte("v1"))
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer func() { _ = readTx.Rollback() }()

	if err := db.Update(func(tx *Tx) error {
		meta := tx.Bucket([]byte("meta"))
		key := tx.Bucket([]byte("key"))
		if err := meta.Put([]byte("rev"), []byte("2")); err != nil {
			return err
		}
		return key.Put([]byte("k"), []byte("v2"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	meta := readTx.Bucket([]byte("meta"))
	key := readTx.Bucket([]byte("key"))
	if got := string(meta.Get([]byte("rev"))); got != "1" {
		t.Fatalf("old read tx saw new meta revision: %q", got)
	}
	if got := string(key.Get([]byte("k"))); got != "v1" {
		t.Fatalf("old read tx saw new key value: %q", got)
	}

	if err := db.View(func(tx *Tx) error {
		meta := tx.Bucket([]byte("meta"))
		key := tx.Bucket([]byte("key"))
		if got := string(meta.Get([]byte("rev"))); got != "2" {
			t.Fatalf("new read tx expected meta revision 2, got %q", got)
		}
		if got := string(key.Get([]byte("k"))); got != "v2" {
			t.Fatalf("new read tx expected key value v2, got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
