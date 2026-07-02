package vmbolt

import (
	"fmt"
	"testing"
)

func TestPublishedTreeDropsTransactionContext(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()

	const valueSize = 50 * 1024
	value := make([]byte, valueSize)

	if err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("pods"))
		if err != nil {
			return err
		}
		for i := 0; i < 16; i++ {
			if err := b.Put([]byte(fmt.Sprintf("/registry/pods/node-a/pod-%04d", i)), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("pods"))
		if b == nil {
			t.Fatal("missing pods bucket")
		}
		for i := 0; i < 8; i++ {
			if err := b.Put([]byte(fmt.Sprintf("/registry/pods/node-b/pod-%04d", i)), value); err != nil {
				return err
			}
		}
		if err := b.Delete([]byte("/registry/pods/node-a/pod-0003")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	assertPublishedTreeDetached(t, db)
}

func assertPublishedTreeDetached(t *testing.T, db *DB) {
	t.Helper()

	st := db.state.Load()
	if st == nil {
		t.Fatal("published db state is nil")
	}
	for name, bs := range st.buckets {
		if bs == nil || bs.root == nil {
			continue
		}
		seen := make(map[*node]bool)
		var walk func(*node)
		walk = func(n *node) {
			t.Helper()
			if n == nil {
				t.Fatalf("bucket %q published tree contains nil node", name)
			}
			if seen[n] {
				return
			}
			seen[n] = true

			if n.bucket != nil {
				t.Fatalf("bucket %q published node %p retained tx-local bucket %p (tx=%p rootNode=%p dirtyLen=%d)",
					name, n, n.bucket, n.bucket.tx, n.bucket.rootNode, len(n.bucket.dirty))
			}
			if n.parent != nil {
				t.Fatalf("bucket %q published node %p retained parent pointer %p", name, n, n.parent)
			}
			for i := range n.inodes {
				if !n.isLeaf {
					walk(n.inodes[i].child)
				}
			}
		}
		walk(bs.root)
	}
}
