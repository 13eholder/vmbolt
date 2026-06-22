package vmbolt

import "testing"

func TestTx_AllocFreeRecycleNid(t *testing.T) {
	db := mustOpenMem(t)
	defer func() { _ = db.Close() }()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("begin write tx: %v", err)
	}

	n0 := tx.allocateNid()
	n1 := tx.allocateNid()
	if n0 != 1 || n1 != 2 {
		t.Fatalf("expected node ids 1,2; got %d,%d", n0, n1)
	}

	tx.freeNid(n0)
	n2 := tx.allocateNid()
	if n2 != n0 {
		t.Fatalf("expected recycled node id %d; got %d", n0, n2)
	}

	n3 := tx.allocateNid()
	if n3 != 3 {
		t.Fatalf("expected fresh node id 3; got %d", n3)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
