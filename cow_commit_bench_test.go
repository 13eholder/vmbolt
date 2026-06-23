package vmbolt

import (
	"fmt"
	"testing"
)

// BenchmarkCommitScaling measures the cost of a single-key write+commit as the
// bucket grows. With the path-copy COW B+tree (branch inodes hold *node child
// pointers, commit publishes a new root) this scales ~O(log N): each commit
// copies only the root-to-leaf path and publishes one new root pointer, so the
// per-commit cost stays nearly flat as the bucket grows — in contrast to the
// previous O(N) per-commit full-map rebuild.
//
// Run: go test -bench BenchmarkCommitScaling -benchtime=1s ./...
// Compare sizes with benchstat; expect ns/op to grow sub-linearly with size.
func BenchmarkCommitScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("bucket=%d", size), func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(fmt.Sprintf("%s/bench.db", dir), 0600, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateBucket([]byte("b"))
				return err
			}); err != nil {
				b.Fatal(err)
			}

			// Pre-build a bucket of `size` keys (its own commits, not measured).
			const batchSize = 1000
			for i := 0; i < size; i += batchSize {
				end := i + batchSize
				if end > size {
					end = size
				}
				if err := db.Update(func(tx *Tx) error {
					bk := tx.Bucket([]byte("b"))
					for j := i; j < end; j++ {
						if err := bk.Put([]byte(fmt.Sprintf("%010d", j)), []byte("x")); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}

			// Overwrite one existing key per iteration: a single-leaf change that
			// exercises path-copy COW + publish, isolating per-commit cost.
			key := []byte(fmt.Sprintf("%010d", size/2))
			val := []byte("v")

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error {
					return tx.Bucket([]byte("b")).Put(key, val)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
