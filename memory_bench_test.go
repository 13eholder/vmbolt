package vmbolt

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func benchmarkPut(b *testing.B, valueSize int) {
	b.Helper()
	err := os.MkdirAll(b.TempDir(), 0755)
	if err != nil {
		b.Fatal(err)
	}
	path := fmt.Sprintf("%s/bench-put.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	value := make([]byte, valueSize)
	rng := rand.New(rand.NewSource(42))
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			return bucket.Put([]byte(fmt.Sprintf("%016d", i)), value)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPut_128B(b *testing.B) {
	benchmarkPut(b, 128)
}

func BenchmarkPut_1KB(b *testing.B) {
	benchmarkPut(b, 1024)
}

func BenchmarkPut_4KB(b *testing.B) {
	benchmarkPut(b, 4096)
}

func benchmarkGet(b *testing.B, valueSize int) {
	b.Helper()
	path := fmt.Sprintf("%s/bench-get.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const numKeys = 10000
	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bench"))
		if err != nil {
			return err
		}
		value := make([]byte, valueSize)
		for i := 0; i < numKeys; i++ {
			if err := bucket.Put([]byte(fmt.Sprintf("%016d", i)), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		keyIdx := rng.Intn(numKeys)
		err := db.View(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			_ = bucket.Get([]byte(fmt.Sprintf("%016d", keyIdx)))
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet_128B(b *testing.B) {
	benchmarkGet(b, 128)
}

func BenchmarkGet_1KB(b *testing.B) {
	benchmarkGet(b, 1024)
}

func benchmarkCursor(b *testing.B, numKeys int) {
	b.Helper()
	path := fmt.Sprintf("%s/bench-cursor.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("bench"))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			if err := bucket.Put([]byte(fmt.Sprintf("%016d", i)), []byte("value")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.View(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			c := bucket.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCursor_1K(b *testing.B) {
	benchmarkCursor(b, 1000)
}

func BenchmarkCursor_10K(b *testing.B) {
	benchmarkCursor(b, 10000)
}

func benchmarkBatch(b *testing.B, parallelism int) {
	b.Helper()
	path := fmt.Sprintf("%s/bench-batch.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		done := make(chan struct{}, parallelism)

		for j := 0; j < parallelism; j++ {
			go func(id int) {
				<-start
				key := fmt.Sprintf("%d-%016d", id, i)
				err := db.Update(func(tx *Tx) error {
					bucket := tx.Bucket([]byte("bench"))
					return bucket.Put([]byte(key), []byte("value"))
				})
				if err != nil {
					b.Error(err)
				}
				done <- struct{}{}
			}(j)
		}
		close(start)
		for j := 0; j < parallelism; j++ {
			<-done
		}
	}
}

func BenchmarkBatch_4(b *testing.B) {
	benchmarkBatch(b, 4)
}

func BenchmarkBatch_16(b *testing.B) {
	benchmarkBatch(b, 16)
}

func benchmarkSequentialReadWrite(b *testing.B, valueSize int) {
	b.Helper()
	path := fmt.Sprintf("%s/bench-seq.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	value := make([]byte, valueSize)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := db.Update(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			return bucket.Put(key, value)
		})
		if err != nil {
			b.Fatal(err)
		}
		err = db.View(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			_ = bucket.Get(key)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSeqReadWrite_128B(b *testing.B) {
	benchmarkSequentialReadWrite(b, 128)
}

func BenchmarkSeqReadWrite_1KB(b *testing.B) {
	benchmarkSequentialReadWrite(b, 1024)
}

func BenchmarkLatency_Put_128B(b *testing.B) {
	benchmarkLatencyPut(b, 128)
}

func benchmarkLatencyPut(b *testing.B, valueSize int) {
	b.Helper()
	path := fmt.Sprintf("%s/bench-latency.db", b.TempDir())

	db, err := Open(path, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	value := make([]byte, valueSize)
	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := db.Update(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("bench"))
			return bucket.Put([]byte(fmt.Sprintf("%016d", i)), value)
		})
		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
	}
	b.StopTimer()

	// Compute and report percentiles.
	reportLatencyPercentiles(b, latencies)
}

func reportLatencyPercentiles(b *testing.B, latencies []time.Duration) {
	// Use quickselect-like approach or just sort.
	// Sorting b.N elements can be expensive for large N, but for reporting it's fine.
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	// Simple sort - for benchmarks this is acceptable post-measurement.
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50 := sorted[len(sorted)*50/100]
	p90 := sorted[len(sorted)*90/100]
	p95 := sorted[len(sorted)*95/100]
	p99 := sorted[len(sorted)*99/100]

	b.ReportMetric(float64(p50.Nanoseconds()), "ns/p50")
	b.ReportMetric(float64(p90.Nanoseconds()), "ns/p90")
	b.ReportMetric(float64(p95.Nanoseconds()), "ns/p95")
	b.ReportMetric(float64(p99.Nanoseconds()), "ns/p99")
}
