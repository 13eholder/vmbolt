# vmbolt

`vmbolt` is a **pure-memory**, **per-bucket** key/value store — a fork of
[bbolt][bbolt] rebuilt around an in-memory node graph instead of an mmap'd file.

- **Module:** `13eholder/vmbolt` · **package:** `vmbolt`
- Every top-level bucket is an **independent, atomically-published B+tree**.
- **No on-disk persistence** by default: the whole database lives in process
  memory. A snapshot can be written/read back on demand (BMSP format), but there
  is no per-commit `fsync`, no WAL, and no file lock.
- The bbolt transaction/bucket/cursor API shape is preserved (`Update`, `View`,
  `Batch`, `Bucket`, `Cursor`, …), so existing call sites port with minimal
  changes.

> vmbolt is for workloads that want bbolt's ACID transaction semantics and
> ordered B+tree access **without a disk** — caches, transient indexes, session
> stores, test doubles, or a non-durable etcd-like store. If you need
> durability across restarts, use upstream bbolt.

[bbolt]: https://github.com/etcd-io/bbolt

---

## Table of contents

- [Install](#install)
- [Quick start](#quick-start)
- [The model](#the-model)
  - [Per-bucket atomic trees](#per-bucket-atomic-trees)
  - [Nid layout](#nid-layout)
  - [Read snapshot isolation](#read-snapshot-isolation)
  - [Single writer](#single-writer)
- [Snapshots & restore (BMSP)](#snapshots--restore-bmsp)
- [Cohorts: atomic bucket groups](#cohorts-atomic-bucket-groups)
- [API reference](#api-reference)
- [Differences from bbolt](#differences-from-bbolt)
- [Caveats & limitations](#caveats--limitations)
- [etcd backend notes](#etcd-backend-notes)

---

## Install

```sh
go get 13eholder/vmbolt@latest
```

```go
import "13eholder/vmbolt"

db, err := vmbolt.Open("", 0600, nil) // path is ignored for storage
if err != nil {
	return err
}
defer db.Close()
```

`Open(path, …)` does **not** create or read a regular bbolt file. The `path` is
kept only so that, if it happens to hold a [BMSP snapshot](#snapshots--restore-bmsp),
the database rehydrates from it on open; otherwise the DB starts empty.

---

## Quick start

```go
db, err := vmbolt.Open("", 0600, nil)
if err != nil { log.Fatal(err) }
defer db.Close()

// Write
err = db.Update(func(tx *vmbolt.Tx) error {
	bucket, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
	if err != nil { return err }
	return bucket.Put([]byte("answer"), []byte("42"))
})

// Read
_ = db.View(func(tx *vmbolt.Tx) error {
	v := tx.Bucket([]byte("MyBucket")).Get([]byte("answer"))
	fmt.Printf("The answer is: %s\n", v) // 42
	return nil
})

// Range scan
_ = db.View(func(tx *vmbolt.Tx) error {
	c := tx.Bucket([]byte("MyBucket")).Cursor()
	for k, v := c.Seek([]byte("a")); k != nil; k, v = c.Next() {
		fmt.Printf("%s = %s\n", k, v)
	}
	return nil
})
```

---

## The model

### Per-bucket atomic trees

The database is a directory of named buckets:

```
DB.buckets  map[string]*bucketHandle
              └─ each handle owns atomic.Pointer[bucketState]
                 bucketState{ root, nodes map[Nid]*snapNode, sequence }
```

- Each top-level bucket is its own B+tree, published through its own atomic
  pointer. A commit rebuilds and republishes **only the buckets it touched**;
  untouched buckets are not churned.
- Writes use copy-on-write: a write transaction materializes mutable
  `workNode`s from the immutable `snapNode`s, mutates them, and on commit
  *freezes* them back into new immutable `snapNode`s (zero-copy inode transfer)
  and atomically swaps the bucket's published state.
- Node ids are **recyclable**: a per-bucket LIFO freelist reuses ids freed by
  deletes/rebalance, so long-lived churn does not run the id space up.

### Nid layout

A node id is 64 bits:

```
[ BucketId (16 bits) ][ NodeId (48 bits) ]
```

- `BucketId` (high 16) makes every Nid globally unique and self-routing; it is
  assigned when a bucket is created and recycled when the bucket is deleted
  (max **65 535** concurrent buckets).
- `NodeId` (low 48) is bucket-local and recycled via the per-bucket freelist.

### Read snapshot isolation

A read transaction pins each bucket's current generation **lazily, on first
access**, and holds that pinned generation for the transaction's lifetime.
Within a single bucket, reads are consistent. Across buckets, a read
transaction may observe different buckets at different generations — this is
intentional and is the price of independent per-bucket publish.

### Single writer

There is one global write lock (`db.Update`/`Begin(true)`). Reads are
lock-free (they dereference atomic pointers). Commit is **build-all-then-
publish-all**: every touched bucket's new state is constructed first, then
published, so a failed commit publishes nothing.

---

## Snapshots & restore (BMSP)

vmbolt is non-durable, but it can serialize the whole database to a **BMSP**
stream on demand and read it back:

```go
// Save
_ = db.View(func(tx *vmbolt.Tx) error {
	return tx.CopyFile("/tmp/snap.bmsp", 0600) // or tx.WriteTo(w)
})

// Restore into a fresh DB
db2, _ := vmbolt.Open("/tmp/snap.bmsp", 0600, nil) // Open auto-restores a BMSP file
// or explicitly: db2.Restore(file)
```

BMSP is a compact, fully-ordered, streaming format (buckets in sorted name
order via `Tx.Cursor`, keys sorted within each bucket), so a snapshot is
**deterministic** — saving and restoring produces a byte-identical tree.
Snapshots are written **only when you ask** (`WriteTo`/`CopyFile`); there is no
background persistence. Between snapshots, data is volatile.

---

## Cohorts: atomic bucket groups

By default buckets are published independently, so there is **no cross-bucket
atomicity**. Some workloads (notably etcd's `consistent_index` vs its MVCC
`key` tree) need a small set of buckets to be observed **jointly**. vmbolt
provides a *cohort* for exactly that:

```go
ag := db.NewCohort()
_ = db.Update(func(tx *vmbolt.Tx) error {
	if _, err := tx.AssignCohort([]byte("meta"), ag); err != nil { return err }
	_, err := tx.AssignCohort([]byte("key"), ag)
	return err
})
```

Members of a cohort share one atomic publication point: a reader loading the
cohort snapshot sees **all members at one consistent generation**. Non-member
buckets keep being published independently, so the cost is paid only by the
buckets that actually need joint consistency.

---

## API reference

Database:

| Method | Notes |
|--------|-------|
| `Open(path, mode, opts)` | Open (empty) or rehydrate from a BMSP file at `path` |
| `Close()` | Release resources |
| `Update(fn)` / `View(fn)` / `Batch(fn)` | Managed read-write / read-only / batched tx |
| `Begin(writable)` | Manual transaction (remember to `Commit`/`Rollback`) |
| `NewCohort()` | Create an atomic bucket group |
| `Restore(r)` | Rehydrate from a BMSP reader |
| `Stats()`, `Info()`, `Path()`, `Logger()` | Diagnostics |

Transaction:

| Method | Notes |
|--------|-------|
| `Bucket(name)` | Open a top-level bucket (nil if absent) |
| `CreateBucket` / `CreateBucketIfNotExists` / `DeleteBucket` | Bucket lifecycle |
| `ForEach(fn)` | Iterate top-level buckets (**unordered** — map iteration) |
| `Cursor()` | Sorted cursor over top-level **bucket names** (deterministic) |
| `Size()` | Whole-DB logical size estimate |
| `WriteTo(w)` / `Copy(w)` / `CopyFile(path, mode)` | Emit a BMSP snapshot |
| `OnCommit(fn)` | Post-commit hook |

Bucket:

| Method | Notes |
|--------|-------|
| `Put` / `Get` / `Delete` | Key/value ops |
| `ForEach(fn)` / `Cursor()` | Sorted iteration / point lookup |
| `Sequence` / `SetSequence` / `NextSequence` | Per-bucket autoincrement |
| `Stats()` | Per-bucket statistics |

---

## Differences from bbolt

Removed (disk-era machinery the pure-memory engine does not need):

- mmap'd file, `fdatasync`/`msync`, WAL, file lock, `Mlock`, `grow`.
- The on-disk freelist package; allocation is a per-bucket monotonic counter +
  LIFO recycle.
- The `cmd/bbolt` CLI and the `internal/{surgeon,guts_cli,freelist,tests}` +
  `tests/*` disk tooling.
- Nested buckets — only **flat top-level** buckets are supported.
- Disk-oriented `Options` fields (`NoSync`, `MmapFlags`, `InitialMmapSize`,
  `FreelistType`, `Mlock`, `Timeout`, …) and `DB.Sync`/`grow`/`loadFreelist`.
- `Tx.Page` (node ids are bucket-local) and `Tx.Check` (currently a no-op stub).

Added:

- Per-bucket atomic publish, `Nid = BucketId|NodeId`, per-bucket id recycling.
- BMSP snapshot format + `Open` auto-restore.
- `Tx.Cursor()` over sorted top-level bucket names; cohorts for joint atomicity.

---

## Caveats & limitations

- **Volatile by design.** A process crash/exit loses everything not captured in a
  snapshot. vmbolt is *not* a replacement for a durable store.
- **Single writer.** One write transaction at a time (global lock). Reads are
  concurrent with the writer.
- **No cross-bucket atomicity by default.** Use a [cohort](#cohorts-atomic-bucket-groups)
  for the buckets that must advance together.
- **Lazy per-bucket read snapshot.** A read tx pins each bucket on first access,
  not at `Begin`; a long-lived read tx that waits before reading a bucket will
  see that bucket's then-current generation.
- **≤ 65 535 top-level buckets** (16-bit `BucketId`, recycled on delete).
- **Byte slices from `Get`/`Cursor` are valid only for the transaction's
  lifetime** (they reference shared immutable storage). Copy them if you need
  them beyond the tx.
- **`Tx.Check` is a no-op stub** (returns no errors). Consistency checking is
  not yet implemented for the per-bucket model.

---

## etcd backend notes

vmbolt's API was shaped with [etcd][etcd]'s `backend` package in mind (etcd uses
only flat top-level buckets, never nested). The pieces etcd depends on are
present:

- sorted `Tx.Cursor()` (deterministic `Hash`/snapshot),
- `Tx.WriteTo`/snapshot streaming (BMSP),
- `Tx.Size()`,
- a cohort so `meta` + `key` stay jointly consistent.

The fundamental gap is **durability**: etcd requires a durable, recoverable
backend, while vmbolt is memory-only (snapshot-on-demand, volatile between
snapshots). vmbolt can serve as a **non-durable / in-memory etcd** (testing,
caching, ephemeral sidecars), not as a drop-in for production etcd storage.

[etcd]: https://github.com/etcd-io/etcd
