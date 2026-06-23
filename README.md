# vmbolt

`vmbolt` is a **pure-memory** key/value store — a fork of
[bbolt][bbolt] rebuilt around an in-memory node graph instead of an mmap'd file.

- **Module:** `github.com/13eholder/vmbolt` · **package:** `vmbolt`
- Top-level buckets stay **flat** and keep their own B+tree structure, but the
  database publishes **one globally consistent view per transaction commit**.
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
  - [Flat top-level buckets](#flat-top-level-buckets)
  - [Read snapshot isolation](#read-snapshot-isolation)
  - [Single writer](#single-writer)
- [Snapshots & restore (BMSP)](#snapshots--restore-bmsp)
- [API reference](#api-reference)
- [Differences from bbolt](#differences-from-bbolt)
- [Caveats & limitations](#caveats--limitations)
- [etcd backend notes](#etcd-backend-notes)

---

## Install

```sh
go get github.com/13eholder/vmbolt@latest
```

```go
import "github.com/13eholder/vmbolt"

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

### Flat top-level buckets

The database is a directory of named top-level buckets held inside one
published database state:

```
DB.state -> dbState{ buckets map[string]*bucketState }
```

- Each top-level bucket is a B+tree whose branch entries hold direct `*node`
  child pointers — the tree itself is the index; there is no flat node-id map.
- Readers observe all buckets through one globally published `dbState`, so a
  transaction sees one consistent database view across all buckets.
- Writes use **path-copy COW**: a write transaction copies only the root-to-leaf
  path it touches; unchanged subtrees are shared by pointer. Commit publishes one
  new root pointer per touched bucket — **O(log N)**, not a rebuilt index — so
  per-commit cost stays nearly flat as a bucket grows.

### Read snapshot isolation

A read transaction pins the current `dbState` at `Begin` time and holds that
global view for the transaction's lifetime. Reads are therefore consistent both
within a bucket and across buckets.

### Single writer

There is one global write lock (`db.Update`/`Begin(true)`). Commit is
**path-copy-then-publish**: each touched bucket gets a new root (built by
copying only the modified path), then one new `dbState` is atomically
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

## API reference

Database:

| Method | Notes |
|--------|-------|
| `Open(path, mode, opts)` | Open (empty) or rehydrate from a BMSP file at `path` |
| `Close()` | Release resources |
| `Update(fn)` / `View(fn)` / `Batch(fn)` | Managed read-write / read-only / batched tx |
| `Begin(writable)` | Manual transaction (remember to `Commit`/`Rollback`) |
| `Restore(r)` | Rehydrate from a BMSP reader |
| `Stats()`, `Info()`, `Logger()` | Diagnostics |

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
| `Stats()` | Per-bucket statistics |

---

## Differences from bbolt

Removed (disk-era machinery the pure-memory engine does not need):

- mmap'd file, `fdatasync`/`msync`, WAL, file lock, `Mlock`, `grow`.
- The on-disk freelist package and the node-id allocator — nodes are linked by
  direct `*node` child pointers, so there are no node ids to allocate or recycle.
- The `cmd/bbolt` CLI and the `internal/{surgeon,guts_cli,freelist,tests}` +
  `tests/*` disk tooling.
- Nested buckets — only **flat top-level** buckets are supported.
- Disk-oriented `Options` fields (`NoSync`, `MmapFlags`, `InitialMmapSize`,
  `FreelistType`, `Mlock`, `Timeout`, …) and `DB.Sync`/`grow`/`loadFreelist`.
- `Tx.Page` (removed); `Tx.Check` is retained as a no-op stub that returns no errors.

Added:

- Global in-memory publication via `dbState`; structurally-shared persistent
  B+tree (branch inodes hold `*node` child pointers, path-copy COW on write).
- BMSP snapshot format + `Open` auto-restore.
- `Tx.Cursor()` over sorted top-level bucket names.

---

## Caveats & limitations

- **Volatile by design.** A process crash/exit loses everything not captured in a
  snapshot. vmbolt is *not* a replacement for a durable store.
- **Single writer.** One write transaction at a time (global lock). Reads are
  concurrent with the writer.
- **Flat top-level buckets only.** Nested bucket APIs were removed.
- **Byte slices from `Get`/`Cursor` are valid only for the transaction's
  lifetime** (they reference shared immutable storage). Copy them if you need
  them beyond the tx.
- **`Tx.Check` is a no-op stub** (returns no errors).

---

## etcd backend notes

vmbolt's API was shaped with [etcd][etcd]'s `backend` package in mind (etcd uses
only flat top-level buckets, never nested). The pieces etcd depends on are
present:

- sorted `Tx.Cursor()` (deterministic `Hash`/snapshot),
- `Tx.WriteTo`/snapshot streaming (BMSP),
- `Tx.Size()`,
- one globally consistent transaction view, so `meta` + `key` advance together.

The fundamental gap is **durability**: etcd requires a durable, recoverable
backend, while vmbolt is memory-only (snapshot-on-demand, volatile between
snapshots). vmbolt can serve as a **non-durable / in-memory etcd** (testing,
caching, ephemeral sidecars), not as a drop-in for production etcd storage.

[etcd]: https://github.com/etcd-io/etcd
