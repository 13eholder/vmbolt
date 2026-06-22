# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`vmbolt` (module `13eholder/vmbolt`, package `vmbolt`) is a **pure-memory** key/value
store forked from [bbolt](https://github.com/etcd-io/bbolt). The disk stack (mmap file,
WAL, fsync, file lock, freelist package, `cmd/bbolt` CLI, `tests/*`) has been removed; the
engine is an in-memory, per-bucket B+tree that keeps bbolt's `DB`/`Tx`/`Bucket`/`Cursor`
API shape. The README is authoritative for the data model and API — read it first.

## Commands

Go toolchain is pinned via `.go-version` (1.23.12); `go.mod` declares `go 1.23`.

```sh
make fmt                       # gofmt + goimports check (CI gate; fix with ./scripts/fix.sh)
make lint                      # golangci-lint run ./...  (CI pins v1.64.8)
make test                      # go test -v ./...
make coverage                  # go test -coverprofile cover.out ./...
make test-benchmark-compare REF=<sha>   # benchstat-compare current vs REF (benchmarks live in *_bench_test.go)
```

Test env knobs (consumed by the `Makefile`):

- `CPU=N` → `-cpu=N`
- `ENABLE_RACE=true` → `-race=true` (default off)
- `TIMEOUT=20m` → `-timeout`
- `TEST_ENABLE_STRICT_MODE` → `internal/btesting` enables strict checks after opening each DB

Run a single test directly (bypass the Makefile):

```sh
go test -run TestSplitKeepsAllKeys ./
CPU=4 ENABLE_RACE=true go test -run TestName -v ./
```

There is no `cmd/bbolt` and no on-disk CLI — `make build`/`clean` were removed. CI runs
only Linux amd64 (`.github/workflows/tests_amd64.yaml` + `tests-template.yml`); windows,
cross-arch, robustness, benchmark, and failpoint workflows were intentionally deleted.

## Architecture

The core idea: **each top-level bucket is an independently, atomically published B+tree**,
and there is no shared on-disk page layer.

- **Per-bucket publish** — `DB.buckets` is a `map[string]*bucketHandle`. Each
  `bucketHandle` owns an `atomic.Pointer[bucketState]`; readers dereference it lock-free.
  `bucketState` (`bucket_state.go`) is the immutable published generation:
  `{id, root Nid, nodes map[Nid]*snapNode}`. A commit rebuilds and republishes
  *only the buckets the tx touched*.
- **Two node shapes** (`snapshot_node.go`) — `snapNode` is immutable and shared across
  transactions; `workNode` is tx-local and mutable. Copy-on-write flows through
  `snapNode.materializeWorkNode()` (read→write) and `workNode.freeze()` (write→published,
  zero-copy inode-slice transfer). Both shell types are recycled via `sync.Pool`
  (`workNodePool`, `inodesPool`); inode/key/value bytes are never retained across reuse.
- **Nid layout** (`internal/common/nid.go`) — a node id is 64 bits: high 16 = `BucketId`
  (≤65535, recycled on bucket delete; 0 reserved), low 48 = bucket-local `NodeId`
  (recycled LIFO per bucket via `bucketHandle.freeNodeIds`). This makes every Nid globally
  unique and self-routing (`Nid.BucketOf()`).
- **Single writer, lazy read snapshot** — one global write lock (`DB.rwlock`); commit is
  build-all-then-publish-all so a failed commit publishes nothing. A read tx pins each
  bucket's generation **on first access, not at `Begin`**, so a single read tx may observe
  different buckets at different generations. This is intentional.
- **Cohorts** (`bucket_state.go`, `cohort.go`) — buckets that must be observed jointly
  (e.g. etcd's `meta`+`key`) share one `atomic.Pointer[cohortSnapshot]`.
  `publishedStateOf()` routes a cohort member's reads through the cohort snapshot instead
  of its own pointer.
- **BMSP snapshots** (`snapshot.go`) — non-durable by design, but `Tx.WriteTo`/`CopyFile`
  and `DB.Restore` serialize/restore the whole DB as a deterministic streaming KV dump
  (buckets sorted by name via `Tx.Cursor`, keys sorted within each bucket). `Open(path)`
  auto-restores if `path` points at a BMSP file; otherwise the path is ignored for storage.
  Magic `"BMSP"`, version 1, little-endian.

## Key packages

- `.` — `DB`, `Tx`, `Bucket`, `Cursor`, `node`, `snapNode`/`workNode`, `bucketState`/`bucketHandle`,
  `bucketCohort`, BMSP serialize/restore.
- `internal/common` — shared low-level types only: `Nid`, `BucketId`, `Inode`/`Inodes`,
  `Txid`, defaults. (No page/mmap code — that was deleted upstream.)
- `internal/btesting` — test `DB` helpers (`MustCreateDB`, `MustReopen`, cleanup); wires
  `TEST_ENABLE_STRICT_MODE`.
- `errors` — sentinel errors (`ErrBucketNotFound`, `ErrTxClosed`, `ErrNestedBucketsUnsupported`, …).
- `version` — version metadata.

## Fork-specific gotchas

- **Stale upstream text.** This is a bbolt fork and much inherited prose/comments no longer
  match reality. `doc.go` still describes an mmap'd single-file store; `errors/errors.go`
  still carries disk-era sentinels (`ErrInvalidMapping`, `ErrChecksum`,
  `ErrVersionMismatch`, `ErrFreePagesNotLoaded`, …) retained only for API compatibility.
  Trust the README and `bucket_state.go`/`snapshot*.go`/`nid.go` comments over legacy text.
- **Flat buckets only.** No nested buckets — returns `ErrNestedBucketsUnsupported`.
  `Tx.Check` is a no-op stub.
- **Lint import ordering may be stale.** `.golangci.yaml` sets the local import prefix to
  `go.etcd.io` (gci section + goimports `local-prefixes`), but the module is now
  `13eholder/vmbolt`. `13eholder/vmbolt` imports therefore sort as third-party under lint.
  Fix the prefix in `.golangci.yaml` if you want them treated as local.
- **Byte slices from `Get`/`Cursor` reference shared immutable storage** and are valid only
  for the transaction's lifetime — copy them to escape the tx.
