# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`vmbolt` (module `github.com/13eholder/vmbolt`, package `vmbolt`) is a **pure-memory** key/value
store forked from [bbolt](https://github.com/etcd-io/bbolt). The disk stack (mmap file,
WAL, fsync, file lock, freelist package, `cmd/bbolt` CLI, `tests/*`) has been removed; the
engine is an in-memory flat-bucket B+tree store with globally consistent transaction views that keeps bbolt's `DB`/`Tx`/`Bucket`/`Cursor`
API shape. The README is authoritative for the data model and API — read it first.

## Commands

Go toolchain is pinned via `.go-version` (1.23.12); `go.mod` declares `go 1.23`.

```sh
make fmt                       # gofmt + goimports check (CI gate; fix with ./scripts/fix.sh)
make lint                      # golangci-lint run ./...  (CI pins v2.0.2; config is v2 schema)
make test                      # go test -v ./...
make coverage                  # go test -coverprofile cover.out ./...
make test-benchmark-compare REF=<sha>   # benchstat-compare current vs REF (benchmarks live in *_bench_test.go)
```

Test env knobs (consumed by the `Makefile`):

- `CPU=N` → `-cpu=N`
- `ENABLE_RACE=true` → `-race=true` (default off)
- `TIMEOUT=20m` → `-timeout`
- `TEST_ENABLE_STRICT_MODE` → accepted by `internal/btesting` but a no-op (strict post-commit checks are not applicable to the pure-memory engine)

Run a single test directly (bypass the Makefile):

```sh
go test -run TestSplitKeepsAllKeys ./
CPU=4 ENABLE_RACE=true go test -run TestName -v ./
```

There is no `cmd/bbolt` and no on-disk CLI — `make build`/`clean` were removed. CI runs
only Linux amd64 (`.github/workflows/tests_amd64.yaml` + `tests-template.yml`); windows,
cross-arch, robustness, benchmark, and failpoint workflows were intentionally deleted.

## Architecture

The core idea: there is **one globally published immutable dbState per commit**,
and there is no shared on-disk page layer.

- **Global publish** — `DB.state` is an `atomic.Pointer[dbState]`. `dbState`
  holds the top-level bucket directory plus the global nid allocator snapshot.
  Readers pin one `dbState` at `Begin`, so one read tx sees one consistent view
  across all buckets.
- **Two node shapes** (`snapshot_node.go`) — `snapNode` is immutable and shared across
  transactions; `workNode` is tx-local and mutable. Copy-on-write flows through
  `snapNode.materializeWorkNode()` (read→write) and `workNode.freeze()` (write→published,
  zero-copy inode-slice transfer). Both shell types are recycled via `sync.Pool`
  (`workNodePool`, `inodesPool`); inode/key/value bytes are never retained across reuse.
- **Global Nid allocator** — `Nid` (defined in `internal/common/types.go`) is a globally
  unique uint64. The allocator state (`nextNid`/`freeNid`) lives in `dbState`
  (`bucket_state.go`) and is snapshotted into write txs (`tx.go`); freed ids are recycled LIFO.
- **Single writer, global read snapshot** — one global write lock (`DB.rwlock`); commit is
  build-all-then-publish so a failed commit publishes nothing. A read tx pins the
  current `dbState` at `Begin`.
- **BMSP snapshots** (`snapshot.go`) — non-durable by design, but `Tx.WriteTo`/`CopyFile`
  and `DB.Restore` serialize/restore the whole DB as a deterministic streaming KV dump
  (buckets sorted by name via `Tx.Cursor`, keys sorted within each bucket). `Open(path)`
  auto-restores if `path` points at a BMSP file; otherwise the path is ignored for storage.
  Magic `"BMSP"`, version 1, little-endian.

## Key packages

- `.` — `DB`, `Tx`, `Bucket`, `Cursor`, `node`, `snapNode`/`workNode`, `bucketState`,
  `dbState`, BMSP serialize/restore.
- `internal/common` — shared low-level types only: `Nid`, `Inode`/`Inodes`,
  defaults. (No page/mmap code — that was deleted upstream.)
- `internal/btesting` — test `DB` helpers (`MustCreateDB`, `MustReopen`, cleanup); wires
  `TEST_ENABLE_STRICT_MODE`.
- `errors` — sentinel errors (`ErrBucketNotFound`, `ErrTxClosed`, …).
- `version` — version metadata.

## Fork-specific gotchas

- **Stale upstream text.** This is a bbolt fork and much inherited prose/comments no longer
  match reality. `doc.go` still describes an mmap'd single-file store; `errors/errors.go`
  still carries disk-era sentinels (`ErrInvalidMapping`, `ErrChecksum`,
  `ErrVersionMismatch`, `ErrMaxSizeReached`, …) retained only for API compatibility.
  Trust the README and `bucket_state.go`/`snapshot*.go`/`nid.go` comments over legacy text.
- **Flat buckets only.** No nested buckets.
- **`Tx.Check` is implemented** (`tx_check.go`): validates the immutable node graph
  (reachability from root, key ordering, branch separator invariant, leaf/branch shape)
  and the global nid allocator invariants. Concurrent calls are safe (snapshot is immutable).
- **Lint needs the pinned toolchain.** `golangci-lint` is built with an older Go than a
  newer system Go may provide, so `make lint` runs it under `GOTOOLCHAIN=go$(cat .go-version)`.
  The local import prefix in `.golangci.yaml` is `github.com/13eholder/vmbolt`.
- **Byte slices from `Get`/`Cursor` reference shared immutable storage** and are valid only
  for the transaction's lifetime — copy them to escape the tx.
