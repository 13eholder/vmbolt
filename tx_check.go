package vmbolt

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"13eholder/vmbolt/internal/common"
)

// Check performs structural consistency validation of the committed database
// snapshot pinned by this transaction (tx.base). It is the page-less analogue
// of bbolt's tx.Check: instead of validating physical pages it validates the
// immutable in-memory node graph.
//
// Every detected inconsistency is streamed on the returned channel; the
// channel is closed when validation completes. A well-formed database yields
// no errors.
//
// Concurrent calls on the same transaction are safe: the published snapshot
// is immutable, so each Check call walks it independently without locks.
//
// Options: WithKVStringer customizes key/value rendering in error messages;
// WithPageId is accepted for API compatibility but is a no-op (the engine is
// page-less).
//
// Scope: only the committed snapshot the transaction pinned is validated.
// Uncommitted, in-flight write-transaction mutations are NOT validated.
func (tx *Tx) Check(options ...CheckOption) <-chan error {
	cfg := newCheckConfig(options)

	ch := make(chan error, 16)
	go func() {
		defer close(ch)

		if tx.base == nil {
			ch <- fmt.Errorf("vmbolt: check: transaction has no pinned snapshot (closed?)")
			return
		}

		// Level A: dbState / global nid allocator (serial).
		for _, err := range checkDBState(tx.base) {
			ch <- err
		}

		// Level B: per-bucket B+tree validation (concurrent; snapshot is immutable).
		var wg sync.WaitGroup
		for _, name := range sortedBucketNames(tx.base.buckets) {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				for _, err := range checkBucket(name, tx.base.buckets[name], cfg) {
					ch <- err
				}
			}(name)
		}
		wg.Wait()
	}()
	return ch
}

func sortedBucketNames(buckets map[string]*bucketState) []string {
	names := make([]string, 0, len(buckets))
	for n := range buckets {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ===========================================================================================
// Level A — dbState / global nid allocator
//
// checkDBState validates the global, cross-bucket invariants of a published
// dbState: bucket presence, node-key/self-nid agreement, the nextNid
// high-water mark, and freelist disjointness from live ids plus global nid
// uniqueness.
func checkDBState(s *dbState) []error {
	if s == nil {
		return []error{fmt.Errorf("vmbolt: check: nil dbState")}
	}
	var errs []error

	// Collect live nids across all buckets; assert global uniqueness and that
	// the map key matches each node's self-nid.
	live := make(map[common.Nid]string, estimateLiveNids(s))
	for name, bs := range s.buckets {
		if bs == nil {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q has nil state", name))
			continue
		}
		for nid, n := range bs.nodes {
			if n == nil {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node nid %d is nil", name, nid))
				continue
			}
			if n.nid != nid {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node keyed by nid %d but self.nid=%d", name, nid, n.nid))
			}
			if owner, dup := live[nid]; dup {
				errs = append(errs, fmt.Errorf("vmbolt: check: nid %d owned by two buckets: %q and %q", nid, owner, name))
			} else {
				live[nid] = name
			}
		}
	}

	// nextNid is the high-water mark: every live id must be 0 < id < nextNid.
	for nid, owner := range live {
		if nid == 0 {
			errs = append(errs, fmt.Errorf("vmbolt: check: reserved nid 0 is live in bucket %q", owner))
		}
		if uint64(nid) >= s.nextNid {
			errs = append(errs, fmt.Errorf("vmbolt: check: live nid %d >= nextNid %d (bucket %q)", nid, s.nextNid, owner))
		}
	}

	// Freelist: entries must be below nextNid, unique, and disjoint from live ids.
	seen := make(map[common.Nid]struct{}, len(s.freeNid))
	for _, nid := range s.freeNid {
		if nid == 0 {
			errs = append(errs, fmt.Errorf("vmbolt: check: reserved nid 0 present in freelist"))
			continue
		}
		if uint64(nid) >= s.nextNid {
			errs = append(errs, fmt.Errorf("vmbolt: check: free nid %d >= nextNid %d", nid, s.nextNid))
		}
		if _, dup := seen[nid]; dup {
			errs = append(errs, fmt.Errorf("vmbolt: check: duplicate free nid %d", nid))
		} else {
			seen[nid] = struct{}{}
		}
		if owner, isLive := live[nid]; isLive {
			errs = append(errs, fmt.Errorf("vmbolt: check: nid %d is both free and live (bucket %q)", nid, owner))
		}
	}

	return errs
}

func estimateLiveNids(s *dbState) int {
	n := 0
	for _, bs := range s.buckets {
		if bs != nil {
			n += len(bs.nodes)
		}
	}
	return n
}

// ===========================================================================================
// Level B — per-bucket B+tree
//
// checkBucket validates one bucket's immutable snapNode graph:
//   - root presence (root 0 == empty bucket with no nodes),
//   - reachability from root (no dangling child pointers, no cycles / shared
//     children),
//   - no orphan nodes (every node reachable),
//   - strictly ascending keys per node,
//   - leaf/branch inode shape consistency,
//   - the branch separator invariant (parent separator == child's first key).
func checkBucket(name string, bs *bucketState, cfg *checkConfig) []error {
	if bs == nil {
		return []error{fmt.Errorf("vmbolt: check: bucket %q has nil state", name)}
	}
	var errs []error
	ks := cfg.kvStringer

	// Empty bucket: root 0, no nodes.
	if bs.root == 0 {
		if len(bs.nodes) != 0 {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q has root 0 but %d orphan nodes", name, len(bs.nodes)))
		}
		return errs
	}

	root, ok := bs.nodes[bs.root]
	if !ok || root == nil {
		return append(errs, fmt.Errorf("vmbolt: check: bucket %q root nid %d not in nodes map", name, bs.root))
	}

	// Iterative reachability walk from root.
	visited := make(map[common.Nid]bool, len(bs.nodes))
	stack := []*snapNode{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == nil {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q encountered nil node during walk", name))
			continue
		}
		if visited[n.nid] {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node nid %d reachable via multiple parents (cycle or shared child)", name, n.nid))
			continue
		}
		visited[n.nid] = true

		// Intra-node invariants.
		errs = append(errs, checkNode(name, n, ks)...)

		// Branch: validate each child pointer + separator invariant.
		if !n.isLeaf {
			for i := range n.inodes {
				inode := &n.inodes[i]
				cid := inode.Nid()
				child, ok := bs.nodes[cid]
				if !ok || child == nil {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch nid %d points to missing child nid %d", name, n.nid, cid))
					continue
				}
				if len(child.inodes) == 0 {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q child nid %d has no inodes (parent nid %d, sep %q)", name, cid, n.nid, ks.KeyToString(inode.Key())))
					visited[cid] = true // not pushed; suppress a duplicate orphan report
					continue
				}
				// Separator invariant: the parent entry key must equal the child's first key.
				if !bytes.Equal(inode.Key(), child.inodes[0].Key()) {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch nid %d separator %q != child nid %d first key %q",
						name, n.nid, ks.KeyToString(inode.Key()), cid, ks.KeyToString(child.inodes[0].Key())))
				}
				stack = append(stack, child)
			}
		}
	}

	// Orphan check: every node must be reachable from root.
	for nid := range bs.nodes {
		if !visited[nid] {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node nid %d is unreachable from root (orphan)", name, nid))
		}
	}

	return errs
}

// checkNode validates intra-node invariants: strictly ascending, non-empty
// keys and leaf/branch inode shape consistency. An empty node is allowed here
// (the caller reports empty non-root children).
func checkNode(name string, n *snapNode, ks KVStringer) []error {
	var errs []error
	if len(n.inodes) == 0 {
		return errs
	}
	prev := n.inodes[0].Key()
	for i := 0; i < len(n.inodes); i++ {
		inode := &n.inodes[i]
		if len(inode.Key()) == 0 {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node nid %d has empty key at index %d", name, n.nid, i))
		}
		if i > 0 {
			if bytes.Compare(inode.Key(), prev) <= 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node nid %d keys not strictly ascending at index %d (%q <= %q)",
					name, n.nid, i, ks.KeyToString(prev), ks.KeyToString(inode.Key())))
			}
			prev = inode.Key()
		}
		// Shape consistency.
		if n.isLeaf {
			if inode.Nid() != 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q leaf node nid %d inode %d has non-zero child nid %d", name, n.nid, i, inode.Nid()))
			}
		} else {
			// Branch inode: must carry a child pointer and no value.
			if inode.Nid() == 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch node nid %d inode %d has zero child nid", name, n.nid, i))
			}
			if len(inode.Value()) != 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch node nid %d inode %d carries a value", name, n.nid, i))
			}
		}
	}
	return errs
}

// ===========================================================================================
// Public diagnostic options/stringers are preserved for API compatibility.

type checkConfig struct {
	kvStringer KVStringer
	pageId     uint64
}

// CheckOption configures Tx.Check.
type CheckOption func(options *checkConfig)

func newCheckConfig(opts []CheckOption) *checkConfig {
	c := &checkConfig{kvStringer: HexKVStringer()}
	for _, o := range opts {
		o(c)
	}
	if c.kvStringer == nil {
		c.kvStringer = HexKVStringer()
	}
	return c
}

// WithKVStringer sets a human-readable renderer for keys/values in error
// messages.
func WithKVStringer(kvStringer KVStringer) CheckOption {
	return func(c *checkConfig) {
		c.kvStringer = kvStringer
	}
}

// WithPageId is retained for API compatibility; it is a no-op in the page-less
// engine.
func WithPageId(pageId uint64) CheckOption {
	return func(c *checkConfig) {
		c.pageId = pageId
	}
}

// KVStringer renders keys/values for human-readable diagnostic messages.
type KVStringer interface {
	KeyToString([]byte) string
	ValueToString([]byte) string
}

// HexKVStringer serializes both key & value to hex representation.
func HexKVStringer() KVStringer {
	return hexKvStringer{}
}

type hexKvStringer struct{}

func (hexKvStringer) KeyToString(key []byte) string {
	return hex.EncodeToString(key)
}

func (hexKvStringer) ValueToString(value []byte) string {
	return hex.EncodeToString(value)
}
