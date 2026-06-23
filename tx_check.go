package vmbolt

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
)

// Check performs structural consistency validation of the committed database
// snapshot pinned by this transaction (tx.base). It validates the immutable
// in-memory node graph directly via child pointers (no flat index).
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

		// Level A: dbState consistency (serial).
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
// Level A — dbState consistency

// checkDBState validates the global, cross-bucket invariants of a published
// dbState: every published bucket state is non-nil.
func checkDBState(s *dbState) []error {
	if s == nil {
		return []error{fmt.Errorf("vmbolt: check: nil dbState")}
	}
	var errs []error
	for name, bs := range s.buckets {
		if bs == nil {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q has nil state", name))
		}
	}
	return errs
}

// ===========================================================================================
// Level B — per-bucket B+tree
//
// checkBucket validates one bucket's published node tree: root presence,
// reachability from root (no nil/dangling child, no cycle / shared child within
// this single tree), strictly ascending keys per node, leaf/branch inode shape
// consistency, and the branch separator invariant (parent separator == child's
// first key). Cross-generation subtree sharing is legitimate; the visited set
// is therefore keyed by pointer identity and only flags a repeat within a
// single tree walk.
func checkBucket(name string, bs *bucketState, cfg *checkConfig) []error {
	if bs == nil {
		return []error{fmt.Errorf("vmbolt: check: bucket %q has nil state", name)}
	}
	var errs []error
	ks := cfg.kvStringer
	if bs.root == nil {
		return errs // empty bucket
	}

	visited := make(map[*node]bool)
	stack := []*node{bs.root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == nil {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q encountered nil node during walk", name))
			continue
		}
		if visited[n] {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node %p reachable via multiple parents (cycle or shared child)", name, n))
			continue
		}
		visited[n] = true

		errs = append(errs, checkNode(name, n, ks)...)

		if !n.isLeaf {
			for i := range n.inodes {
				child := n.inodes[i].child
				if child == nil {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch node %p inode %d has nil child", name, n, i))
					continue
				}
				if len(child.inodes) == 0 {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q child %p has no inodes (parent %p, sep %q)", name, child, n, ks.KeyToString(n.inodes[i].key)))
					continue
				}
				if !bytes.Equal(n.inodes[i].key, child.inodes[0].key) {
					errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch separator %q != child %p first key %q",
						name, ks.KeyToString(n.inodes[i].key), child, ks.KeyToString(child.inodes[0].key)))
				}
				stack = append(stack, child)
			}
		}
	}

	return errs
}

// checkNode validates intra-node invariants: strictly ascending, non-empty
// keys and leaf/branch inode shape consistency.
func checkNode(name string, n *node, ks KVStringer) []error {
	var errs []error
	var prev []byte
	for i := range n.inodes {
		in := &n.inodes[i]
		if len(in.key) == 0 {
			errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node %p has empty key at index %d", name, n, i))
		}
		if i > 0 {
			if bytes.Compare(in.key, prev) <= 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q node %p keys not strictly ascending at index %d (%q <= %q)",
					name, n, i, ks.KeyToString(prev), ks.KeyToString(in.key)))
			}
		}
		prev = in.key
		// Shape consistency.
		if n.isLeaf {
			if in.child != nil {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q leaf node %p inode %d has a non-nil child", name, n, i))
			}
		} else {
			if in.child == nil {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch node %p inode %d has nil child", name, n, i))
			}
			if len(in.value) != 0 {
				errs = append(errs, fmt.Errorf("vmbolt: check: bucket %q branch node %p inode %d carries a value", name, n, i))
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
