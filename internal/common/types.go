package common

import (
	"time"
)

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
)

// Nid is the globally-unique logical node identifier.
// Nid 0 is reserved as the "unassigned" sentinel.
type Nid uint64
