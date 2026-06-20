package common

import (
	"os"
	"time"
)

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
)

// DefaultPageSize is the default page size used only for statistics/size
// estimates (the engine is page-less).
var DefaultPageSize = os.Getpagesize()

// Txid represents the internal transaction identifier.
type Txid uint64
