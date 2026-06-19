//go:build linux

package robustness

import (
	"flag"
	"os"
	"testing"

	testutils "13eholder/vmbolt/tests/utils"
)

func TestMain(m *testing.M) {
	flag.Parse()
	testutils.RequiresRoot()
	os.Exit(m.Run())
}
