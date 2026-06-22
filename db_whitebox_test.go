package vmbolt

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	berrors "github.com/13eholder/vmbolt/errors"
)

func TestBeginTx_StateUnavailable(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0666, nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// After close the db is not open; beginTx must reject it.
	tx, err := db.beginTx()
	require.Nil(t, tx)
	require.ErrorIs(t, err, berrors.ErrDatabaseNotOpen)
}
