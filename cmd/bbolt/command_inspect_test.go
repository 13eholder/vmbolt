package main_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	bolt "13eholder/vmbolt"
	main "13eholder/vmbolt/cmd/bbolt"
	"13eholder/vmbolt/internal/btesting"
)

func TestInspect(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()
	db.Close()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{
		"inspect", srcPath,
	})
	err := rootCmd.Execute()
	require.NoError(t, err)
}
