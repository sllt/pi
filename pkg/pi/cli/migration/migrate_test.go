package migration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigratePreservesRegistryAndCWD(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	require.NoError(t, os.Mkdir("migrations", 0755))
	original := `package migrations
import "github.com/sllt/pi/pkg/pi/migration"
// Keep application comments.
func All() map[int64]migration.Migrate { return map[int64]migration.Migrate{20200101000000: originalMigration()} }
`
	require.NoError(t, os.WriteFile("migrations/all.go", []byte(original), 0644))
	_, err := Migrate("AddOrders")
	require.NoError(t, err)
	wd, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, dir, wd)
	data, err := os.ReadFile("migrations/all.go")
	require.NoError(t, err)
	require.Contains(t, string(data), "originalMigration()")
	require.Contains(t, string(data), "AddOrders()")
	require.Contains(t, string(data), "Keep application comments")
	_, err = Migrate("invalid-name")
	require.Error(t, err)
	_, err = Migrate("AddOrders")
	require.Error(t, err)
	files, err := filepath.Glob("migrations/*AddOrders.go")
	require.NoError(t, err)
	require.Len(t, files, 1)
}
