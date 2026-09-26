package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/stretchr/testify/require"
)

func TestSnapshotPrecedenceAndNoEnvironmentMutation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PI_SNAPSHOT_TEST", "unchanged")
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".env"), []byte("APP_ENV=test\nAPP_NAME=base\nPI_SNAPSHOT_TEST=file\nJWT_SECRET=secret\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".test.env"), []byte("APP_NAME=override\n"), 0644))
	s, err := config.LoadSnapshot(dir, []string{"APP_NAME=explicit"})
	require.NoError(t, err)
	require.Equal(t, "explicit", s.Get("APP_NAME"))
	require.Equal(t, "unchanged", os.Getenv("PI_SNAPSHOT_TEST"))
	m := s.Values()
	m["APP_NAME"] = "mutated"
	require.Equal(t, "explicit", s.Get("APP_NAME"))
	require.Equal(t, "[redacted]", s.Redacted()["JWT_SECRET"])
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".test.env"), []byte("oops broken @ value"), 0644))
	_, err = config.LoadSnapshot(dir, nil)
	require.Error(t, err)
}
