package helper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFilesProtectsAndStages(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.go")
	b := filepath.Join(dir, "b.go")
	require.Error(t, WriteFiles(map[string]File{a: {Data: []byte("package a")}, b: {Data: []byte("bad syntax")}}))
	_, err := os.Stat(a)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, os.WriteFile(a, []byte("package original\n"), 0644))
	require.Error(t, WriteFiles(map[string]File{a: {Data: []byte("package generated")}}))
	got, err := os.ReadFile(a)
	require.NoError(t, err)
	require.Equal(t, "package original\n", string(got))
	require.NoError(t, WriteFiles(map[string]File{a: {Data: []byte("package generated"), Replace: true}}))
	left, err := filepath.Glob(filepath.Join(dir, ".pi-generate-*"))
	require.NoError(t, err)
	require.Empty(t, left)
}
