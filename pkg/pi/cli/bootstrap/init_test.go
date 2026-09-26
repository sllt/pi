package bootstrap

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitPinnedAtomicOffline(t *testing.T) {
	ctx := t.Context()
	repo := t.TempDir()
	files := map[string]string{
		"go.mod":     "module github.com/sllt/pi-layout\n\ngo 1.24.0\n\nrequire github.com/sllt/pi v0.3.1\n",
		"main.go":    "package main\nfunc main() {}\n",
		"user.proto": "option go_package = \"github.com/sllt/pi-layout/user\";\n",
	}
	for n, s := range files {
		require.NoError(t, os.WriteFile(filepath.Join(repo, n), []byte(s), 0644))
	}
	for _, args := range [][]string{{"init"}, {"add", "."}, {"-c", "user.name=Pi Test", "-c", "user.email=test@example.com", "commit", "-m", "template"}, {"tag", "v-test"}} {
		_, err := command(ctx, repo, "git", args...)
		require.NoError(t, err)
	}
	dest := filepath.Join(t.TempDir(), "app")
	opts := Options{Directory: dest, Module: "example.com/company/orders", Template: repo, Ref: "v-test", Offline: true}
	require.NoError(t, CreateContext(ctx, opts))
	b, err := os.ReadFile(filepath.Join(dest, "user.proto"))
	require.NoError(t, err)
	require.Contains(t, string(b), "example.com/company/orders/user")
	b, err = os.ReadFile(filepath.Join(dest, ".pi-template.json"))
	require.NoError(t, err)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(b, &meta))
	require.Equal(t, false, meta["verified"])
	require.ErrorIs(t, CreateContext(ctx, opts), ErrProjectExists)
	opts.Directory = filepath.Join(filepath.Dir(dest), "failed")
	opts.Ref = "missing"
	require.Error(t, CreateContext(ctx, opts))
	_, err = os.Stat(opts.Directory)
	require.True(t, os.IsNotExist(err))
	left, err := filepath.Glob(filepath.Join(filepath.Dir(dest), ".pi-init-*"))
	require.NoError(t, err)
	require.Empty(t, left)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	opts.Ref = "v-test"
	require.Error(t, CreateContext(canceled, opts))
	_, err = os.Stat(opts.Directory)
	require.True(t, os.IsNotExist(err))
}
