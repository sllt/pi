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
		"go.mod":               "module github.com/sllt/pi-layout\n\ngo 1.24.0\n\nrequire github.com/sllt/pi v0.3.1\n",
		"main.go":              "package main\nfunc main() {}\n",
		"user.proto":           "option go_package = \"github.com/sllt/pi-layout/user\";\n",
		"configs/.env.example": "HTTP_PORT=8000\nJWT_SECRET=replace-with-a-strong-random-secret\n",
		"storage/.gitkeep":     "",
	}
	for n, s := range files {
		fullPath := filepath.Join(repo, n)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		require.NoError(t, os.WriteFile(fullPath, []byte(s), 0644))
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

	// Verify storage directory is preserved and configs/.env is generated with a random 64-hex JWT_SECRET.
	_, err = os.Stat(filepath.Join(dest, "storage", ".gitkeep"))
	require.NoError(t, err)
	exampleBytes, err := os.ReadFile(filepath.Join(dest, "configs", ".env.example"))
	require.NoError(t, err)
	require.Contains(t, string(exampleBytes), "JWT_SECRET=replace-with-a-strong-random-secret")

	envInfo, err := os.Stat(filepath.Join(dest, "configs", ".env"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), envInfo.Mode().Perm())
	envBytes, err := os.ReadFile(filepath.Join(dest, "configs", ".env"))
	require.NoError(t, err)
	require.NotContains(t, string(envBytes), "replace-with-a-strong-random-secret")
	require.Regexp(t, `(?m)^JWT_SECRET=[0-9a-f]{64}$`, string(envBytes))

	// A second project gets an independent random JWT_SECRET.
	dest2 := filepath.Join(t.TempDir(), "app2")
	require.NoError(t, CreateContext(ctx, Options{Directory: dest2, Module: "example.com/company/orders2", Template: repo, Ref: "v-test", Offline: true}))
	envBytes2, err := os.ReadFile(filepath.Join(dest2, "configs", ".env"))
	require.NoError(t, err)
	require.NotEqual(t, string(envBytes), string(envBytes2))
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
