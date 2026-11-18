package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestInitSkipVerifyAndCommitHashFallback(t *testing.T) {
	ctx := t.Context()
	repo := t.TempDir()
	files := map[string]string{
		"go.mod":               "module github.com/sllt/pi-layout\n\ngo 1.24.0\n\nrequire github.com/sllt/pi v0.5.4\n",
		"main.go":              "package main\nfunc main() {}\n",
		"configs/.env.example": "HTTP_PORT=8000\nJWT_SECRET=replace-with-a-strong-random-secret\n",
		"storage/.gitkeep":     "",
	}
	for n, s := range files {
		fullPath := filepath.Join(repo, n)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		require.NoError(t, os.WriteFile(fullPath, []byte(s), 0644))
	}
	for _, args := range [][]string{
		{"init"},
		{"add", "."},
		{"-c", "user.name=Pi Test", "-c", "user.email=test@example.com", "commit", "-m", "template"},
		{"tag", "v-test"},
	} {
		_, err := command(ctx, repo, "git", args...)
		require.NoError(t, err)
	}
	commitOut, err := command(ctx, repo, "git", "rev-parse", "HEAD")
	require.NoError(t, err)
	commitSHA := strings.TrimSpace(commitOut)

	require.True(t, canTryShallowBranchClone("v-test"))
	require.True(t, canTryShallowBranchClone("main"))
	require.False(t, canTryShallowBranchClone(commitSHA))
	require.False(t, canTryShallowBranchClone(commitSHA[:6]))
	require.False(t, canTryShallowBranchClone("v-test~0"))
	require.False(t, canTryShallowBranchClone("HEAD"))
	require.False(t, canTryShallowBranchClone("refs/tags/v-test"))
	require.False(t, canTryShallowBranchClone(""))

	// a) SkipVerify: true with Offline: false skips go mod download / go build.
	var buf bytes.Buffer
	dest := filepath.Join(t.TempDir(), "app-skip")
	require.NoError(t, CreateContext(ctx, Options{
		Directory:  dest,
		Module:     "example.com/company/orders",
		Template:   repo,
		Ref:        "v-test",
		SkipVerify: true,
		Out:        &buf,
	}))
	out := buf.String()
	require.Contains(t, out, "[1/2] Fetching template")
	require.Contains(t, out, "[2/2] Rewriting module path")
	require.Contains(t, out, "build verification skipped via --skip-verify")

	metaBytes, err := os.ReadFile(filepath.Join(dest, ".pi-template.json"))
	require.NoError(t, err)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Equal(t, false, meta["verified"])
	require.Equal(t, "v-test", meta["template_ref"])
	require.Equal(t, commitSHA, meta["template_commit"])

	// b) Raw commit SHA Ref falls back to --no-checkout + checkout --detach <sha>.
	var shaBuf bytes.Buffer
	destSHA := filepath.Join(t.TempDir(), "app-sha")
	require.NoError(t, CreateContext(ctx, Options{
		Directory:  destSHA,
		Module:     "example.com/company/orders-sha",
		Template:   repo,
		Ref:        commitSHA,
		SkipVerify: true,
		Out:        &shaBuf,
	}))
	shaMetaBytes, err := os.ReadFile(filepath.Join(destSHA, ".pi-template.json"))
	require.NoError(t, err)
	var shaMeta map[string]any
	require.NoError(t, json.Unmarshal(shaMetaBytes, &shaMeta))
	require.Equal(t, false, shaMeta["verified"])
	require.Equal(t, commitSHA, shaMeta["template_ref"])
	require.Equal(t, commitSHA, shaMeta["template_commit"])

	// Non-branch/tag git rev expression triggers --branch failure and falls back cleanly.
	var fallbackBuf bytes.Buffer
	destFallback := filepath.Join(t.TempDir(), "app-fallback")
	require.NoError(t, CreateContext(ctx, Options{
		Directory:  destFallback,
		Module:     "example.com/company/orders-fallback",
		Template:   repo,
		Ref:        "v-test~0",
		SkipVerify: true,
		Out:        &fallbackBuf,
	}))
	fallbackMetaBytes, err := os.ReadFile(filepath.Join(destFallback, ".pi-template.json"))
	require.NoError(t, err)
	var fallbackMeta map[string]any
	require.NoError(t, json.Unmarshal(fallbackMetaBytes, &fallbackMeta))
	require.Equal(t, commitSHA, fallbackMeta["template_commit"])

	// c) runStep heartbeat dots and failure formatting under -race.
	var stepBuf bytes.Buffer
	err = runStepWithInterval(&stepBuf, 1, 2, "Downloading Go module dependencies", 10*time.Millisecond, func() error {
		time.Sleep(55 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	stepOut := stepBuf.String()
	require.Contains(t, stepOut, "[1/2] Downloading Go module dependencies....")
	require.Contains(t, stepOut, " done (")

	var failBuf bytes.Buffer
	err = runStepWithInterval(&failBuf, 2, 2, "Verifying project build", 10*time.Millisecond, func() error {
		time.Sleep(25 * time.Millisecond)
		return errors.New("build boom")
	})
	require.EqualError(t, err, "build boom")
	failOut := failBuf.String()
	require.True(t, strings.HasPrefix(failOut, "[2/2] Verifying project build..."))
	require.Contains(t, failOut, " failed (")
}
