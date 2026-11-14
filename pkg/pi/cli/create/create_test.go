package create

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllWithOptionsDryRunWriteAndPreserve(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	require.NoError(t, os.WriteFile("go.mod", []byte("module example.com/demo\n\ngo 1.27.0\n"), 0644))

	expectedPaths := []string{
		filepath.Join("api", "v1", "user_profile.go"),
		filepath.Join("internal", "handler", "user_profile.go"),
		filepath.Join("internal", "router", "user_profile.go"),
		filepath.Join("internal", "service", "user_profile.go"),
		filepath.Join("internal", "repository", "user_profile.go"),
		filepath.Join("internal", "model", "user_profile.go"),
	}

	// 1. Dry-run must preview all creations without writing anything to disk.
	dryOut, err := AllWithOptions("user_profile", Options{DryRun: true})
	require.NoError(t, err)
	for _, p := range expectedPaths {
		assert.Contains(t, dryOut, "Would create: "+p)
		_, statErr := os.Stat(p)
		assert.True(t, os.IsNotExist(statErr), "dry-run must not write %s", p)
	}
	_, apiErr := os.Stat("api")
	assert.True(t, os.IsNotExist(apiErr), "dry-run must not create api directory")
	_, internalErr := os.Stat("internal")
	assert.True(t, os.IsNotExist(internalErr), "dry-run must not create internal directory")

	// 2. Real generation writes formatted, AST-valid Go source files.
	out, err := AllWithOptions("user_profile", Options{DryRun: false})
	require.NoError(t, err)
	for _, p := range expectedPaths {
		assert.Contains(t, out, "Created: "+p)

		src, readErr := os.ReadFile(p)
		require.NoError(t, readErr)
		fset := token.NewFileSet()
		_, parseErr := parser.ParseFile(fset, p, src, parser.AllErrors)
		require.NoError(t, parseErr, "generated file %s must parse cleanly", p)
	}

	dtoSrc, err := os.ReadFile(expectedPaths[0])
	require.NoError(t, err)
	assert.Contains(t, string(dtoSrc), "pi.Meta")
	assert.Contains(t, string(dtoSrc), "GetUserProfileReq")
	assert.Contains(t, string(dtoSrc), "GetUserProfileRes")
	assert.Contains(t, string(dtoSrc), "CreateUserProfileReq")
	assert.Contains(t, string(dtoSrc), "CreateUserProfileRes")

	handlerSrc, err := os.ReadFile(expectedPaths[1])
	require.NoError(t, err)
	assert.NotContains(t, string(handlerSrc), "@Summary")
	assert.NotContains(t, string(handlerSrc), "@Router")
	assert.Contains(t, string(handlerSrc), "func (h *UserProfileHandler) GetUserProfile(ctx *pi.Context, req *v1.GetUserProfileReq) (*v1.GetUserProfileRes, error)")
	assert.Contains(t, string(handlerSrc), "func (h *UserProfileHandler) CreateUserProfile(ctx *pi.Context, _ *v1.CreateUserProfileReq) (*v1.CreateUserProfileRes, error)")
	assert.Contains(t, string(handlerSrc), "apperror.New(apperror.NotFound, http.StatusNotFound")

	routerSrc, err := os.ReadFile(expectedPaths[2])
	require.NoError(t, err)
	assert.Contains(t, string(routerSrc), "InitUserProfileRouter(deps RouterDeps, h *handler.UserProfileHandler)")
	assert.Contains(t, string(routerSrc), `deps.App.Group("/api/v1").Group("/user_profile")`)
	assert.Contains(t, string(routerSrc), "pi.Register(group, h.GetUserProfile)")
	assert.Contains(t, string(routerSrc), "pi.Register(group, h.CreateUserProfile)")

	// 3. Re-running must preserve handwritten edits in both dry-run and write modes.
	handwritten := "// handwritten business logic\npackage handler\n"
	require.NoError(t, os.WriteFile(expectedPaths[1], []byte(handwritten), 0644))

	dryAgain, err := AllWithOptions("user_profile", Options{DryRun: true})
	require.NoError(t, err)
	for _, p := range expectedPaths {
		assert.Contains(t, dryAgain, "Would preserve: "+p)
	}

	writeAgain, err := AllWithOptions("user_profile", Options{DryRun: false})
	require.NoError(t, err)
	for _, p := range expectedPaths {
		assert.Contains(t, writeAgain, "Preserved: "+p)
	}

	after, err := os.ReadFile(expectedPaths[1])
	require.NoError(t, err)
	assert.Equal(t, handwritten, string(after))
}

func TestSingleComponentHelpersAndErrors(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	_, err := DTO("order")
	require.ErrorIs(t, err, ErrNoProjectName)

	require.NoError(t, os.WriteFile("go.mod", []byte("module example.com/shop\n\ngo 1.27.0\n"), 0644))

	dtoOut, err := DTO("order")
	require.NoError(t, err)
	assert.Equal(t, "Created: "+filepath.Join("api", "v1", "order.go"), dtoOut)

	routerDry, err := CreateComponentWithOptions("order", "router", Options{DryRun: true})
	require.NoError(t, err)
	assert.Equal(t, "Would create: "+filepath.Join("internal", "router", "order.go"), routerDry)
	_, statErr := os.Stat(filepath.Join("internal", "router", "order.go"))
	assert.True(t, os.IsNotExist(statErr))

	routerOut, err := Router("order")
	require.NoError(t, err)
	assert.Equal(t, "Created: "+filepath.Join("internal", "router", "order.go"), routerOut)

	_, err = CreateComponent("", "dto")
	require.ErrorIs(t, err, ErrNameEmpty)

	_, err = CreateComponent("../escape", "dto")
	require.Error(t, err)

	_, err = CreateComponent("order", "unknown")
	require.ErrorIs(t, err, ErrInvalidType)
}
