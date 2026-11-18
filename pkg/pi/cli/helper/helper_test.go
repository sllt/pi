package helper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestGetProjectName(t *testing.T) {
	t.Run("ignores blank lines and comments including inline comments", func(t *testing.T) {
		dir := t.TempDir()
		goMod := `
// generated file
// module example.com/commented-out

   module   example.com/my-service   // inline comment

go 1.27.0
`
		require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0644))

		out := testutil.StdoutOutputForFunc(func() {
			assert.Equal(t, "example.com/my-service", GetProjectName(dir))
		})
		assert.Empty(t, out)
	})

	t.Run("missing or invalid go.mod returns empty without stdout pollution", func(t *testing.T) {
		missingDir := t.TempDir()
		out := testutil.StdoutOutputForFunc(func() {
			assert.Equal(t, "", GetProjectName(missingDir))
		})
		assert.Empty(t, out)

		emptyDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(emptyDir, "go.mod"), []byte("// no module\n"), 0644))
		out2 := testutil.StdoutOutputForFunc(func() {
			assert.Equal(t, "", GetProjectName(emptyDir))
		})
		assert.Empty(t, out2)
	})
}

func TestCaseConversions(t *testing.T) {
	assert.Equal(t, "order_id", ToSnakeCase("OrderID"))
	assert.Equal(t, "http_server", ToSnakeCase("HTTPServer"))
	assert.Equal(t, "user_profile", ToSnakeCase("UserProfile"))
	assert.Equal(t, "user_profile", ToSnakeCase("user_profile"))
	assert.Equal(t, "order_item", ToSnakeCase("OrderItem"))
	assert.Equal(t, "api_key", ToSnakeCase("APIKey"))

	assert.Equal(t, "UserProfile", ToCamelCase("user_profile"))
	assert.Equal(t, "OrderItem", ToCamelCase("order-item"))
	assert.Equal(t, "userProfile", ToLowerFirst("UserProfile"))
	assert.Equal(t, "", ToLowerFirst(""))
}
