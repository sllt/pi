package pi

import (
	"bytes"
	"encoding/json"
	"io"
	iofs "io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/static"
)

func newStaticTestApp(t *testing.T, opts ...Option) *App {
	t.Helper()
	baseOpts := []Option{
		WithConfig(map[string]string{
			"HTTP_ADDR":       "127.0.0.1:0",
			"HTTP_ENABLED":    "true",
			"GRPC_ENABLED":    "false",
			"METRICS_ENABLED": "false",
			"PI_TELEMETRY":    "false",
			"LOG_LEVEL":       "ERROR",
		}),
	}
	baseOpts = append(baseOpts, opts...)
	app, err := Build(baseOpts...)
	require.NoError(t, err)
	return app
}

// nonSeekableFS wraps an iofs.FS so returned files do not implement io.ReadSeeker.
type nonSeekableFS struct {
	inner iofs.FS
}

func (n nonSeekableFS) Open(name string) (iofs.File, error) {
	f, err := n.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return nonSeekableFile{inner: f}, nil
}

type nonSeekableFile struct {
	inner iofs.File
}

func (f nonSeekableFile) Stat() (iofs.FileInfo, error) { return f.inner.Stat() }
func (f nonSeekableFile) Read(p []byte) (int, error)   { return f.inner.Read(p) }
func (f nonSeekableFile) Close() error                 { return f.inner.Close() }

func TestStaticFS_SPAFallbackAndExclusions(t *testing.T) {
	distFS := fstest.MapFS{
		"index.html":        &fstest.MapFile{Data: []byte("<!doctype html><html><body>SPA</body></html>")},
		"robots.txt":        &fstest.MapFile{Data: []byte("User-agent: *\n")},
		"docs/index.html":   &fstest.MapFile{Data: []byte("<html>Docs</html>")},
		"assets/app.123.js": &fstest.MapFile{Data: []byte("console.log('app');")},
		"openapi.json":      &fstest.MapFile{Data: []byte(`{"secret":true}`)},
		"sub/openapi.json":  &fstest.MapFile{Data: []byte(`{"secret":true}`)},
		"empty-dir/keep":    &fstest.MapFile{Data: []byte("keep")},
	}

	app := newStaticTestApp(t, WithOpenAPI(OpenAPIConfig{
		Title:   "Static Test",
		Version: "1.0.0",
	}))
	app.GET("/api/v1/users", func(c *Context) (any, error) {
		return map[string]string{"user": "alice"}, nil
	})
	app.StaticFS("/", distFS,
		WithSPAFallback("index.html"),
		WithStaticExclude("/api"),
		WithStaticCacheControl("public, max-age=600"),
		WithImmutableAssets("/assets"),
	)

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	t.Run("GET API route reaches API handler", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users", http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "alice")
	})

	t.Run("GET unmatched excluded /api path returns JSON 404 instead of SPA index", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/unknown", http.NoBody))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		var payload map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
		assert.NotContains(t, rec.Body.String(), "SPA")
	})

	t.Run("GET unmatched /.well-known path is automatically excluded and returns JSON 404", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/unknown", http.NoBody))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		assert.NotContains(t, rec.Body.String(), "SPA")
	})

	t.Run("built-in /.well-known/alive still works", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/alive", http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("GET missing asset with extension returns JSON 404 instead of index.html", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/missing.js", http.NoBody))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		assert.NotContains(t, rec.Body.String(), "SPA")
	})

	t.Run("GET extensionless SPA route serves index.html with no-cache", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/posts/hello-world", http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
		assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
		assert.Contains(t, rec.Body.String(), "SPA")
	})

	t.Run("GET root and /index.html serve index.html without redirect loop and with no-cache", func(t *testing.T) {
		for _, target := range []string{"/", "/index.html"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
			assert.Equal(t, http.StatusOK, rec.Code, "target %s", target)
			assert.Empty(t, rec.Header().Get("Location"), "target %s", target)
			assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"), "target %s", target)
			assert.Contains(t, rec.Body.String(), "SPA")
		}
	})

	t.Run("GET /docs/ serves docs/index.html without redirect", func(t *testing.T) {
		for _, target := range []string{"/docs/", "/docs"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Empty(t, rec.Header().Get("Location"))
			assert.Equal(t, "<html>Docs</html>", rec.Body.String())
		}
	})

	t.Run("Cache-Control distinguishes immutable assets, normal files, and SPA index", func(t *testing.T) {
		recAsset := httptest.NewRecorder()
		h.ServeHTTP(recAsset, httptest.NewRequest(http.MethodGet, "/assets/app.123.js", http.NoBody))
		assert.Equal(t, http.StatusOK, recAsset.Code)
		assert.Equal(t, "public, max-age=31536000, immutable", recAsset.Header().Get("Cache-Control"))

		recRobots := httptest.NewRecorder()
		h.ServeHTTP(recRobots, httptest.NewRequest(http.MethodGet, "/robots.txt", http.NoBody))
		assert.Equal(t, http.StatusOK, recRobots.Code)
		assert.Equal(t, "public, max-age=600", recRobots.Header().Get("Cache-Control"))
	})

	t.Run("Blocks path traversal and openapi.json direct access", func(t *testing.T) {
		for _, badPath := range []string{
			"/../etc/passwd",
			"/assets/../robots.txt",
			"/%2e%2e/robots.txt",
			"/assets/a%5cb",
			"/assets/a%00b",
		} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, badPath, http.NoBody))
			assert.Equal(t, http.StatusNotFound, rec.Code, "path %s", badPath)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		}

		for _, restricted := range []string{"/openapi.json", "/sub/openapi.json"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, restricted, http.NoBody))
			assert.Equal(t, http.StatusForbidden, rec.Code, "restricted %s", restricted)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		}
	})

	t.Run("OpenAPI spec excludes StaticFS routes", func(t *testing.T) {
		doc, specErr := app.OpenAPISpec()
		require.NoError(t, specErr)
		assert.Empty(t, doc.Paths)
	})
}

func TestStaticFS_DirectoryWithoutSPAFallbackDoesNotList(t *testing.T) {
	fsys := fstest.MapFS{
		"docs/guide.txt": &fstest.MapFile{Data: []byte("guide")},
	}
	app := newStaticTestApp(t)
	app.StaticFS("/", fsys)

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/docs/", http.NoBody))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	assert.NotContains(t, rec.Body.String(), "guide.txt")
}

func TestStaticFS_PrecompressedAndRangeBypass(t *testing.T) {
	fsys := fstest.MapFS{
		"app.js":    &fstest.MapFile{Data: []byte("0123456789")},
		"app.js.br": &fstest.MapFile{Data: []byte("BROTLI_BYTES")},
		"app.js.gz": &fstest.MapFile{Data: []byte("GZIP_BYTES")},
	}

	app := newStaticTestApp(t)
	app.StaticFS("/", fsys)

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	t.Run("prefers br over gzip when both q > 0", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req.Header.Set("Accept-Encoding", "gzip, deflate, br")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "br", rec.Header().Get("Content-Encoding"))
		assert.Contains(t, rec.Header().Values("Vary"), "Accept-Encoding")
		assert.Contains(t, rec.Header().Get("Content-Type"), "javascript")
		assert.Equal(t, "BROTLI_BYTES", rec.Body.String())
	})

	t.Run("respects q=0 on br and falls back to gzip", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req.Header.Set("Accept-Encoding", "br;q=0, gzip;q=0.5")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
		assert.Contains(t, rec.Header().Values("Vary"), "Accept-Encoding")
		assert.Contains(t, rec.Header().Get("Content-Type"), "javascript")
		assert.Equal(t, "GZIP_BYTES", rec.Body.String())
	})

	t.Run("Range header bypasses precompressed negotiation", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req.Header.Set("Accept-Encoding", "br, gzip")
		req.Header.Set("Range", "bytes=0-3")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusPartialContent, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, "bytes 0-3/10", rec.Header().Get("Content-Range"))
		assert.Equal(t, "0123", rec.Body.String())
	})

	t.Run("WithStaticPrecompressed(false) serves uncompressed", func(t *testing.T) {
		appDisabled := newStaticTestApp(t)
		appDisabled.StaticFS("/", fsys, WithStaticPrecompressed(false))
		hd, hdErr := appDisabled.HTTPHandler()
		require.NoError(t, hdErr)

		req := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req.Header.Set("Accept-Encoding", "br, gzip")
		rec := httptest.NewRecorder()
		hd.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.Equal(t, "0123456789", rec.Body.String())
	})
}

func TestStaticFS_EmbedFS_ETagAnd304AndHead(t *testing.T) {
	app := newStaticTestApp(t)
	app.StaticFS("/embed", static.Files)

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	// First GET against real embed.FS (ModTime().IsZero() == true).
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/embed/index.html", http.NoBody))
	assert.Equal(t, http.StatusOK, rec1.Code)
	etag := rec1.Header().Get("ETag")
	require.NotEmpty(t, etag)
	assert.True(t, strings.HasPrefix(etag, `"sha256-`) && strings.HasSuffix(etag, `"`), "unexpected ETag %q", etag)
	assert.Len(t, etag, len(`"sha256-`)+16+1)
	assert.Contains(t, rec1.Header().Get("Content-Type"), "text/html")

	// Second GET with If-None-Match returns 304 Not Modified with empty body.
	req2 := httptest.NewRequest(http.MethodGet, "/embed/index.html", http.NoBody)
	req2.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusNotModified, rec2.Code)
	assert.Empty(t, rec2.Body.Bytes())
	assert.Equal(t, etag, rec2.Header().Get("ETag"))

	// HEAD returns 200 with ETag & Content-Type and zero body bytes.
	recHead := httptest.NewRecorder()
	h.ServeHTTP(recHead, httptest.NewRequest(http.MethodHead, "/embed/index.html", http.NoBody))
	assert.Equal(t, http.StatusOK, recHead.Code)
	assert.Equal(t, etag, recHead.Header().Get("ETag"))
	assert.Empty(t, recHead.Body.Bytes())
}

func TestStaticFS_NonSeekableFSFallback(t *testing.T) {
	fsys := nonSeekableFS{
		inner: fstest.MapFS{
			"hello.txt": &fstest.MapFile{Data: []byte("hello non-seekable"), ModTime: time.Time{}},
		},
	}
	app := newStaticTestApp(t)
	app.StaticFS("/", fsys)
	h, err := app.HTTPHandler()
	require.NoError(t, err)

	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/hello.txt", http.NoBody))
	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, "hello non-seekable", rec1.Body.String())
	etag := rec1.Header().Get("ETag")
	require.NotEmpty(t, etag)

	// Cached ETag + non-seekable file still supports 304 and HEAD.
	req2 := httptest.NewRequest(http.MethodGet, "/hello.txt", http.NoBody)
	req2.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusNotModified, rec2.Code)
	assert.Empty(t, rec2.Body.Bytes())

	recHead := httptest.NewRecorder()
	h.ServeHTTP(recHead, httptest.NewRequest(http.MethodHead, "/hello.txt", http.NoBody))
	assert.Equal(t, http.StatusOK, recHead.Code)
	assert.Empty(t, recHead.Body.Bytes())
}

func TestStaticFS_GroupMiddlewareInheritanceAndStart(t *testing.T) {
	distFS := fstest.MapFS{
		"index.html":   &fstest.MapFile{Data: []byte("<html>Admin</html>")},
		"assets/ui.js": &fstest.MapFile{Data: []byte("export default 1;")},
	}

	app := newStaticTestApp(t)
	ui := app.Group("/admin")
	ui.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Group-HTTP", "applied")
			next.ServeHTTP(w, r)
		})
	})
	ui.UseMiddleware(func(next Handler) Handler {
		return func(c *Context) (any, error) {
			if c.Header("X-Block") == "1" {
				return nil, apperror.New(apperror.Forbidden, http.StatusForbidden, "blocked by pi middleware")
			}
			return next(c)
		}
	})
	ui.StaticFS("/", distFS, WithSPAFallback("index.html"))

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	for _, target := range []string{"/admin", "/admin/", "/admin/users/42"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
		assert.Equal(t, http.StatusOK, rec.Code, target)
		assert.Equal(t, "applied", rec.Header().Get("X-Group-HTTP"), target)
		assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"), target)
		assert.Equal(t, "<html>Admin</html>", rec.Body.String(), target)
	}

	reqBlocked := httptest.NewRequest(http.MethodGet, "/admin/assets/ui.js", http.NoBody)
	reqBlocked.Header.Set("X-Block", "1")
	recBlocked := httptest.NewRecorder()
	h.ServeHTTP(recBlocked, reqBlocked)
	assert.Equal(t, http.StatusForbidden, recBlocked.Code)
	assert.Equal(t, "applied", recBlocked.Header().Get("X-Group-HTTP"))
	assert.Contains(t, recBlocked.Body.String(), "blocked by pi middleware")

	// Standalone StaticFS-only app activates HTTP server cleanly in Start().
	standalone := newStaticTestApp(t)
	standalone.StaticFS("/", distFS, WithSPAFallback("index.html"))
	require.NoError(t, standalone.Start(t.Context()))
	defer func() { _ = standalone.Stop(t.Context()) }()
	require.NotEmpty(t, standalone.HTTPAddress())

	resp, err := http.Get("http://" + standalone.HTTPAddress() + "/dashboard")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "<html>Admin</html>", string(body))
}

func TestStaticFS_ValidationAndRouteErrors(t *testing.T) {
	validFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<html>OK</html>")},
		"sub/file":   &fstest.MapFile{Data: []byte("x")},
	}

	cases := []struct {
		name  string
		setup func(a *App)
	}{
		{
			name: "nil iofs.FS",
			setup: func(a *App) {
				a.StaticFS("/", nil)
			},
		},
		{
			name: "nil StaticOption",
			setup: func(a *App) {
				a.StaticFS("/", validFS, nil)
			},
		},
		{
			name: "invalid prefix with wildcard or brace",
			setup: func(a *App) {
				a.StaticFS("/assets/*", validFS)
			},
		},
		{
			name: "invalid prefix with path param brace",
			setup: func(a *App) {
				a.StaticFS("/{tenant}", validFS)
			},
		},
		{
			name: "reserved /.well-known prefix",
			setup: func(a *App) {
				a.StaticFS("/.well-known", validFS)
			},
		},
		{
			name: "reserved /.well-known subpath",
			setup: func(a *App) {
				a.StaticFS("/.well-known/ui", validFS)
			},
		},
		{
			name: "missing SPA fallback index.html",
			setup: func(a *App) {
				a.StaticFS("/", validFS, WithSPAFallback("missing.html"))
			},
		},
		{
			name: "SPA fallback indexFile pointing to directory",
			setup: func(a *App) {
				a.StaticFS("/", validFS, WithSPAFallback("sub"))
			},
		},
		{
			name: "SPA fallback rejects traversal and leading slash",
			setup: func(a *App) {
				a.StaticFS("/", validFS, WithSPAFallback("../index.html"))
			},
		},
		{
			name: "SPA fallback rejects leading slash",
			setup: func(a *App) {
				a.StaticFS("/", validFS, WithSPAFallback("/index.html"))
			},
		},
		{
			name: "SPA fallback rejects backslash",
			setup: func(a *App) {
				a.StaticFS("/", validFS, WithSPAFallback(`sub\index.html`))
			},
		},
		{
			name: "duplicate mount at same normalized prefix",
			setup: func(a *App) {
				a.StaticFS("/web", validFS)
				a.Group("/web").StaticFS("/", validFS)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := newStaticTestApp(t, WithOpenAPI(OpenAPIConfig{
				Title:   "Err Test",
				Version: "1.0.0",
			}))
			tc.setup(app)

			assert.Error(t, app.RouteError())
			_, httpErr := app.HTTPHandler()
			assert.Error(t, httpErr)
			_, specErr := app.OpenAPISpec()
			assert.Error(t, specErr)
			assert.Error(t, app.Start(t.Context()))
		})
	}

	t.Run("HEAD on missing asset returns 404 JSON with zero body", func(t *testing.T) {
		app := newStaticTestApp(t)
		app.StaticFS("/", validFS, WithSPAFallback("index.html"))
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/assets/missing.js", http.NoBody))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		assert.True(t, bytes.Equal(nil, rec.Body.Bytes()))
	})

	t.Run("post-freeze StaticFS is rejected cleanly", func(t *testing.T) {
		app := newStaticTestApp(t)
		app.StaticFS("/", validFS)
		h, err := app.HTTPHandler()
		require.NoError(t, err)
		require.NotNil(t, h)

		require.NotPanics(t, func() {
			app.StaticFS("/late", validFS)
		})
	})
}

func TestStaticFS_DotfilesAndSymlinksBlocked(t *testing.T) {
	t.Run("rejects dotfiles even with SPA fallback", func(t *testing.T) {
		fsys := fstest.MapFS{
			"index.html":    &fstest.MapFile{Data: []byte("<html>SPA</html>")},
			".env":          &fstest.MapFile{Data: []byte("SECRET=123")},
			".git/HEAD":     &fstest.MapFile{Data: []byte("ref: refs/heads/main")},
			"sub/.htaccess": &fstest.MapFile{Data: []byte("deny from all")},
		}
		app := newStaticTestApp(t)
		app.StaticFS("/", fsys, WithSPAFallback("index.html"))
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		for _, target := range []string{"/.env", "/.git/HEAD", "/.git/missing", "/sub/.htaccess", "/sub/.hidden"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
			assert.Equal(t, http.StatusNotFound, rec.Code, "target %s", target)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/json", "target %s", target)
			assert.NotContains(t, rec.Body.String(), "SECRET")
			assert.NotContains(t, rec.Body.String(), "SPA")
		}
	})

	t.Run("WithSPAFallback rejects dotfile indexFile", func(t *testing.T) {
		fsys := fstest.MapFS{
			".env":            &fstest.MapFile{Data: []byte("SECRET=123")},
			"sub/.index.html": &fstest.MapFile{Data: []byte("x")},
		}
		for _, idx := range []string{".env", "sub/.index.html"} {
			app := newStaticTestApp(t)
			app.StaticFS("/", fsys, WithSPAFallback(idx))
			assert.Error(t, app.RouteError(), idx)
		}
	})

	t.Run("os.DirFS blocks symlinks along path, index, and companions", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideSecret := filepath.Join(outsideDir, "secret.txt")
		require.NoError(t, os.WriteFile(outsideSecret, []byte("OUTSIDE_SECRET"), 0o600))

		rootDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(rootDir, "index.html"), []byte("<html>OK</html>"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(rootDir, "app.js"), []byte("plain-js"), 0o600))
		require.NoError(t, os.MkdirAll(filepath.Join(rootDir, "docs"), 0o755))

		// Symlinked file, symlinked dir, symlinked docs/index.html, and symlinked .br companion.
		if err := os.Symlink(outsideSecret, filepath.Join(rootDir, "escape.txt")); err != nil {
			t.Skipf("symlinks unsupported on this filesystem: %v", err)
		}
		require.NoError(t, os.Symlink(outsideDir, filepath.Join(rootDir, "linkdir")))
		require.NoError(t, os.Symlink(outsideSecret, filepath.Join(rootDir, "docs", "index.html")))
		require.NoError(t, os.Symlink(outsideSecret, filepath.Join(rootDir, "app.js.br")))

		app := newStaticTestApp(t)
		app.StaticFS("/", os.DirFS(rootDir), WithSPAFallback("index.html"))
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		for _, target := range []string{"/escape.txt", "/linkdir/secret.txt", "/docs/", "/docs"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, http.NoBody))
			assert.NotContains(t, rec.Body.String(), "OUTSIDE_SECRET", "target %s", target)
			if target == "/escape.txt" || target == "/linkdir/secret.txt" {
				assert.Equal(t, http.StatusNotFound, rec.Code, "target %s", target)
			}
		}

		// Symlinked .br companion must be ignored; real app.js is served uncompressed without Vary.
		req := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req.Header.Set("Accept-Encoding", "br, gzip")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "plain-js", rec.Body.String())
		assert.Empty(t, rec.Header().Get("Content-Encoding"))
		assert.NotContains(t, rec.Header().Values("Vary"), "Accept-Encoding")
	})

	t.Run("os.DirFS blocks symlinked SPA indexFile", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideHTML := filepath.Join(outsideDir, "outside.html")
		require.NoError(t, os.WriteFile(outsideHTML, []byte("OUTSIDE_HTML"), 0o600))

		rootDir := t.TempDir()
		if err := os.Symlink(outsideHTML, filepath.Join(rootDir, "index.html")); err != nil {
			t.Skipf("symlinks unsupported on this filesystem: %v", err)
		}

		app := newStaticTestApp(t)
		app.StaticFS("/", os.DirFS(rootDir), WithSPAFallback("index.html"))
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/posts/hello", http.NoBody))
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.NotContains(t, rec.Body.String(), "OUTSIDE_HTML")
	})
}

func TestStaticFS_MethodNotAllowedPreservesAllowHeader(t *testing.T) {
	distFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<html>SPA</html>")},
	}
	app := newStaticTestApp(t)
	app.POST("/api/v1/users", func(c *Context) (any, error) {
		return map[string]string{"created": "ok"}, nil
	})
	app.DELETE("/api/v1/users", func(c *Context) (any, error) {
		return nil, nil
	})
	app.POST("/submit", func(c *Context) (any, error) {
		return map[string]string{"submitted": "ok"}, nil
	})
	app.StaticFS("/", distFS, WithSPAFallback("index.html"), WithStaticExclude("/api"))

	h, err := app.HTTPHandler()
	require.NoError(t, err)

	t.Run("excluded prefix matching other methods returns 405 with sorted Allow", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users", http.NoBody))
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		assert.Equal(t, "DELETE, POST", rec.Header().Get("Allow"))
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	})

	t.Run("HEAD on excluded prefix matching other methods returns 405 with zero body", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/api/v1/users", http.NoBody))
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		assert.Equal(t, "DELETE, POST", rec.Header().Get("Allow"))
		assert.Empty(t, rec.Body.Bytes())
	})

	t.Run("non-excluded path matching another method returns 405 instead of SPA fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/submit", http.NoBody))
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		assert.Equal(t, "POST", rec.Header().Get("Allow"))
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
		assert.NotContains(t, rec.Body.String(), "SPA")
	})
}

func TestStaticFS_ConflictValidationWithReservedAndAppRoutes(t *testing.T) {
	distFS := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<html>OK</html>")},
	}

	t.Run("rejects mounting on /favicon.ico", func(t *testing.T) {
		app := newStaticTestApp(t)
		app.StaticFS("/favicon.ico", distFS)
		assert.Error(t, app.RouteError())
		_, err := app.HTTPHandler()
		assert.Error(t, err)
	})

	t.Run("rejects mounting on custom OpenAPI UIPath, SpecPath, and UI asset child", func(t *testing.T) {
		for _, prefix := range []string{"/swagger", "/swagger/app.js", "/custom/openapi.json"} {
			app := newStaticTestApp(t, WithOpenAPI(OpenAPIConfig{
				Title:     "API",
				Version:   "1.0.0",
				ServeSpec: true,
				ServeUI:   true,
				SpecPath:  "/custom/openapi.json",
				UIPath:    "/swagger",
			}))
			app.StaticFS(prefix, distFS)
			assert.Error(t, app.RouteError(), "prefix %s", prefix)
			_, err := app.HTTPHandler()
			assert.Error(t, err, "prefix %s", prefix)
			_, specErr := app.OpenAPISpec()
			assert.Error(t, specErr, "prefix %s", prefix)
		}
	})

	t.Run("rejects duplicate route with GET/HEAD at same path or wildcard", func(t *testing.T) {
		app1 := newStaticTestApp(t)
		app1.StaticFS("/web", distFS)
		app1.GET("/web", func(c *Context) (any, error) { return "dup", nil })
		assert.Error(t, app1.RouteError())
		_, err1 := app1.HTTPHandler()
		assert.Error(t, err1)

		app2 := newStaticTestApp(t)
		app2.GET("/web", func(c *Context) (any, error) { return "dup", nil })
		app2.StaticFS("/web", distFS)
		assert.Error(t, app2.RouteError())
		_, err2 := app2.HTTPHandler()
		assert.Error(t, err2)

		app3 := newStaticTestApp(t)
		app3.StaticFS("/web", distFS)
		type emptyReq struct{}
		type emptyRes struct {
			OK bool `json:"ok"`
		}
		app3.GET("/web", H(func(c *Context, _ *emptyReq) (*emptyRes, error) {
			return &emptyRes{OK: true}, nil
		}))
		assert.Error(t, app3.RouteError())
	})
}

func TestStaticFS_VaryAndAcceptEncodingEdgeCases(t *testing.T) {
	fsys := fstest.MapFS{
		"app.js":    &fstest.MapFile{Data: []byte("0123456789")},
		"app.js.br": &fstest.MapFile{Data: []byte("BROTLI")},
		"app.js.gz": &fstest.MapFile{Data: []byte("GZIP")},
		"plain.txt": &fstest.MapFile{Data: []byte("plain")},
	}
	app := newStaticTestApp(t)
	app.StaticFS("/", fsys)
	h, err := app.HTTPHandler()
	require.NoError(t, err)

	t.Run("sets Vary: Accept-Encoding on identity and Range fallback when companion exists", func(t *testing.T) {
		recIdentity := httptest.NewRecorder()
		h.ServeHTTP(recIdentity, httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody))
		assert.Equal(t, http.StatusOK, recIdentity.Code)
		assert.Empty(t, recIdentity.Header().Get("Content-Encoding"))
		assert.Contains(t, recIdentity.Header().Values("Vary"), "Accept-Encoding")
		assert.Equal(t, "0123456789", recIdentity.Body.String())

		reqRange := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		reqRange.Header.Set("Accept-Encoding", "br, gzip")
		reqRange.Header.Set("Range", "bytes=0-3")
		recRange := httptest.NewRecorder()
		h.ServeHTTP(recRange, reqRange)
		assert.Equal(t, http.StatusPartialContent, recRange.Code)
		assert.Empty(t, recRange.Header().Get("Content-Encoding"))
		assert.Contains(t, recRange.Header().Values("Vary"), "Accept-Encoding")
		assert.Equal(t, "0123", recRange.Body.String())
	})

	t.Run("does not set Vary: Accept-Encoding when no companion exists", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/plain.txt", http.NoBody)
		req.Header.Set("Accept-Encoding", "br, gzip")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Header().Values("Vary"), "Accept-Encoding")
	})

	t.Run("scans multiple Accept-Encoding headers and honors <enc>;q=0 and *;q=0", func(t *testing.T) {
		// Multi-header: gzip accepted, br explicitly rejected with q=0.
		req1 := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req1.Header.Add("Accept-Encoding", "gzip")
		req1.Header.Add("Accept-Encoding", "br;q=0")
		rec1 := httptest.NewRecorder()
		h.ServeHTTP(rec1, req1)
		assert.Equal(t, http.StatusOK, rec1.Code)
		assert.Equal(t, "gzip", rec1.Header().Get("Content-Encoding"))
		assert.Contains(t, rec1.Header().Values("Vary"), "Accept-Encoding")

		// *;q=0 rejects all non-listed encodings.
		req2 := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req2.Header.Set("Accept-Encoding", "*;q=0")
		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, req2)
		assert.Equal(t, http.StatusOK, rec2.Code)
		assert.Empty(t, rec2.Header().Get("Content-Encoding"))
		assert.Contains(t, rec2.Header().Values("Vary"), "Accept-Encoding")

		// br;q=0, * explicitly rejects br.
		req3 := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req3.Header.Set("Accept-Encoding", "br;q=0, *")
		rec3 := httptest.NewRecorder()
		h.ServeHTTP(rec3, req3)
		assert.Equal(t, http.StatusOK, rec3.Code)
		assert.Empty(t, rec3.Header().Get("Content-Encoding"))

		// br, *;q=0 explicitly allows br while rejecting unlisted gzip.
		req4 := httptest.NewRequest(http.MethodGet, "/app.js", http.NoBody)
		req4.Header.Set("Accept-Encoding", "br, *;q=0")
		rec4 := httptest.NewRecorder()
		h.ServeHTTP(rec4, req4)
		assert.Equal(t, http.StatusOK, rec4.Code)
		assert.Equal(t, "br", rec4.Header().Get("Content-Encoding"))
	})
}

func TestWithEndpointCloser_RunsOnAppStop(t *testing.T) {
	type unifiedReq struct{}
	type registeredReq struct {
		Meta `method:"POST" path:"/closed"`
	}
	type resDTO struct {
		OK bool `json:"ok"`
	}

	var closedUnified atomic.Int32
	var closedRegistered atomic.Int32

	app := newStaticTestApp(t)
	app.POST("/unified", H(func(c *Context, _ *unifiedReq) (*resDTO, error) {
		return &resDTO{OK: true}, nil
	}), withEndpointCloser(func() error {
		closedUnified.Add(1)
		return nil
	}))
	require.NoError(t, Register(app, func(c *Context, _ *registeredReq) (*resDTO, error) {
		return &resDTO{OK: true}, nil
	}, withEndpointCloser(func() error {
		closedRegistered.Add(1)
		return nil
	})))

	require.NoError(t, app.Start(t.Context()))
	require.NoError(t, app.Stop(t.Context()))

	assert.Equal(t, int32(1), closedUnified.Load())
	assert.Equal(t, int32(1), closedRegistered.Load())
}

func TestStaticFS_WithStaticSubDirAndZeroArgSPAFallback(t *testing.T) {
	t.Run("valid_subdir_with_zero_arg_spa_fallback", func(t *testing.T) {
		embedded := fstest.MapFS{
			"dist/index.html":   &fstest.MapFile{Data: []byte("<html>Dist SPA</html>")},
			"dist/assets/ui.js": &fstest.MapFile{Data: []byte("console.log('dist');")},
			"README.md":         &fstest.MapFile{Data: []byte("not exposed")},
		}
		app := newStaticTestApp(t)
		app.StaticFS("/", embedded,
			WithStaticSubDir("dist"),
			WithSPAFallback(),
			WithImmutableAssets("/assets"),
		)
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recIndex := httptest.NewRecorder()
		h.ServeHTTP(recIndex, httptest.NewRequest(http.MethodGet, "/", http.NoBody))
		assert.Equal(t, http.StatusOK, recIndex.Code)
		assert.Equal(t, "<html>Dist SPA</html>", recIndex.Body.String())
		assert.Equal(t, "no-cache", recIndex.Header().Get("Cache-Control"))

		recSPA := httptest.NewRecorder()
		h.ServeHTTP(recSPA, httptest.NewRequest(http.MethodGet, "/dashboard/settings", http.NoBody))
		assert.Equal(t, http.StatusOK, recSPA.Code)
		assert.Equal(t, "<html>Dist SPA</html>", recSPA.Body.String())

		recAsset := httptest.NewRecorder()
		h.ServeHTTP(recAsset, httptest.NewRequest(http.MethodGet, "/assets/ui.js", http.NoBody))
		assert.Equal(t, http.StatusOK, recAsset.Code)
		assert.Equal(t, "console.log('dist');", recAsset.Body.String())
		assert.Equal(t, "public, max-age=31536000, immutable", recAsset.Header().Get("Cache-Control"))

		recMissing := httptest.NewRecorder()
		h.ServeHTTP(recMissing, httptest.NewRequest(http.MethodGet, "/README.md", http.NoBody))
		assert.Equal(t, http.StatusNotFound, recMissing.Code)
		assert.Contains(t, recMissing.Header().Get("Content-Type"), "application/json")
	})

	t.Run("subdir_validation_errors_surface_via_RouteError", func(t *testing.T) {
		fsys := fstest.MapFS{
			"dist/index.html": &fstest.MapFile{Data: []byte("ok")},
			"robots.txt":      &fstest.MapFile{Data: []byte("file")},
		}
		for _, bad := range []string{
			"",
			"   ",
			".",
			"./",
			"/dist",
			"../dist",
			"dist/../other",
			`dist\sub`,
			".git",
			"dist/.hidden",
			"missing",
			"robots.txt",
		} {
			app := newStaticTestApp(t)
			app.StaticFS("/", fsys, WithStaticSubDir(bad))
			assert.Error(t, app.RouteError(), "expected RouteError for subdir %q", bad)
		}
	})

	t.Run("subdir_rejects_symlink_along_subdir_on_DirFS", func(t *testing.T) {
		outsideDir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "index.html"), []byte("OUTSIDE"), 0o600))

		rootDir := t.TempDir()
		if err := os.Symlink(outsideDir, filepath.Join(rootDir, "dist")); err != nil {
			t.Skipf("symlinks unsupported on this filesystem: %v", err)
		}

		app := newStaticTestApp(t)
		app.StaticFS("/", os.DirFS(rootDir), WithStaticSubDir("dist"), WithSPAFallback())
		assert.Error(t, app.RouteError())
	})

	t.Run("WithSPAFallback_rejects_multiple_args", func(t *testing.T) {
		fsys := fstest.MapFS{
			"index.html": &fstest.MapFile{Data: []byte("ok")},
		}
		app := newStaticTestApp(t)
		app.StaticFS("/", fsys, WithSPAFallback("index.html", "app.html"))
		assert.Error(t, app.RouteError())
	})
}
