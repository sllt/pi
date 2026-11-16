package pi_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/openapi"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type oasPingReq struct {
	pi.Meta `path:"/ping" method:"GET" tags:"health" summary:"Ping"`
}

type oasPingRes struct {
	Status string `json:"status"`
}

type oasUndefinedSecReq struct {
	pi.Meta `path:"/secret" method:"GET" security:"missingScheme"`
}

type oasItemDTO struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type oasPageRes[T any] struct {
	Items []T   `json:"items"`
	Total int64 `json:"total"`
}

type oasListItemsReq struct {
	pi.Meta `path:"/items" method:"GET" tags:"items" summary:"List items"`
	Limit   int `query:"limit" default:"10" validate:"gte=1,lte=100"`
}

type oasCreateOrderReq struct {
	pi.Meta `path:"/orders" method:"POST" status:"201" tags:"orders" summary:"Create order"`
	Title   string `json:"title" validate:"required,min=3"`
	Nested  struct {
		Amount float64 `json:"amount" validate:"gt=0"`
	} `json:"nested"`
}

type oasCreateOrderRes struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
}

type oasEmptyAckReq struct {
	pi.Meta `path:"/ack" method:"GET" summary:"Empty ack"`
}

type oasEmptyAckRes struct{}

type oasNullableProfileReq struct {
	pi.Meta `path:"/profile" method:"GET" nullable:"true" summary:"Nullable profile"`
}

type oasProfileRes struct {
	Email string `json:"email"`
}

type oasDeleteOrderReq struct {
	pi.Meta `path:"/orders/{id}" method:"DELETE" status:"204" summary:"Delete order"`
	ID      string `path:"id" validate:"required"`
}

func TestOpenAPIConfigAndDefaults(t *testing.T) {
	t.Run("ServeSpec and ServeUI default false while offline export works without Start", func(t *testing.T) {
		var onStartCalled atomic.Bool
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:       "OfflineAPI",
			Version:     "2.0.0",
			Description: "Pure memory spec generation",
		}))
		app.OnStart(func(ctx *pi.Context) error {
			onStartCalled.Store(true)
			return nil
		})
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))

		// Online HTTP endpoints remain 404 when ServeSpec/ServeUI are false.
		require.Equal(t, http.StatusNotFound, testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/.well-known/openapi.json", nil)).Code)
		require.Equal(t, http.StatusNotFound, testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/.well-known/swagger", nil)).Code)

		// Offline export on a fresh unstarted app succeeds with zero OnStart side effects.
		app2 := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "OfflineAPI", Version: "2.0.0"}))
		app2.OnStart(func(ctx *pi.Context) error {
			onStartCalled.Store(true)
			return nil
		})
		require.NoError(t, pi.Register(app2, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))
		spec, err := app2.OpenAPISpec()
		require.NoError(t, err)
		require.Equal(t, openapi.Version303, spec.OpenAPI)
		require.Contains(t, spec.Paths, "/ping")

		jsonBytes, err := app2.OpenAPIJSON()
		require.NoError(t, err)
		require.Contains(t, string(jsonBytes), `"OfflineAPI"`)

		yamlBytes, err := app2.OpenAPIYAML()
		require.NoError(t, err)
		require.Contains(t, string(yamlBytes), "title: OfflineAPI")

		require.False(t, onStartCalled.Load())

		// OpenAPISpec/JSON/YAML must NOT freeze subsequent route registrations.
		type lateReq struct {
			pi.Meta `path:"/late" method:"GET"`
		}
		require.NoError(t, pi.Register(app2, func(ctx *pi.Context, req *lateReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))
	})

	t.Run("Build rejects invalid OpenAPIConfig combinations", func(t *testing.T) {
		_, err := pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:   "Bad",
			Version: "1.0.0",
			ServeUI: true,
		}))
		require.Error(t, err)

		_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{ServeSpec: true}))
		require.ErrorIs(t, err, pi.ErrMissingOpenAPIInfo)

		for _, badSpec := range []string{"openapi.json", "/spec?v=1", "/spec#frag", "/a//b"} {
			_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:     "Bad",
				Version:   "1.0.0",
				ServeSpec: true,
				SpecPath:  badSpec,
			}))
			require.Error(t, err, "specPath=%s", badSpec)
		}

		for _, badUI := range []string{"docs", "/docs?x=1", "/docs#frag", "/", "/.well-known", "/.well-known/"} {
			_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:     "Bad",
				Version:   "1.0.0",
				ServeSpec: true,
				ServeUI:   true,
				UIPath:    badUI,
			}))
			require.Error(t, err, "uiPath=%s", badUI)
		}

		_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "Bad",
			Version:   "1.0.0",
			ServeSpec: true,
			ServeUI:   true,
			SpecPath:  "/docs",
			UIPath:    "/docs",
		}))
		require.Error(t, err)

		// Invalid SecuritySchemes entries.
		for _, scheme := range []openapi.SecurityScheme{
			{Type: ""},
			{Type: "http"},
			{Type: "apiKey", Name: "X-Key"},
			{Type: "apiKey", In: "header"},
		} {
			_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:   "BadSec",
				Version: "1.0.0",
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"auth": scheme,
				},
			}))
			require.Error(t, err)
		}
	})

	t.Run("missing Title Version or undefined SecurityScheme fails export HTTPHandler and Start", func(t *testing.T) {
		noInfoApp := testkit.New(t)
		require.NoError(t, pi.Register(noInfoApp, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))
		_, err := noInfoApp.OpenAPISpec()
		require.ErrorIs(t, err, pi.ErrMissingOpenAPIInfo)
		_, err = noInfoApp.OpenAPIJSON()
		require.ErrorIs(t, err, pi.ErrMissingOpenAPIInfo)
		_, err = noInfoApp.OpenAPIYAML()
		require.ErrorIs(t, err, pi.ErrMissingOpenAPIInfo)

		badSecApp := testkit.New(t,
			pi.WithConfig(map[string]string{
				"HTTP_ENABLED":    "false",
				"GRPC_ENABLED":    "false",
				"METRICS_ENABLED": "false",
				"PI_TELEMETRY":    "false",
				"LOG_LEVEL":       "ERROR",
			}),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:   "SecCheck",
				Version: "1.0.0",
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"Bearer": {Type: "http", Scheme: "bearer"},
				},
			}),
		)
		require.NoError(t, pi.Register(badSecApp, func(ctx *pi.Context, req *oasUndefinedSecReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))

		_, err = badSecApp.OpenAPISpec()
		require.Error(t, err)
		require.Contains(t, err.Error(), "missingScheme")

		_, err = badSecApp.HTTPHandler()
		require.Error(t, err)

		err = badSecApp.Start(t.Context())
		require.Error(t, err)
	})

	t.Run("typed route conflicting with custom SpecPath or UIPath rejected", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "Collision",
			Version:   "1.0.0",
			ServeSpec: true,
			ServeUI:   true,
			SpecPath:  "/api/openapi.json",
			UIPath:    "/docs",
		}))
		type collideSpecReq struct {
			pi.Meta `path:"/api/openapi.json" method:"GET"`
		}
		type collideUIReq struct {
			pi.Meta `path:"/docs" method:"GET"`
		}
		require.Error(t, pi.Register(app, func(ctx *pi.Context, req *collideSpecReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))
		require.Error(t, pi.Register(app, func(ctx *pi.Context, req *collideUIReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))
	})
}

func TestOpenAPIHTTPServingAndUIHTML(t *testing.T) {
	t.Run("custom SpecPath and UIPath with trailing slash handling and asset allowlist", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "ServingAPI",
			Version:   "1.2.3",
			ServeSpec: true,
			ServeUI:   true,
			SpecPath:  "/api/openapi.json",
			UIPath:    "/docs",
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))

		wantSpecJSON, err := app.OpenAPIJSON()
		require.NoError(t, err)

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		specRec := httptest.NewRecorder()
		h.ServeHTTP(specRec, httptest.NewRequest(http.MethodGet, "/api/openapi.json", nil))
		require.Equal(t, http.StatusOK, specRec.Code)
		require.Contains(t, specRec.Header().Get("Content-Type"), "application/json")
		require.Equal(t, string(wantSpecJSON), specRec.Body.String())

		// Without trailing slash: assets relative to "/" -> "docs/scalar.js", spec -> "api/openapi.json".
		noSlashRec := httptest.NewRecorder()
		h.ServeHTTP(noSlashRec, httptest.NewRequest(http.MethodGet, "/docs", nil))
		require.Equal(t, http.StatusOK, noSlashRec.Code)
		require.Contains(t, noSlashRec.Header().Get("Content-Type"), "text/html")
		noSlashHTML := noSlashRec.Body.String()
		require.Contains(t, noSlashHTML, `src="docs/scalar.js"`)
		require.Contains(t, noSlashHTML, `href="docs/favicon-32x32.png"`)
		require.Contains(t, noSlashHTML, `href="docs/favicon-16x16.png"`)
		require.Contains(t, noSlashHTML, `url: "api/openapi.json"`)
		require.NotContains(t, noSlashHTML, `url: "openapi.json"`)
		require.Contains(t, noSlashHTML, "withDefaultFonts: false")
		require.Contains(t, noSlashHTML, "telemetry: false")

		// With trailing slash: assets relative to "/docs/" -> "scalar.js", spec -> "../api/openapi.json".
		slashRec := httptest.NewRecorder()
		h.ServeHTTP(slashRec, httptest.NewRequest(http.MethodGet, "/docs/", nil))
		require.Equal(t, http.StatusOK, slashRec.Code)
		slashHTML := slashRec.Body.String()
		require.Contains(t, slashHTML, `src="scalar.js"`)
		require.Contains(t, slashHTML, `href="favicon-32x32.png"`)
		require.Contains(t, slashHTML, `href="favicon-16x16.png"`)
		require.Contains(t, slashHTML, `url: "../api/openapi.json"`)

		// /docs/scalar.js without Accept-Encoding returns raw JS with Vary: Accept-Encoding.
		rawJSRec := httptest.NewRecorder()
		h.ServeHTTP(rawJSRec, httptest.NewRequest(http.MethodGet, "/docs/scalar.js", nil))
		require.Equal(t, http.StatusOK, rawJSRec.Code)
		require.Equal(t, "text/javascript; charset=utf-8", rawJSRec.Header().Get("Content-Type"))
		require.Equal(t, "Accept-Encoding", rawJSRec.Header().Get("Vary"))
		require.Empty(t, rawJSRec.Header().Get("Content-Encoding"))
		rawJSBytes := rawJSRec.Body.Bytes()
		require.Contains(t, string(rawJSBytes), "createApiReference")
		require.NotContains(t, string(rawJSBytes), "sourceMappingURL")

		// /docs/scalar.js with Accept-Encoding: br, gzip;q=0.8 returns pre-gzipped bytes matching uncompressed JS.
		gzReq := httptest.NewRequest(http.MethodGet, "/docs/scalar.js", nil)
		gzReq.Header.Set("Accept-Encoding", "br, gzip;q=0.8")
		gzJSRec := httptest.NewRecorder()
		h.ServeHTTP(gzJSRec, gzReq)
		require.Equal(t, http.StatusOK, gzJSRec.Code)
		require.Equal(t, "text/javascript; charset=utf-8", gzJSRec.Header().Get("Content-Type"))
		require.Equal(t, "gzip", gzJSRec.Header().Get("Content-Encoding"))
		require.Equal(t, "Accept-Encoding", gzJSRec.Header().Get("Vary"))
		gzReader, err := gzip.NewReader(bytes.NewReader(gzJSRec.Body.Bytes()))
		require.NoError(t, err)
		gunzippedJS, err := io.ReadAll(gzReader)
		require.NoError(t, err)
		require.NoError(t, gzReader.Close())
		require.Equal(t, rawJSBytes, gunzippedJS)

		// /docs/scalar.js with Accept-Encoding: gzip;q=0, deflate explicitly rejects gzip.
		q0Req := httptest.NewRequest(http.MethodGet, "/docs/scalar.js", nil)
		q0Req.Header.Set("Accept-Encoding", "gzip;q=0, deflate")
		q0Rec := httptest.NewRecorder()
		h.ServeHTTP(q0Rec, q0Req)
		require.Equal(t, http.StatusOK, q0Rec.Code)
		require.Equal(t, "text/javascript; charset=utf-8", q0Rec.Header().Get("Content-Type"))
		require.Equal(t, "Accept-Encoding", q0Rec.Header().Get("Vary"))
		require.Empty(t, q0Rec.Header().Get("Content-Encoding"))
		require.Equal(t, rawJSBytes, q0Rec.Body.Bytes())

		// Allowed static image/icon assets served with accurate Content-Type.
		for _, tc := range []struct {
			path string
			ct   string
		}{
			{path: "/docs/favicon-32x32.png", ct: "image/png"},
			{path: "/docs/favicon-16x16.png", ct: "image/png"},
			{path: "/docs/favicon.ico", ct: "image/x-icon"},
		} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			require.Equal(t, http.StatusOK, rec.Code, tc.path)
			require.Contains(t, rec.Header().Get("Content-Type"), tc.ct, tc.path)
			require.NotEmpty(t, rec.Body.Bytes(), tc.path)
		}

		// Disallowed, internal, legacy, extensionless, and traversal asset routes fail cleanly with 404 without panic.
		for _, badPath := range []string{
			"/docs/scalar.js.gz",
			"/docs/swagger-ui.css",
			"/docs/index.html",
			"/docs/files.go",
			"/docs/noext",
			"/docs/unknown.js",
			"/docs/..%2Fsecret",
		} {
			rec := httptest.NewRecorder()
			require.NotPanics(t, func() {
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, badPath, nil))
			})
			require.Equal(t, http.StatusNotFound, rec.Code, badPath)
		}
	})

	t.Run("legacy static openapi.json preserved only when neither WithOpenAPI nor typed routes used", func(t *testing.T) {
		tmpDir := t.TempDir()
		staticDir := filepath.Join(tmpDir, "static")
		require.NoError(t, os.MkdirAll(staticDir, 0o755))
		legacyDoc := []byte(`{"openapi":"3.0.0","info":{"title":"LegacyDisk","version":"0.1.0"}}`)
		require.NoError(t, os.WriteFile(filepath.Join(staticDir, "openapi.json"), legacyDoc, 0o600))
		t.Chdir(tmpDir)

		// 1. Pure legacy app -> reads disk ./static/openapi.json, extensionless /.well-known/noext does not panic.
		legacyApp := testkit.New(t)
		legacyApp.GET("/legacy", func(c *pi.Context) (any, error) { return "ok", nil })
		legacyHandler, err := legacyApp.HTTPHandler()
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		legacyHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/openapi.json", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, string(legacyDoc), rec.Body.String())

		require.NotPanics(t, func() {
			extRec := httptest.NewRecorder()
			legacyHandler.ServeHTTP(extRec, httptest.NewRequest(http.MethodGet, "/.well-known/noext", nil))
		})

		// 2. Typed routes without WithOpenAPI -> must NOT serve legacy disk ./static/openapi.json.
		typedApp := testkit.New(t)
		require.NoError(t, pi.Register(typedApp, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))
		typedRec := testkit.Request(t, typedApp, httptest.NewRequest(http.MethodGet, "/.well-known/openapi.json", nil))
		require.Equal(t, http.StatusNotFound, typedRec.Code)

		// 3. WithOpenAPI ServeSpec=false -> must NOT serve legacy disk ./static/openapi.json.
		oasApp := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "Gen", Version: "1.0.0"}))
		oasRec := testkit.Request(t, oasApp, httptest.NewRequest(http.MethodGet, "/.well-known/openapi.json", nil))
		require.Equal(t, http.StatusNotFound, oasRec.Code)
	})
}

func TestOpenAPIParityIsolationAndCache(t *testing.T) {
	t.Run("JSON YAML parity cache cloning revision invalidation and system route exclusion", func(t *testing.T) {
		app := testkit.New(t,
			pi.WithExplicitHTTPStatus(),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:       "ParityAPI",
				Version:     "1.0.0",
				Description: "JSON and YAML parity test",
				Servers:     []openapi.Server{{URL: "https://api.example.com", Description: "Prod"}},
				ServeSpec:   true,
				ServeUI:     true,
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"Bearer": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
				},
			}),
		)

		privGrp := app.Group("/api").WithDoc(pi.DocTags("users"), pi.DocSecurity("Bearer"))
		require.NoError(t, pi.Register(privGrp, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}, pi.WithEndpointDoc(pi.DocNoSecurity())))

		spec1, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec1.Paths, "/api/ping")
		require.NotContains(t, spec1.Paths, "/.well-known/startup")
		require.NotContains(t, spec1.Paths, "/.well-known/ready")
		require.NotContains(t, spec1.Paths, "/.well-known/health")
		require.NotContains(t, spec1.Paths, "/.well-known/alive")
		require.NotContains(t, spec1.Paths, "/favicon.ico")
		require.NotContains(t, spec1.Paths, "/.well-known/openapi.json")
		require.NotContains(t, spec1.Paths, "/.well-known/swagger")

		// Mutating returned *openapi.Document clone must not mutate cached copy.
		spec1.Info.Title = "MutatedTitle"
		delete(spec1.Paths, "/api/ping")
		spec2, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Equal(t, "ParityAPI", spec2.Info.Title)
		require.Contains(t, spec2.Paths, "/api/ping")

		// Registering another route invalidates cache via revision bump.
		type extraReq struct {
			pi.Meta `path:"/extra" method:"GET"`
		}
		require.NoError(t, pi.Register(privGrp, func(ctx *pi.Context, req *extraReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))
		spec3, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec3.Paths, "/api/extra")

		// JSON and YAML parse to equal structures including explicit security: [].
		jsonBytes, err := app.OpenAPIJSON()
		require.NoError(t, err)
		yamlBytes, err := app.OpenAPIYAML()
		require.NoError(t, err)
		require.Contains(t, string(jsonBytes), `"security": []`)
		require.Contains(t, string(yamlBytes), "security: []")

		var jsonMap, yamlMap map[string]any
		require.NoError(t, json.Unmarshal(jsonBytes, &jsonMap))
		require.NoError(t, yaml.Unmarshal(yamlBytes, &yamlMap))
		jsonViaYAMLBytes, err := json.Marshal(yamlMap)
		require.NoError(t, err)
		var yamlAsJSON map[string]any
		require.NoError(t, json.Unmarshal(jsonViaYAMLBytes, &yamlAsJSON))
		require.Equal(t, jsonMap, yamlAsJSON)
	})

	t.Run("multi-App instance isolation", func(t *testing.T) {
		appA := testkit.New(t,
			pi.WithTypedBindConfig(pi.TypedBindConfig{StrictUnknownFields: true}),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:   "ServiceA",
				Version: "1.0.0",
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"AuthA": {Type: "http", Scheme: "bearer"},
				},
			}),
		)
		appB := testkit.New(t,
			pi.WithTypedBindConfig(pi.TypedBindConfig{StrictUnknownFields: false}),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:   "ServiceB",
				Version: "2.0.0",
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"AuthB": {Type: "apiKey", Name: "X-B", In: "header"},
				},
			}),
		)

		type aReq struct {
			pi.Meta `path:"/only-a" method:"GET" security:"AuthA"`
		}
		type bReq struct {
			pi.Meta `path:"/only-b" method:"GET" security:"AuthB"`
		}
		require.NoError(t, pi.Register(appA, func(ctx *pi.Context, req *aReq) (*oasEmptyAckRes, error) { return nil, nil }))
		require.NoError(t, pi.Register(appB, func(ctx *pi.Context, req *bReq) (*oasEmptyAckRes, error) { return nil, nil }))

		specA, err := appA.OpenAPISpec()
		require.NoError(t, err)
		specB, err := appB.OpenAPISpec()
		require.NoError(t, err)

		require.Equal(t, "ServiceA", specA.Info.Title)
		require.Contains(t, specA.Paths, "/only-a")
		require.NotContains(t, specA.Paths, "/only-b")
		require.Contains(t, specA.Components.SecuritySchemes, "AuthA")
		require.NotContains(t, specA.Components.SecuritySchemes, "AuthB")

		require.Equal(t, "ServiceB", specB.Info.Title)
		require.Contains(t, specB.Paths, "/only-b")
		require.NotContains(t, specB.Paths, "/only-a")
		require.Contains(t, specB.Components.SecuritySchemes, "AuthB")
		require.NotContains(t, specB.Components.SecuritySchemes, "AuthA")
	})

	t.Run("live HTTP JSON responses conform to exported OpenAPI 3.0.3 schemas", func(t *testing.T) {
		app := testkit.New(t,
			pi.WithExplicitHTTPStatus(),
			pi.WithOpenAPI(pi.OpenAPIConfig{Title: "ContractParity", Version: "1.0.0"}),
		)

		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasListItemsReq) (*oasPageRes[oasItemDTO], error) {
			return &oasPageRes[oasItemDTO]{
				Items: []oasItemDTO{{ID: "i1", Name: "alpha"}},
				Total: int64(req.Limit),
			}, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasCreateOrderReq) (*oasCreateOrderRes, error) {
			return &oasCreateOrderRes{OrderID: "ord-1", Amount: req.Nested.Amount}, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasEmptyAckReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasNullableProfileReq) (*oasProfileRes, error) {
			return nil, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasDeleteOrderReq) (*oasEmptyAckRes, error) {
			return nil, nil
		}))

		doc, err := app.OpenAPISpec()
		require.NoError(t, err)

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		// 1. Generic PageRes[oasItemDTO] response parity.
		recList := httptest.NewRecorder()
		h.ServeHTTP(recList, httptest.NewRequest(http.MethodGet, "/items", nil))
		require.Equal(t, http.StatusOK, recList.Code)
		assertJSONBodyMatchesSchema(t, doc, doc.Paths["/items"].Get.Responses["200"].Content["application/json"].Schema, recList.Body.Bytes())

		// 2. Nested 201 POST response parity + 400 error response parity.
		createReq := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"title":"order1","nested":{"amount":19.5}}`))
		createReq.Header.Set("Content-Type", "application/json")
		recCreate := httptest.NewRecorder()
		h.ServeHTTP(recCreate, createReq)
		require.Equal(t, http.StatusCreated, recCreate.Code)
		assertJSONBodyMatchesSchema(t, doc, doc.Paths["/orders"].Post.Responses["201"].Content["application/json"].Schema, recCreate.Body.Bytes())

		badReq := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"title":"ab","nested":{"amount":-1}}`))
		badReq.Header.Set("Content-Type", "application/json")
		recBad := httptest.NewRecorder()
		h.ServeHTTP(recBad, badReq)
		require.Equal(t, http.StatusBadRequest, recBad.Code)
		assertJSONBodyMatchesSchema(t, doc, doc.Paths["/orders"].Post.Responses["400"].Content["application/json"].Schema, recBad.Body.Bytes())

		// 3. Empty Res parity (data: null).
		recAck := httptest.NewRecorder()
		h.ServeHTTP(recAck, httptest.NewRequest(http.MethodGet, "/ack", nil))
		require.Equal(t, http.StatusOK, recAck.Code)
		assertJSONBodyMatchesSchema(t, doc, doc.Paths["/ack"].Get.Responses["200"].Content["application/json"].Schema, recAck.Body.Bytes())

		// 4. Nullable non-empty Res returning nil, nil parity.
		recProf := httptest.NewRecorder()
		h.ServeHTTP(recProf, httptest.NewRequest(http.MethodGet, "/profile", nil))
		require.Equal(t, http.StatusOK, recProf.Code)
		assertJSONBodyMatchesSchema(t, doc, doc.Paths["/profile"].Get.Responses["200"].Content["application/json"].Schema, recProf.Body.Bytes())

		// 5. 204 No Content has no content media type and empty body.
		recDel := httptest.NewRecorder()
		h.ServeHTTP(recDel, httptest.NewRequest(http.MethodDelete, "/orders/ord-1", nil))
		require.Equal(t, http.StatusNoContent, recDel.Code)
		require.Empty(t, recDel.Body.Bytes())
		require.Nil(t, doc.Paths["/orders/{id}"].Delete.Responses["204"].Content)
	})
}

func assertJSONBodyMatchesSchema(t *testing.T, doc *openapi.Document, schema *openapi.Schema, raw []byte) {
	t.Helper()
	var val any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&val))
	assertValueMatchesSchema(t, doc, schema, val, "$")
}

func assertValueMatchesSchema(t *testing.T, doc *openapi.Document, schema *openapi.Schema, val any, path string) {
	t.Helper()
	require.NotNil(t, schema, "missing schema at %s", path)

	if schema.Ref != "" {
		const prefix = "#/components/schemas/"
		require.True(t, strings.HasPrefix(schema.Ref, prefix), "unexpected ref %q at %s", schema.Ref, path)
		name := strings.TrimPrefix(schema.Ref, prefix)
		require.NotNil(t, doc.Components, "missing components at %s", path)
		resolved, ok := doc.Components.Schemas[name]
		require.True(t, ok, "missing schema component %q at %s", name, path)
		assertValueMatchesSchema(t, doc, resolved, val, path)
		return
	}

	if val == nil {
		require.True(t, schema.Nullable, "unexpected null at %s", path)
		return
	}

	if len(schema.AllOf) > 0 {
		for _, sub := range schema.AllOf {
			assertValueMatchesSchema(t, doc, sub, val, path)
		}
		return
	}

	switch schema.Type {
	case "object":
		m, ok := val.(map[string]any)
		require.True(t, ok, "expected object at %s, got %T", path, val)
		for _, reqKey := range schema.Required {
			_, exists := m[reqKey]
			require.True(t, exists, "missing required key %q at %s", reqKey, path)
		}
		for k, propSchema := range schema.Properties {
			if propVal, exists := m[k]; exists {
				assertValueMatchesSchema(t, doc, propSchema, propVal, path+"."+k)
			}
		}
	case "array":
		arr, ok := val.([]any)
		require.True(t, ok, "expected array at %s, got %T", path, val)
		for i, elem := range arr {
			assertValueMatchesSchema(t, doc, schema.Items, elem, path+"[elem]")
			_ = i
		}
	case "string":
		_, ok := val.(string)
		require.True(t, ok, "expected string at %s, got %T", path, val)
	case "boolean":
		_, ok := val.(bool)
		require.True(t, ok, "expected bool at %s, got %T", path, val)
	case "integer":
		num, ok := val.(json.Number)
		require.True(t, ok, "expected json.Number integer at %s, got %T", path, val)
		_, err := num.Int64()
		require.NoError(t, err, "expected integer at %s, got %q", path, num.String())
	case "number":
		num, ok := val.(json.Number)
		require.True(t, ok, "expected json.Number at %s, got %T", path, val)
		_, err := num.Float64()
		require.NoError(t, err, "expected number at %s, got %q", path, num.String())
	}
}

func TestOpenAPISystemProbePathCollisions(t *testing.T) {
	systemPaths := []string{
		service.StartupPath,
		service.ReadyPath,
		service.HealthPath,
		service.AlivePath,
		"/favicon.ico",
	}

	for _, p := range systemPaths {
		t.Run("SpecPath collides with "+p, func(t *testing.T) {
			_, err := pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:     "BadSpec",
				Version:   "1.0.0",
				ServeSpec: true,
				SpecPath:  p,
			}))
			require.Error(t, err)
			require.Contains(t, err.Error(), "SpecPath")
		})

		t.Run("UIPath collides with "+p, func(t *testing.T) {
			_, err := pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:     "BadUI",
				Version:   "1.0.0",
				ServeSpec: true,
				ServeUI:   true,
				UIPath:    p,
			}))
			require.Error(t, err)
			require.Contains(t, err.Error(), "UIPath")
		})
	}

	t.Run("SpecPath directly under UIPath asset pattern is rejected", func(t *testing.T) {
		_, err := pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "ShadowedSpec",
			Version:   "1.0.0",
			ServeSpec: true,
			ServeUI:   true,
			UIPath:    "/docs",
			SpecPath:  "/docs/openapi.json",
		}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "conflicts with UIPath")
	})

	t.Run("custom non-colliding OpenAPI paths preserve system probes", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:     "CustomOK",
			Version:   "1.0.0",
			ServeSpec: true,
			ServeUI:   true,
			SpecPath:  "/openapi.json",
			UIPath:    "/docs",
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))

		for _, target := range append([]string{"/openapi.json", "/docs"}, service.StartupPath, service.ReadyPath, service.HealthPath, service.AlivePath) {
			rec := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, target, nil))
			require.NotEqual(t, http.StatusNotFound, rec.Code, "target=%s", target)
			if target == "/openapi.json" {
				require.Equal(t, http.StatusOK, rec.Code)
				require.Contains(t, rec.Body.String(), `"CustomOK"`)
			}
		}
	})

	t.Run("HTTP server startup logs clickable Swagger UI URL when ServeUI is enabled", func(t *testing.T) {
		var logBuf bytes.Buffer
		app, err := pi.Build(
			pi.WithConfig(map[string]string{
				"HTTP_ADDR":       "127.0.0.1:0",
				"HTTP_ENABLED":    "true",
				"GRPC_ENABLED":    "false",
				"METRICS_ENABLED": "false",
				"PI_TELEMETRY":    "false",
			}),
			pi.WithLogger(logging.NewWriterLogger(logging.INFO, &logBuf, &logBuf)),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:     "StartupLogAPI",
				Version:   "1.0.0",
				ServeSpec: true,
				ServeUI:   true,
			}),
		)
		require.NoError(t, err)
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *oasPingReq) (*oasPingRes, error) {
			return &oasPingRes{Status: "ok"}, nil
		}))

		require.NoError(t, app.Start(t.Context()))
		t.Cleanup(func() { _ = app.Stop(t.Context()) })

		logs := logBuf.String()
		require.Contains(t, logs, "HTTP server listening on http://127.0.0.1:")
		require.Contains(t, logs, "Swagger UI available at http://127.0.0.1:")
		require.Contains(t, logs, "/.well-known/swagger")
	})
}

func TestOpenAPIUnifiedRoutingScopeFormFileXMLAndHeadOptions(t *testing.T) {
	type noMetaQueryReq struct {
		ID string `path:"id" validate:"required"`
	}
	type noMetaBodyReq struct {
		ID   string `path:"id" validate:"required"`
		Name string `json:"name" validate:"required"`
	}
	type noMetaRes struct {
		ID string `json:"id"`
	}
	type formScalarReq struct {
		Email string `form:"email" validate:"required,email"`
	}
	type formFileReq struct {
		pi.Meta `status:"201" summary:"Upload avatar"`
		Avatar  *multipart.FileHeader `form:"avatar" validate:"required"`
	}
	type mediaServeReq struct {
		pi.Meta `produces:"image/*" summary:"Serve avatar"`
		ID      string `path:"id" validate:"required"`
	}
	type rssServeReq struct {
		pi.Meta `produces:"application/rss+xml" summary:"RSS feed"`
	}

	buildUnifiedApp := func(usePiH bool) *pi.App {
		app := testkit.New(t,
			pi.WithExplicitHTTPStatus(),
			pi.WithOpenAPI(pi.OpenAPIConfig{
				Title:   "UnifiedSpec",
				Version: "1.0.0",
				SecuritySchemes: map[string]openapi.SecurityScheme{
					"Bearer": {Type: "http", Scheme: "bearer"},
				},
			}),
		)

		v1 := app.Group("/api/v1").WithDoc(pi.DocTags("items"))
		authed := v1.Scope().WithDoc(pi.DocSecurity("Bearer"))

		getFn := func(ctx *pi.Context, req *noMetaQueryReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		postFn := func(ctx *pi.Context, req *noMetaBodyReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		putFn := func(ctx *pi.Context, req *noMetaBodyReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		patchFn := func(ctx *pi.Context, req *noMetaBodyReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		delFn := func(ctx *pi.Context, req *noMetaQueryReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		headFn := func(ctx *pi.Context, req *noMetaQueryReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		optFn := func(ctx *pi.Context, req *noMetaQueryReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.ID}, nil
		}
		if usePiH {
			v1.GET("/items/{id}", pi.H(getFn))
			authed.POST("/items/{id}", pi.H(postFn))
			authed.PUT("/items/{id}", pi.H(putFn))
			authed.PATCH("/items/{id}", pi.H(patchFn))
			authed.DELETE("/items/{id}", pi.H(delFn))
			v1.HEAD("/items/{id}", pi.H(headFn))
			v1.OPTIONS("/items/{id}", pi.H(optFn))
		} else {
			v1.GET("/items/{id}", getFn)
			authed.POST("/items/{id}", postFn)
			authed.PUT("/items/{id}", putFn)
			authed.PATCH("/items/{id}", patchFn)
			authed.DELETE("/items/{id}", delFn)
			v1.HEAD("/items/{id}", headFn)
			v1.OPTIONS("/items/{id}", optFn)
		}

		v1.POST("/login", func(ctx *pi.Context, req *formScalarReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.Email}, nil
		})
		authed.POST("/upload", func(ctx *pi.Context, req *formFileReq) (*noMetaRes, error) {
			return &noMetaRes{ID: req.Avatar.Filename}, nil
		})
		app.GET("/media/{id}", func(ctx *pi.Context, req *mediaServeReq) (*response.File, error) {
			return &response.File{Content: []byte("img"), ContentType: "image/png"}, nil
		})
		app.GET("/rss.xml", func(ctx *pi.Context, req *rssServeReq) (*response.XML, error) {
			return &response.XML{Content: []byte("<rss/>")}, nil
		})
		return app
	}

	appDirect := buildUnifiedApp(false)
	appPiH := buildUnifiedApp(true)

	jsonDirect, err := appDirect.OpenAPIJSON()
	require.NoError(t, err)
	jsonPiH, err := appPiH.OpenAPIJSON()
	require.NoError(t, err)
	require.Equal(t, string(jsonDirect), string(jsonPiH))

	yamlDirect, err := appDirect.OpenAPIYAML()
	require.NoError(t, err)
	yamlPiH, err := appPiH.OpenAPIYAML()
	require.NoError(t, err)
	require.Equal(t, string(yamlDirect), string(yamlPiH))

	doc, err := appDirect.OpenAPISpec()
	require.NoError(t, err)

	item := doc.Paths["/api/v1/items/{id}"]
	require.NotNil(t, item)
	require.NotNil(t, item.Get)
	require.NotNil(t, item.Post)
	require.NotNil(t, item.Put)
	require.NotNil(t, item.Patch)
	require.NotNil(t, item.Delete)
	require.NotNil(t, item.Head)
	require.NotNil(t, item.Options)

	// Scope security isolation: GET (on v1) has no security, POST (on v1.Scope()) has Bearer + inherited "items" tag.
	require.Nil(t, item.Get.Security)
	require.Equal(t, []string{"items"}, item.Get.Tags)
	require.NotNil(t, item.Post.Security)
	require.Equal(t, []openapi.SecurityRequirement{{"Bearer": {}}}, *item.Post.Security)
	require.Equal(t, []string{"items"}, item.Post.Tags)

	// Scalar form generates both urlencoded and multipart requestBody schemas.
	loginOp := doc.Paths["/api/v1/login"].Post
	require.NotNil(t, loginOp.RequestBody)
	require.Contains(t, loginOp.RequestBody.Content, "application/x-www-form-urlencoded")
	require.Contains(t, loginOp.RequestBody.Content, "multipart/form-data")

	// File form generates multipart/form-data with format: binary.
	uploadOp := doc.Paths["/api/v1/upload"].Post
	require.NotNil(t, uploadOp.RequestBody)
	require.True(t, uploadOp.RequestBody.Required)
	require.Contains(t, uploadOp.RequestBody.Content, "multipart/form-data")
	require.NotContains(t, uploadOp.RequestBody.Content, "application/x-www-form-urlencoded")

	// *response.File and *response.XML emit produces media types without leaking File/XML into components.schemas.
	mediaOp := doc.Paths["/media/{id}"].Get
	require.Contains(t, mediaOp.Responses["200"].Content, "image/*")
	rssOp := doc.Paths["/rss.xml"].Get
	require.Contains(t, rssOp.Responses["200"].Content, "application/rss+xml")
	if doc.Components != nil {
		require.NotContains(t, doc.Components.Schemas, "File")
		require.NotContains(t, doc.Components.Schemas, "XML")
	}
}
