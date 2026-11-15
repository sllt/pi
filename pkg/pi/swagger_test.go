package pi

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/logging"
)

func TestOpenAPIHandler(t *testing.T) {
	// Create the openapi.json file within the static directory
	openAPIFilePath := filepath.Join("static", OpenAPIJSON)

	openAPIContent := []byte(`{"swagger": "2.0", "info": {"version": "1.0.0", "title": "Sample API"}}`)
	if err := os.WriteFile(openAPIFilePath, openAPIContent, 0600); err != nil {
		t.Fatalf("Failed to create openapi.json file: %v", err)
	}

	// Defer removal of the openapi.json file from static directory
	defer func() {
		if err := os.Remove("static/openapi.json"); err != nil {
			t.Errorf("Failed to remove file from static directory: %v", err)
		}
	}()

	testKernel := newTestKernel(nil)

	ctx := createTestContext(http.MethodGet, "/.well-known/openapi.json", "", nil, testKernel)

	result, err := OpenAPIHandler(ctx)
	if err != nil {
		t.Fatalf("OpenAPIHandler failed: %v", err)
	}

	fileResponse, ok := result.(response.File)
	if !ok {
		t.Errorf("Expected a FileResponse")
	}

	if fileResponse.ContentType != "application/json" {
		t.Errorf("Expected content type 'application/json', got '%s'", fileResponse.ContentType)
	}

	if !bytes.Equal(fileResponse.Content, openAPIContent) {
		t.Errorf("Expected response content '%s', got '%s'", string(openAPIContent), string(fileResponse.Content))
	}
}

func TestOpenAPIHandler_Error(t *testing.T) {
	testKernel := newTestKernel(nil)

	ctx := createTestContext(http.MethodGet, "/.well-known/openapi.json", "", nil, testKernel)
	ctx.ContextLogger = *logging.NewContextLogger(ctx, testKernel.getLogger())

	result, err := OpenAPIHandler(ctx)

	assert.Nil(t, result, "Expected result to be nil")
	errors.Is(err, &os.PathError{Path: "/Users/raramuri/Projects/github.com/sllt/pi/pi/pkg/pi/static/openapi.json"})
	require.Error(t, err, "Expected error")
}

func TestSwaggerHandler(t *testing.T) {
	testKernel := newTestKernel(nil)

	tests := []struct {
		desc           string
		fileName       string
		acceptEncoding string
		contentType    string
		wantGzip       bool
	}{
		{desc: "fetch index.html", fileName: "", contentType: "text/html"},
		{desc: "fetch favicon image", fileName: "favicon-16x16.png", contentType: "image/png"},
		{desc: "fetch scalar.js uncompressed", fileName: "scalar.js", contentType: "text/javascript"},
		{desc: "fetch scalar.js gzip", fileName: "scalar.js", acceptEncoding: "gzip", contentType: "text/javascript", wantGzip: true},
	}

	var uncompressedScalarJS []byte
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			testReq := httptest.NewRequest(http.MethodGet, "/.well-known/swagger/"+tc.fileName, http.NoBody)
			if tc.acceptEncoding != "" {
				testReq.Header.Set("Accept-Encoding", tc.acceptEncoding)
			}
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("name", tc.fileName)
			testReq = testReq.WithContext(context.WithValue(testReq.Context(), chi.RouteCtxKey, rctx))
			piReq := piHTTP.NewRequest(testReq)

			ctx := newContext(piHTTP.NewResponder(httptest.NewRecorder(), http.MethodGet), piReq, testKernel)

			resp, err := SwaggerUIHandler(ctx)
			require.NoError(t, err, "Expected err to be nil")

			fileResponse, ok := resp.(response.File)
			require.True(t, ok, "Expected a response.File")
			assert.Equal(t, tc.contentType, strings.Split(fileResponse.ContentType, ";")[0])

			if tc.fileName == "scalar.js" {
				assert.Equal(t, "Accept-Encoding", fileResponse.Headers["Vary"])
				if tc.wantGzip {
					assert.Equal(t, "gzip", fileResponse.Headers["Content-Encoding"])
					gr, gzErr := gzip.NewReader(bytes.NewReader(fileResponse.Content))
					require.NoError(t, gzErr)
					plain, readErr := io.ReadAll(gr)
					require.NoError(t, readErr)
					require.NoError(t, gr.Close())
					assert.Contains(t, string(plain), "createApiReference")
					assert.NotContains(t, string(plain), "sourceMappingURL")
					if len(uncompressedScalarJS) > 0 {
						assert.Equal(t, uncompressedScalarJS, plain)
					}
				} else {
					assert.Empty(t, fileResponse.Headers["Content-Encoding"])
					assert.Contains(t, string(fileResponse.Content), "createApiReference")
					assert.NotContains(t, string(fileResponse.Content), "sourceMappingURL")
					uncompressedScalarJS = fileResponse.Content
				}
			}
		})
	}
}

func TestRequestAcceptsGzip(t *testing.T) {
	testKernel := newTestKernel(nil)

	cases := []struct {
		name    string
		headers []string
		want    bool
	}{
		{name: "no header", headers: nil, want: false},
		{name: "exact gzip", headers: []string{"gzip"}, want: true},
		{name: "x-gzip uppercase", headers: []string{"X-GZIP"}, want: true},
		{name: "weighted gzip", headers: []string{"br, gzip;q=0.8"}, want: true},
		{name: "q equals one", headers: []string{"deflate, gzip;Q=1.000"}, want: true},
		{name: "explicit q=0 rejection", headers: []string{"gzip;q=0, deflate"}, want: false},
		{name: "explicit q=0.000 rejection", headers: []string{"gzip;q=0.000"}, want: false},
		{name: "bare wildcard not treated as gzip", headers: []string{"*"}, want: false},
		{name: "weighted wildcard not treated as gzip", headers: []string{"*;q=1"}, want: false},
		{name: "multi header with gzip", headers: []string{"br", "gzip;q=0.5"}, want: true},
		{name: "multi header explicit reject overrides", headers: []string{"gzip;q=0.5", "gzip;q=0"}, want: false},
		{name: "invalid qvalue rejected", headers: []string{"gzip;q=1.5"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/.well-known/swagger/scalar.js", http.NoBody)
			for _, h := range tc.headers {
				req.Header.Add("Accept-Encoding", h)
			}
			ctx := newContext(piHTTP.NewResponder(httptest.NewRecorder(), http.MethodGet), piHTTP.NewRequest(req), testKernel)
			assert.Equal(t, tc.want, requestAcceptsGzip(ctx))
		})
	}
}

func TestSwaggerUIHandler_Error(t *testing.T) {
	testKernel := newTestKernel(nil)

	for _, badFile := range []string{"abc.abc", "scalar.js.gz", "files.go"} {
		t.Run(badFile, func(t *testing.T) {
			testReq := httptest.NewRequest(http.MethodGet, "/.well-known/swagger/"+badFile, http.NoBody)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("name", badFile)
			testReq = testReq.WithContext(context.WithValue(testReq.Context(), chi.RouteCtxKey, rctx))

			piReq := piHTTP.NewRequest(testReq)
			ctx := newContext(piHTTP.NewResponder(httptest.NewRecorder(), http.MethodGet), piReq, testKernel)
			ctx.ContextLogger = *logging.NewContextLogger(ctx, testKernel.getLogger())

			resp, err := SwaggerUIHandler(ctx)

			assert.Nil(t, resp)
			require.Error(t, err)
		})
	}
}
