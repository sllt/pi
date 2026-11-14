package pi

import (
	"bytes"
	"compress/gzip"
	"embed"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/response"
)

//go:embed static/*
var fs embed.FS

const (
	OpenAPIJSON                  = "openapi.json"
	scalarJSAssetName            = "scalar.js"
	scalarJSGzipPath             = "static/scalar.js.gz"
	scalarJSContentType          = "text/javascript; charset=utf-8"
	maxEmbeddedScalarJSBytes     = 32 << 20
	maxEmbeddedScalarJSReadLimit = maxEmbeddedScalarJSBytes + 1
)

var errScalarJSTooLarge = errors.New("embedded scalar.js exceeds maximum allowed size")

var loadRawScalarJS = sync.OnceValues(func() ([]byte, error) {
	gzData, err := fs.ReadFile(scalarJSGzipPath)
	if err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(bytes.NewReader(gzData))
	if err != nil {
		return nil, err
	}

	raw, readErr := io.ReadAll(io.LimitReader(gr, maxEmbeddedScalarJSReadLimit))
	closeErr := gr.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(raw) > maxEmbeddedScalarJSBytes {
		return nil, errScalarJSTooLarge
	}

	return raw, nil
})

func requestAcceptsGzip(c *Context) bool {
	if c == nil {
		return false
	}
	httpReq, ok := c.Request.(*piHTTP.Request)
	if !ok || httpReq == nil {
		return false
	}
	r := httpReq.HTTPRequest()
	if r == nil {
		return false
	}

	accepted := false
	for _, headerVal := range r.Header.Values("Accept-Encoding") {
		for _, part := range strings.Split(headerVal, ",") {
			entryAccepts, entryRejects := parseAcceptEncodingGzipEntry(part)
			if entryRejects {
				return false
			}
			if entryAccepts {
				accepted = true
			}
		}
	}

	return accepted
}

func parseAcceptEncodingGzipEntry(part string) (accepted, rejected bool) {
	segments := strings.Split(part, ";")
	coding := strings.ToLower(strings.TrimSpace(segments[0]))
	if coding != "gzip" && coding != "x-gzip" {
		return false, false
	}

	hasQ := false
	qVal := 1.0
	for _, param := range segments[1:] {
		trimmed := strings.TrimSpace(param)
		if trimmed == "" {
			continue
		}
		k, v, hasEq := strings.Cut(trimmed, "=")
		if !hasEq || strings.ToLower(strings.TrimSpace(k)) != "q" {
			continue
		}
		if hasQ {
			return false, false
		}
		q, ok := parseAcceptEncodingQValue(strings.TrimSpace(v))
		if !ok {
			return false, false
		}
		hasQ = true
		qVal = q
	}

	if hasQ && qVal == 0 {
		return false, true
	}

	return qVal > 0 && qVal <= 1, false
}

func parseAcceptEncodingQValue(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if (ch < '0' || ch > '9') && ch != '.' {
			return 0, false
		}
	}
	q, err := strconv.ParseFloat(raw, 64)
	if err != nil || q < 0 || q > 1 {
		return 0, false
	}

	return q, true
}

func serveScalarJS(c *Context) (response.File, error) {
	if requestAcceptsGzip(c) {
		gzData, err := fs.ReadFile(scalarJSGzipPath)
		if err != nil {
			return response.File{}, err
		}

		return response.File{
			Content:     gzData,
			ContentType: scalarJSContentType,
			Headers: map[string]string{
				"Content-Encoding": "gzip",
				"Vary":             "Accept-Encoding",
			},
		}, nil
	}

	rawData, err := loadRawScalarJS()
	if err != nil {
		return response.File{}, err
	}

	return response.File{
		Content:     rawData,
		ContentType: scalarJSContentType,
		Headers: map[string]string{
			"Vary": "Accept-Encoding",
		},
	}, nil
}

// OpenAPIHandler serves the `openapi.json` file at the specified path.
// It reads the file from the disk and returns its content as a response.
func OpenAPIHandler(c *Context) (any, error) {
	rootDir, _ := os.Getwd()
	filePath := filepath.Join(rootDir, "static", OpenAPIJSON)

	b, err := os.ReadFile(filepath.Clean(filePath))
	if err != nil {
		c.Errorf("Failed to read OpenAPI JSON file at path %s: %v", filePath, err)
		return nil, err
	}

	return response.File{Content: b, ContentType: "application/json"}, nil
}

// SwaggerUIHandler serves the static files of the embedded Scalar API Reference UI.
func SwaggerUIHandler(c *Context) (any, error) {
	fileName := strings.TrimSpace(c.PathParam("name"))
	if fileName == "" {
		// Read the index.html file.
		fileName = "index.html"
	}

	if fileName == "scalar.js.gz" || fileName == "files.go" {
		err := piHTTP.ErrorInvalidRoute{}
		if c.Container != nil && c.Logger != nil {
			c.Errorf("Rejected embedded UI file %s: %v", fileName, err)
		}
		return nil, err
	}

	if fileName == scalarJSAssetName {
		file, err := serveScalarJS(c)
		if err != nil {
			if c.Container != nil && c.Logger != nil {
				c.Errorf("Failed to serve Scalar UI script from embedded file system: %v", err)
			}
			return nil, err
		}
		return file, nil
	}

	data, err := fs.ReadFile("static/" + fileName)
	if err != nil {
		if c.Container != nil && c.Logger != nil {
			c.Errorf("Failed to read UI file %s from embedded file system: %v", fileName, err)
		}
		return nil, err
	}

	ct := mime.TypeByExtension(filepath.Ext(fileName))
	if ct == "" && fileName == "favicon.ico" {
		ct = "image/x-icon"
	}

	// Return the embedded UI file response.
	return response.File{Content: data, ContentType: ct}, nil
}

func (a *App) checkAndAddOpenAPIDocumentationLocked() {
	if a.hasOpenAPIConfig || a.hasTypedRoutesLocked() {
		a.mountOpenAPIRoutesLocked()
		return
	}

	// If the openapi.json file exists in the static directory, set up legacy routes for OpenAPI and Scalar UI documentation.
	if _, err := os.Stat("./static/" + piHTTP.DefaultSwaggerFileName); err == nil {
		// Route to serve the OpenAPI JSON specification file.
		a.addSystemRouteLocked(http.MethodGet, "/.well-known/"+piHTTP.DefaultSwaggerFileName, OpenAPIHandler)
		// Route to serve the embedded Scalar API Reference UI at the legacy /.well-known/swagger path.
		a.addSystemRouteLocked(http.MethodGet, "/.well-known/swagger", SwaggerUIHandler)
		// Catchall route: any request to /.well-known/{name} will be handled by SwaggerUIHandler.
		a.addSystemRouteLocked(http.MethodGet, "/.well-known/{name}", SwaggerUIHandler)
	}
}
