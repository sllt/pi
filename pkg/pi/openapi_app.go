package pi

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/contract"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/openapi"
	"github.com/sllt/pi/pkg/pi/service"
)

// ErrMissingOpenAPIInfo is returned when OpenAPI serving or export is enabled without Title and Version.
var ErrMissingOpenAPIInfo = errors.New("OpenAPI Title and Version are required")

// OpenAPIConfig configures OpenAPI 3.0.3 generation and optional Spec/Scalar API Reference UI HTTP endpoints.
type OpenAPIConfig struct {
	Title           string
	Version         string
	Description     string
	Servers         []openapi.Server
	ServeSpec       bool
	ServeUI         bool
	SpecPath        string
	UIPath          string
	SecuritySchemes map[string]openapi.SecurityScheme
}

// WithOpenAPI configures OpenAPI metadata, security schemes, and optional Spec/UI serving on the application.
func WithOpenAPI(cfg OpenAPIConfig) Option {
	return func(o *buildOptions) error {
		o.openAPI = cloneOpenAPIConfig(cfg)
		o.hasOpenAPI = true
		return nil
	}
}

// OpenAPISpec validates all registered routes and returns a defensive clone of the generated OpenAPI 3.0.3 document.
// Calling OpenAPISpec never starts servers, triggers OnStart hooks, or freezes route registration.
func (a *App) OpenAPISpec() (*openapi.Document, error) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil, errors.New("application has no HTTP router")
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if err := a.syncAndValidateRoutesLocked(true); err != nil {
		return nil, err
	}
	if strings.TrimSpace(a.openAPI.Title) == "" || strings.TrimSpace(a.openAPI.Version) == "" {
		return nil, ErrMissingOpenAPIInfo
	}

	if reg.hasCachedDoc && reg.cachedRev == reg.revision && reg.cachedDoc != nil {
		return reg.cachedDoc.Clone(), nil
	}

	doc, err := a.buildOpenAPIDocumentLocked()
	if err != nil {
		return nil, err
	}
	reg.cachedDoc = doc
	reg.cachedRev = reg.revision
	reg.hasCachedDoc = true

	return doc.Clone(), nil
}

// OpenAPIJSON validates routes and serializes the generated OpenAPI 3.0.3 document as deterministic JSON.
func (a *App) OpenAPIJSON() ([]byte, error) {
	doc, err := a.OpenAPISpec()
	if err != nil {
		return nil, err
	}
	return doc.MarshalJSONBytes()
}

// OpenAPIYAML validates routes and serializes the generated OpenAPI 3.0.3 document as deterministic YAML.
func (a *App) OpenAPIYAML() ([]byte, error) {
	doc, err := a.OpenAPISpec()
	if err != nil {
		return nil, err
	}
	return doc.MarshalYAMLBytes()
}

func (a *App) buildOpenAPIDocumentLocked() (*openapi.Document, error) {
	var entries []*typedRouteEntry
	walkGroupNodesLocked(a.httpServer.registry.root, "", docConfig{}, func(node *GroupNode, _ string, _ docConfig) {
		for i := range node.routes {
			meta := node.routeMetaAt(i)
			if meta == nil || meta.isSystem || meta.typed == nil || meta.typed.op == nil {
				continue
			}
			entries = append(entries, meta.typed)
		}
	})

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].op.CanonicalPath != entries[j].op.CanonicalPath {
			return entries[i].op.CanonicalPath < entries[j].op.CanonicalPath
		}
		return methodSortRank(entries[i].op.Method) < methodSortRank(entries[j].op.Method)
	})

	exportSR := contract.NewSchemaRegistry()
	paths := make(map[string]*openapi.PathItem)

	for _, entry := range entries {
		op := entry.op
		openapiOp := op.BuildOpenAPIOperation(exportSR)
		item, exists := paths[op.CanonicalPath]
		if !exists || item == nil {
			item = &openapi.PathItem{}
			paths[op.CanonicalPath] = item
		}
		assignPathItemOperation(item, op.Method, openapiOp)
	}

	var servers []openapi.Server
	if len(a.openAPI.Servers) > 0 {
		servers = make([]openapi.Server, len(a.openAPI.Servers))
		copy(servers, a.openAPI.Servers)
	}

	schemas := exportSR.Schemas()
	var components *openapi.Components
	if len(schemas) > 0 || len(a.openAPI.SecuritySchemes) > 0 {
		components = &openapi.Components{}
		if len(schemas) > 0 {
			components.Schemas = make(map[string]*openapi.Schema, len(schemas))
			for name, schema := range schemas {
				components.Schemas[name] = schema.Clone()
			}
		}
		if len(a.openAPI.SecuritySchemes) > 0 {
			components.SecuritySchemes = make(map[string]openapi.SecurityScheme, len(a.openAPI.SecuritySchemes))
			for name, scheme := range a.openAPI.SecuritySchemes {
				components.SecuritySchemes[name] = scheme
			}
		}
	}

	return &openapi.Document{
		OpenAPI: openapi.Version303,
		Info: openapi.Info{
			Title:       a.openAPI.Title,
			Version:     a.openAPI.Version,
			Description: a.openAPI.Description,
		},
		Servers:    servers,
		Paths:      paths,
		Components: components,
	}, nil
}

func (a *App) mountOpenAPIRoutesLocked() {
	if !a.hasOpenAPIConfig {
		return
	}
	if a.openAPI.ServeSpec && a.openAPI.SpecPath != "" {
		a.addSystemRouteLocked(http.MethodGet, a.openAPI.SpecPath, a.serveOpenAPISpecHandler)
	}
	if a.openAPI.ServeUI && a.openAPI.UIPath != "" {
		a.addSystemRouteLocked(http.MethodGet, a.openAPI.UIPath, a.serveOpenAPIUIHandler)
		a.addSystemRouteLocked(http.MethodGet, swaggerAssetRoutePattern(a.openAPI.UIPath), a.serveOpenAPIUIHandler)
	}
}

func (a *App) logOpenAPIEndpoints() {
	if a == nil || a.httpServer == nil {
		return
	}
	l := a.httpServer.listener.Load()
	if l == nil || l.Addr() == nil {
		return
	}
	tlsEnabled := a.httpServer.certFile != "" && a.httpServer.keyFile != ""
	baseURL := formatClickableBaseURL(l.Addr().String(), tlsEnabled)

	if a.hasOpenAPIConfig {
		if a.openAPI.ServeUI && a.openAPI.UIPath != "" {
			a.Logger().Infof("Swagger UI available at %s%s", baseURL, a.openAPI.UIPath)
		} else if a.openAPI.ServeSpec && a.openAPI.SpecPath != "" {
			a.Logger().Infof("OpenAPI spec available at %s%s", baseURL, a.openAPI.SpecPath)
		}
		return
	}

	if a.httpServer.registry != nil {
		a.httpServer.registry.mu.Lock()
		hasTyped := a.hasTypedRoutesLocked()
		a.httpServer.registry.mu.Unlock()
		if hasTyped {
			return
		}
	}
	if _, err := os.Stat("./static/" + piHTTP.DefaultSwaggerFileName); err == nil {
		a.Logger().Infof("Swagger UI available at %s/.well-known/swagger", baseURL)
	}
}

func (a *App) serveOpenAPISpecHandler(_ *Context) (any, error) {
	data, err := a.OpenAPIJSON()
	if err != nil {
		return nil, err
	}
	return response.File{
		Content:     data,
		ContentType: "application/json; charset=utf-8",
	}, nil
}

// serveOpenAPIUIHandler serves the embedded 100% offline Scalar API Reference UI and its allowlisted assets.
func (a *App) serveOpenAPIUIHandler(c *Context) (any, error) {
	fileName := strings.TrimSpace(c.PathParam("name"))
	if fileName == "" {
		rawHTML, err := fs.ReadFile("static/index.html")
		if err != nil {
			return nil, err
		}
		reqPath := a.openAPI.UIPath
		if httpReq, ok := c.Request.(*piHTTP.Request); ok && httpReq != nil {
			if r := httpReq.HTTPRequest(); r != nil {
				if rawReqPath := extractRawRequestPath(r); rawReqPath != "" {
					reqPath = rawReqPath
				}
			}
		}
		rendered, err := openapi.RenderUIHTML(a.openAPI.UIPath, a.openAPI.SpecPath, reqPath, rawHTML)
		if err != nil {
			return nil, err
		}
		return response.File{
			Content:     rendered,
			ContentType: "text/html; charset=utf-8",
		}, nil
	}

	ct, ok := openapi.IsAllowedUIAsset(fileName)
	if !ok {
		return nil, piHTTP.ErrorInvalidRoute{}
	}
	if fileName == scalarJSAssetName {
		file, err := serveScalarJS(c)
		if err != nil {
			return nil, err
		}
		return file, nil
	}
	data, err := fs.ReadFile("static/" + fileName)
	if err != nil {
		return nil, piHTTP.ErrorInvalidRoute{}
	}
	return response.File{
		Content:     data,
		ContentType: ct,
	}, nil
}

func validateAndNormalizeOpenAPIConfig(cfg OpenAPIConfig) (OpenAPIConfig, error) {
	out := cloneOpenAPIConfig(cfg)
	out.Title = strings.TrimSpace(out.Title)
	out.Version = strings.TrimSpace(out.Version)

	if out.ServeUI && !out.ServeSpec {
		return out, errors.New("openapi: ServeUI requires ServeSpec to be true")
	}
	if out.ServeSpec && (out.Title == "" || out.Version == "") {
		return out, ErrMissingOpenAPIInfo
	}

	if strings.TrimSpace(out.SpecPath) != "" {
		norm, err := normalizeOpenAPIRoutePath(out.SpecPath, "SpecPath")
		if err != nil {
			return out, err
		}
		out.SpecPath = norm
	} else if out.ServeSpec {
		out.SpecPath = "/.well-known/openapi.json"
	}

	if strings.TrimSpace(out.UIPath) != "" {
		norm, err := normalizeOpenAPIRoutePath(out.UIPath, "UIPath")
		if err != nil {
			return out, err
		}
		out.UIPath = norm
	} else if out.ServeUI {
		out.UIPath = "/.well-known/swagger"
	}

	if out.UIPath == "/" || out.UIPath == "/.well-known" {
		return out, fmt.Errorf("openapi: UIPath %q is not allowed because asset routes would shadow root or system probe routes", out.UIPath)
	}

	if out.SpecPath != "" && out.UIPath != "" && out.SpecPath == out.UIPath {
		return out, fmt.Errorf("openapi: SpecPath and UIPath must differ, both resolve to %q", out.SpecPath)
	}

	if err := validateOpenAPIRoutePaths(out.SpecPath, out.UIPath); err != nil {
		return out, err
	}

	for name, scheme := range out.SecuritySchemes {
		if strings.TrimSpace(name) == "" {
			return out, errors.New("openapi: security scheme name must not be empty")
		}
		schemeType := strings.TrimSpace(scheme.Type)
		if schemeType == "" {
			return out, fmt.Errorf("openapi: security scheme %q requires a non-empty Type", name)
		}
		switch schemeType {
		case "http":
			if strings.TrimSpace(scheme.Scheme) == "" {
				return out, fmt.Errorf("openapi: http security scheme %q requires a non-empty Scheme", name)
			}
		case "apiKey":
			if strings.TrimSpace(scheme.Name) == "" || strings.TrimSpace(scheme.In) == "" {
				return out, fmt.Errorf("openapi: apiKey security scheme %q requires non-empty Name and In", name)
			}
		}
	}

	return out, nil
}

func normalizeOpenAPIRoutePath(raw, fieldName string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("openapi: %s %q must start with '/'", fieldName, raw)
	}
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch <= 0x20 || ch == 0x7f || ch == '?' || ch == '#' || ch == '*' || ch == '{' || ch == '}' {
			return "", fmt.Errorf("openapi: %s %q contains invalid characters", fieldName, raw)
		}
	}
	if strings.Contains(trimmed, "//") {
		return "", fmt.Errorf("openapi: %s %q must not contain empty path segments", fieldName, raw)
	}
	for _, seg := range strings.Split(strings.Trim(trimmed, "/"), "/") {
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("openapi: %s %q must not contain relative path segments", fieldName, raw)
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		cleaned = "/"
	}
	return cleaned, nil
}

func builtinSystemPaths() []string {
	return []string{
		service.StartupPath,
		service.ReadyPath,
		service.HealthPath,
		service.AlivePath,
		"/favicon.ico",
	}
}

func validateOpenAPIRoutePaths(specPath, uiPath string) error {
	sysPaths := builtinSystemPaths()
	systemShapes := make(map[string]string, len(sysPaths))
	for _, p := range sysPaths {
		_, shape := contract.NormalizeRouteConflict("", http.MethodGet, p)
		systemShapes[shape] = p
	}

	var specShape string
	if specPath != "" {
		_, specShape = contract.NormalizeRouteConflict("", http.MethodGet, specPath)
		if sysPath, conflict := systemShapes[specShape]; conflict {
			return fmt.Errorf("openapi: SpecPath %q conflicts with built-in system route %q", specPath, sysPath)
		}
	}

	var uiShape, assetShape string
	if uiPath != "" {
		_, uiShape = contract.NormalizeRouteConflict("", http.MethodGet, uiPath)
		if sysPath, conflict := systemShapes[uiShape]; conflict {
			return fmt.Errorf("openapi: UIPath %q conflicts with built-in system route %q", uiPath, sysPath)
		}
		assetPattern := swaggerAssetRoutePattern(uiPath)
		_, assetShape = contract.NormalizeRouteConflict("", http.MethodGet, assetPattern)
		if sysPath, conflict := systemShapes[assetShape]; conflict {
			return fmt.Errorf("openapi: UIPath %q asset route %q conflicts with built-in system route %q", uiPath, assetPattern, sysPath)
		}
		for _, sysPath := range sysPaths {
			if isDirectChildRoute(uiPath, sysPath) {
				return fmt.Errorf("openapi: UIPath %q asset route %q shadows built-in system route %q", uiPath, assetPattern, sysPath)
			}
		}
	}

	if specPath != "" && uiPath != "" {
		if specShape == uiShape || specShape == assetShape || isDirectChildRoute(uiPath, specPath) {
			return fmt.Errorf("openapi: SpecPath %q conflicts with UIPath %q", specPath, uiPath)
		}
	}

	return nil
}

func isDirectChildRoute(parentPath, childPath string) bool {
	trimmedParent := strings.TrimRight(parentPath, "/")
	prefix := trimmedParent + "/"
	if !strings.HasPrefix(childPath, prefix) {
		return false
	}
	rest := strings.TrimPrefix(childPath, prefix)
	return rest != "" && !strings.Contains(rest, "/")
}

func swaggerAssetRoutePattern(uiPath string) string {
	trimmed := strings.TrimRight(uiPath, "/")
	if trimmed == "" {
		return "/{name}"
	}
	return trimmed + "/{name}"
}

func extractRawRequestPath(r *http.Request) string {
	if r == nil {
		return ""
	}
	if rawURI := strings.TrimSpace(r.RequestURI); rawURI != "" {
		if idx := strings.IndexAny(rawURI, "?#"); idx >= 0 {
			rawURI = rawURI[:idx]
		}
		if strings.HasPrefix(rawURI, "/") {
			return rawURI
		}
		if u, err := url.Parse(rawURI); err == nil && u.Path != "" {
			return u.Path
		}
	}
	if r.URL != nil {
		return r.URL.Path
	}
	return ""
}

func assignPathItemOperation(item *openapi.PathItem, method string, op *openapi.Operation) {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet:
		item.Get = op
	case http.MethodPost:
		item.Post = op
	case http.MethodPut:
		item.Put = op
	case http.MethodPatch:
		item.Patch = op
	case http.MethodDelete:
		item.Delete = op
	}
}

func methodSortRank(method string) int {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet:
		return 0
	case http.MethodPost:
		return 1
	case http.MethodPut:
		return 2
	case http.MethodPatch:
		return 3
	case http.MethodDelete:
		return 4
	default:
		return 5
	}
}

func cloneOpenAPIConfig(cfg OpenAPIConfig) OpenAPIConfig {
	out := cfg
	if cfg.Servers != nil {
		out.Servers = make([]openapi.Server, len(cfg.Servers))
		copy(out.Servers, cfg.Servers)
	}
	if cfg.SecuritySchemes != nil {
		out.SecuritySchemes = make(map[string]openapi.SecurityScheme, len(cfg.SecuritySchemes))
		for k, v := range cfg.SecuritySchemes {
			out.SecuritySchemes[k] = v
		}
	}
	return out
}
