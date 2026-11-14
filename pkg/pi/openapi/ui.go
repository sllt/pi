package openapi

import (
	"encoding/json"
	"errors"
	"html"
	"path"
	"strings"
)

var allowedUIAssets = map[string]string{
	"scalar.js":         "text/javascript; charset=utf-8",
	"favicon-16x16.png": "image/png",
	"favicon-32x32.png": "image/png",
	"favicon.ico":       "image/x-icon",
}

var uiIndexAssetAttrs = []struct {
	attr string
	name string
}{
	{attr: "href", name: "favicon-32x32.png"},
	{attr: "href", name: "favicon-16x16.png"},
	{attr: "src", name: "scalar.js"},
}

// IsAllowedUIAsset validates an embedded OpenAPI UI static asset name against a strict allowlist
// and returns its explicit Content-Type.
func IsAllowedUIAsset(name string) (contentType string, ok bool) {
	ct, found := allowedUIAssets[name]
	return ct, found
}

// IsAllowedSwaggerAsset is a compatibility wrapper for IsAllowedUIAsset.
func IsAllowedSwaggerAsset(name string) (contentType string, ok bool) {
	return IsAllowedUIAsset(name)
}

// RenderUIHTML rewrites the embedded OpenAPI UI index.html asset URLs and OpenAPI spec URL
// using safe relative paths computed from requestPath (handling trailing slashes and reverse-proxy prefixes).
func RenderUIHTML(uiPath, specPath, requestPath string, rawIndexHTML []byte) ([]byte, error) {
	if len(rawIndexHTML) == 0 {
		return nil, errors.New("openapi: raw ui index.html is empty")
	}
	if strings.TrimSpace(uiPath) == "" || strings.TrimSpace(specPath) == "" {
		return nil, errors.New("openapi: uiPath and specPath are required")
	}

	normUI := cleanAbsURLPath(uiPath)
	normSpec := cleanAbsURLPath(specPath)

	if strings.TrimSpace(requestPath) == "" {
		requestPath = normUI
	}
	hasTrailingSlash := strings.HasSuffix(requestPath, "/")
	cleanReq := cleanAbsURLPath(requestPath)

	proxyPrefix := ""
	if normUI == "/" {
		if cleanReq != "/" {
			proxyPrefix = cleanReq
		}
	} else if cleanReq != normUI && strings.HasSuffix(cleanReq, normUI) {
		proxyPrefix = strings.TrimSuffix(cleanReq, normUI)
	}

	effectiveUI := normUI
	effectiveSpec := normSpec
	if proxyPrefix != "" {
		effectiveUI = path.Join(proxyPrefix, normUI)
		effectiveSpec = path.Join(proxyPrefix, normSpec)
	}

	baseDir := path.Dir(cleanReq)
	if hasTrailingSlash {
		baseDir = cleanReq
	}

	specRelURL := relURLPath(baseDir, effectiveSpec)
	escapedSpecJSON, err := json.Marshal(specRelURL)
	if err != nil {
		return nil, err
	}

	rendered := string(rawIndexHTML)
	for _, asset := range uiIndexAssetAttrs {
		assetRel := relURLPath(baseDir, path.Join(effectiveUI, asset.name))
		oldAttr := asset.attr + `="` + asset.name + `"`
		newAttr := asset.attr + `="` + html.EscapeString(assetRel) + `"`
		rendered = strings.ReplaceAll(rendered, oldAttr, newAttr)
	}
	rendered = strings.ReplaceAll(rendered, `url: "openapi.json"`, `url: `+string(escapedSpecJSON))

	return []byte(rendered), nil
}

// RenderSwaggerUIHTML is a compatibility wrapper for RenderUIHTML.
func RenderSwaggerUIHTML(uiPath, specPath, requestPath string, rawIndexHTML []byte) ([]byte, error) {
	return RenderUIHTML(uiPath, specPath, requestPath, rawIndexHTML)
}

func cleanAbsURLPath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func relURLPath(baseDir, targetPath string) string {
	baseSegs := splitURLSegments(baseDir)
	targetSegs := splitURLSegments(targetPath)

	common := 0
	for common < len(baseSegs) && common < len(targetSegs) && baseSegs[common] == targetSegs[common] {
		common++
	}

	rel := make([]string, 0, len(baseSegs)-common+len(targetSegs)-common)
	for i := common; i < len(baseSegs); i++ {
		rel = append(rel, "..")
	}
	rel = append(rel, targetSegs[common:]...)
	if len(rel) == 0 {
		return "."
	}
	return strings.Join(rel, "/")
}

func splitURLSegments(p string) []string {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}
