package openapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestOperationSecuritySerialization(t *testing.T) {
	empty := []SecurityRequirement{}
	var nilSlice []SecurityRequirement

	doc := &Document{
		OpenAPI: Version303,
		Info:    Info{Title: "Test API", Version: "1.0.0"},
		Paths: map[string]*PathItem{
			"/inherited": {
				Get: &Operation{
					OperationID: "getInherited",
					Responses: map[string]*Response{
						"200": {Description: "ok"},
					},
				},
			},
			"/public": {
				Get: &Operation{
					OperationID: "getPublic",
					Responses: map[string]*Response{
						"200": {Description: "ok"},
					},
					Security: &empty,
				},
				Post: &Operation{
					OperationID: "postPublicNilSlice",
					Responses: map[string]*Response{
						"200": {Description: "ok"},
					},
					Security: &nilSlice,
				},
			},
			"/protected": {
				Get: &Operation{
					OperationID: "getProtected",
					Responses: map[string]*Response{
						"200": {Description: "ok"},
					},
					Security: &[]SecurityRequirement{{"Bearer": nil}},
				},
			},
		},
	}

	jsonBytes, err := doc.MarshalJSONBytes()
	require.NoError(t, err)

	yamlBytes, err := doc.MarshalYAMLBytes()
	require.NoError(t, err)

	// Unmarshal both JSON and YAML back to verify semantic parity.
	var fromJSON Document
	require.NoError(t, json.Unmarshal(jsonBytes, &fromJSON))
	var fromYAML Document
	require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYAML))

	// Nil Security must be omitted in both JSON and YAML.
	assert.Nil(t, fromJSON.Paths["/inherited"].Get.Security)
	assert.Nil(t, fromYAML.Paths["/inherited"].Get.Security)

	// Non-nil empty Security must emit [] in both JSON and YAML.
	require.NotNil(t, fromJSON.Paths["/public"].Get.Security)
	assert.Empty(t, *fromJSON.Paths["/public"].Get.Security)
	require.NotNil(t, fromYAML.Paths["/public"].Get.Security)
	assert.Empty(t, *fromYAML.Paths["/public"].Get.Security)

	require.NotNil(t, fromJSON.Paths["/public"].Post.Security)
	assert.Empty(t, *fromJSON.Paths["/public"].Post.Security)
	require.NotNil(t, fromYAML.Paths["/public"].Post.Security)
	assert.Empty(t, *fromYAML.Paths["/public"].Post.Security)

	assert.Contains(t, string(jsonBytes), `"security": []`)
	assert.Contains(t, string(yamlBytes), "security: []")

	// Nil scopes map entry must normalize to non-nil empty []string{}.
	require.NotNil(t, fromJSON.Paths["/protected"].Get.Security)
	require.Len(t, *fromJSON.Paths["/protected"].Get.Security, 1)
	scopesJSON := (*fromJSON.Paths["/protected"].Get.Security)[0]["Bearer"]
	assert.NotNil(t, scopesJSON)
	assert.Empty(t, scopesJSON)

	require.NotNil(t, fromYAML.Paths["/protected"].Get.Security)
	require.Len(t, *fromYAML.Paths["/protected"].Get.Security, 1)
	scopesYAML := (*fromYAML.Paths["/protected"].Get.Security)[0]["Bearer"]
	assert.NotNil(t, scopesYAML)
	assert.Empty(t, scopesYAML)

	// Direct json.Marshal / yaml.Marshal on Operation must also honor Security semantics.
	directOp := Operation{
		Responses: map[string]*Response{"200": {Description: "ok"}},
		Security:  &empty,
	}
	directJSON, err := json.Marshal(directOp)
	require.NoError(t, err)
	assert.Contains(t, string(directJSON), `"security":[]`)

	directYAML, err := yaml.Marshal(directOp)
	require.NoError(t, err)
	assert.Contains(t, string(directYAML), "security: []")
}

func TestAdditionalPropertiesRoundTrip(t *testing.T) {
	doc := &Document{
		OpenAPI: Version303,
		Info:    Info{Title: "Schema API", Version: "1.0.0"},
		Paths:   map[string]*PathItem{},
		Components: &Components{
			Schemas: map[string]*Schema{
				"StrictObj": {
					Type:                 "object",
					AdditionalProperties: AdditionalPropertiesAllowed(false),
				},
				"OpenObj": {
					Type:                 "object",
					AdditionalProperties: AdditionalPropertiesAllowed(true),
				},
				"StringMap": {
					Type:                 "object",
					AdditionalProperties: AdditionalPropertiesSchema(&Schema{Type: "string", Format: "uuid"}),
				},
			},
		},
	}

	jsonBytes, err := doc.MarshalJSONBytes()
	require.NoError(t, err)
	assert.Contains(t, string(jsonBytes), `"additionalProperties": false`)
	assert.Contains(t, string(jsonBytes), `"additionalProperties": true`)

	yamlBytes, err := doc.MarshalYAMLBytes()
	require.NoError(t, err)
	assert.Contains(t, string(yamlBytes), "additionalProperties: false")
	assert.Contains(t, string(yamlBytes), "additionalProperties: true")

	var fromJSON Document
	require.NoError(t, json.Unmarshal(jsonBytes, &fromJSON))
	var fromYAML Document
	require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYAML))

	for _, parsed := range []Document{fromJSON, fromYAML} {
		strictAP := parsed.Components.Schemas["StrictObj"].AdditionalProperties
		require.NotNil(t, strictAP)
		require.NotNil(t, strictAP.Allowed)
		assert.False(t, *strictAP.Allowed)
		assert.Nil(t, strictAP.Schema)

		openAP := parsed.Components.Schemas["OpenObj"].AdditionalProperties
		require.NotNil(t, openAP)
		require.NotNil(t, openAP.Allowed)
		assert.True(t, *openAP.Allowed)
		assert.Nil(t, openAP.Schema)

		mapAP := parsed.Components.Schemas["StringMap"].AdditionalProperties
		require.NotNil(t, mapAP)
		assert.Nil(t, mapAP.Allowed)
		require.NotNil(t, mapAP.Schema)
		assert.Equal(t, "string", mapAP.Schema.Type)
		assert.Equal(t, "uuid", mapAP.Schema.Format)
	}
}

func TestZeroValueDefaultAndExamplePreserved(t *testing.T) {
	doc := &Document{
		OpenAPI: Version303,
		Info:    Info{Title: "Zero Values API", Version: "1.0.0"},
		Paths: map[string]*PathItem{
			"/items": {
				Get: &Operation{
					Parameters: []*Parameter{
						{
							Name:    "flag",
							In:      "query",
							Example: false,
							Schema:  &Schema{Type: "boolean", Default: false, Example: false},
						},
						{
							Name:    "count",
							In:      "query",
							Example: 0,
							Schema:  &Schema{Type: "integer", Default: 0, Example: 0},
						},
						{
							Name:    "keyword",
							In:      "query",
							Example: "",
							Schema:  &Schema{Type: "string", Default: "", Example: ""},
						},
					},
					Responses: map[string]*Response{
						"200": {Description: "ok"},
					},
				},
			},
		},
	}

	jsonBytes, err := doc.MarshalJSONBytes()
	require.NoError(t, err)
	yamlBytes, err := doc.MarshalYAMLBytes()
	require.NoError(t, err)

	jsonStr := string(jsonBytes)
	yamlStr := string(yamlBytes)

	assert.Contains(t, jsonStr, `"default": false`)
	assert.Contains(t, jsonStr, `"example": false`)
	assert.Contains(t, jsonStr, `"default": 0`)
	assert.Contains(t, jsonStr, `"example": 0`)
	assert.Contains(t, jsonStr, `"default": ""`)
	assert.Contains(t, jsonStr, `"example": ""`)

	assert.Contains(t, yamlStr, "default: false")
	assert.Contains(t, yamlStr, "example: false")
	assert.Contains(t, yamlStr, "default: 0")
	assert.Contains(t, yamlStr, "example: 0")
	assert.Contains(t, yamlStr, `default: ""`)
	assert.Contains(t, yamlStr, `example: ""`)

	var fromYAML map[string]any
	require.NoError(t, yaml.Unmarshal(yamlBytes, &fromYAML))
	paths := fromYAML["paths"].(map[string]any)
	items := paths["/items"].(map[string]any)
	get := items["get"].(map[string]any)
	params := get["parameters"].([]any)

	p0 := params[0].(map[string]any)
	assert.Equal(t, false, p0["example"])
	assert.Equal(t, false, p0["schema"].(map[string]any)["default"])

	p1 := params[1].(map[string]any)
	assert.Equal(t, 0, p1["example"])
	assert.Equal(t, 0, p1["schema"].(map[string]any)["default"])

	p2 := params[2].(map[string]any)
	assert.Equal(t, "", p2["example"])
	assert.Equal(t, "", p2["schema"].(map[string]any)["default"])
}

func TestDocumentCloneAndCyclicSchemaClone(t *testing.T) {
	var nilDoc *Document
	assert.Nil(t, nilDoc.Clone())
	var nilSchema *Schema
	assert.Nil(t, nilSchema.Clone())

	minVal := 1.0
	minLen := 2
	sec := []SecurityRequirement{{"Bearer": []string{"read"}}}
	orig := &Document{
		OpenAPI: Version303,
		Info:    Info{Title: "Original", Version: "1.0.0"},
		Servers: []Server{{URL: "https://api.example.com"}},
		Tags:    []Tag{{Name: "users"}},
		Paths: map[string]*PathItem{
			"/users": {
				Post: &Operation{
					Tags: []string{"users"},
					Parameters: []*Parameter{
						{
							Name:    "q",
							In:      "query",
							Example: map[string]any{"nested": []string{"a"}},
							Schema: &Schema{
								Type:      "string",
								MinLength: &minLen,
								Default:   []string{"x"},
							},
						},
					},
					RequestBody: &RequestBody{
						Required: true,
						Content: map[string]*MediaType{
							"application/json": {
								Schema:  &Schema{Ref: "#/components/schemas/User"},
								Example: map[string]any{"count": 1},
							},
						},
					},
					Responses: map[string]*Response{
						"200": {
							Description: "ok",
							Content: map[string]*MediaType{
								"application/json": {
									Schema: &Schema{Type: "object"},
								},
							},
						},
					},
					Security: &sec,
				},
			},
		},
		Components: &Components{
			Schemas: map[string]*Schema{
				"User": {
					Type: "object",
					Properties: map[string]*Schema{
						"age": {Type: "integer", Minimum: &minVal, Enum: []any{1, 2}},
					},
					Required:             []string{"age"},
					AdditionalProperties: AdditionalPropertiesAllowed(false),
				},
			},
			SecuritySchemes: map[string]SecurityScheme{
				"Bearer": {Type: "http", Scheme: "bearer"},
			},
		},
	}

	cloned := orig.Clone()
	require.NotNil(t, cloned)

	// Mutate every layer of cloned and confirm orig remains unaffected.
	cloned.Info.Title = "Mutated"
	cloned.Servers[0].URL = "https://mutated.example.com"
	cloned.Tags[0].Name = "mutated"
	cloned.Paths["/users"].Post.Tags[0] = "mutated"
	*cloned.Paths["/users"].Post.Parameters[0].Schema.MinLength = 99
	cloned.Paths["/users"].Post.Parameters[0].Schema.Default.([]string)[0] = "mutated"
	cloned.Paths["/users"].Post.Parameters[0].Example.(map[string]any)["nested"].([]string)[0] = "mutated"
	cloned.Paths["/users"].Post.RequestBody.Content["application/json"].Example.(map[string]any)["count"] = 999
	(*cloned.Paths["/users"].Post.Security)[0]["Bearer"][0] = "write"
	*cloned.Components.Schemas["User"].Properties["age"].Minimum = 42.0
	cloned.Components.Schemas["User"].Properties["age"].Enum[0] = 999
	cloned.Components.Schemas["User"].Required[0] = "mutated"
	*cloned.Components.Schemas["User"].AdditionalProperties.Allowed = true
	cloned.Components.SecuritySchemes["Bearer"] = SecurityScheme{Type: "apiKey"}
	cloned.Paths["/extra"] = &PathItem{}

	assert.Equal(t, "Original", orig.Info.Title)
	assert.Equal(t, "https://api.example.com", orig.Servers[0].URL)
	assert.Equal(t, "users", orig.Tags[0].Name)
	assert.Equal(t, "users", orig.Paths["/users"].Post.Tags[0])
	assert.Equal(t, 2, *orig.Paths["/users"].Post.Parameters[0].Schema.MinLength)
	assert.Equal(t, "x", orig.Paths["/users"].Post.Parameters[0].Schema.Default.([]string)[0])
	assert.Equal(t, "a", orig.Paths["/users"].Post.Parameters[0].Example.(map[string]any)["nested"].([]string)[0])
	assert.Equal(t, 1, orig.Paths["/users"].Post.RequestBody.Content["application/json"].Example.(map[string]any)["count"])
	assert.Equal(t, "read", (*orig.Paths["/users"].Post.Security)[0]["Bearer"][0])
	assert.Equal(t, 1.0, *orig.Components.Schemas["User"].Properties["age"].Minimum)
	assert.Equal(t, 1, orig.Components.Schemas["User"].Properties["age"].Enum[0])
	assert.Equal(t, "age", orig.Components.Schemas["User"].Required[0])
	assert.False(t, *orig.Components.Schemas["User"].AdditionalProperties.Allowed)
	assert.Equal(t, "http", orig.Components.SecuritySchemes["Bearer"].Type)
	assert.NotContains(t, orig.Paths, "/extra")

	// Cyclic schema must clone cleanly without stack overflow.
	node := &Schema{
		Type:       "object",
		Properties: map[string]*Schema{},
	}
	node.Properties["self"] = node
	node.Items = node

	clonedCycle := node.Clone()
	require.NotNil(t, clonedCycle)
	assert.NotSame(t, node, clonedCycle)
	assert.Same(t, clonedCycle, clonedCycle.Properties["self"])
	assert.Same(t, clonedCycle, clonedCycle.Items)
}

func TestIsAllowedUIAsset(t *testing.T) {
	allowed := map[string]string{
		"scalar.js":         "text/javascript; charset=utf-8",
		"favicon-16x16.png": "image/png",
		"favicon-32x32.png": "image/png",
		"favicon.ico":       "image/x-icon",
	}
	for name, wantCT := range allowed {
		ct, ok := IsAllowedUIAsset(name)
		assert.True(t, ok, "asset %s should be allowed", name)
		assert.Equal(t, wantCT, ct, "asset %s content-type mismatch", name)

		compatCT, compatOK := IsAllowedSwaggerAsset(name)
		assert.True(t, compatOK, "compat wrapper should allow %s", name)
		assert.Equal(t, wantCT, compatCT, "compat wrapper content-type mismatch for %s", name)
	}

	disallowed := []string{
		"",
		"scalar.js.gz",
		"index.html",
		"files.go",
		"openapi.json",
		"swagger-ui.css",
		"index.css",
		"swagger-ui-bundle.js",
		"swagger-ui-standalone-preset.js",
		"swagger-ui.js",
		"oauth2-redirect.html",
		"../scalar.js",
		"./scalar.js",
		"sub/scalar.js",
		"extensionless",
		"unknown.js",
	}
	for _, name := range disallowed {
		ct, ok := IsAllowedUIAsset(name)
		assert.False(t, ok, "asset %q should be rejected", name)
		assert.Empty(t, ct)

		compatCT, compatOK := IsAllowedSwaggerAsset(name)
		assert.False(t, compatOK, "compat wrapper should reject %q", name)
		assert.Empty(t, compatCT)
	}
}

func TestRenderUIHTML(t *testing.T) {
	rawHTML := []byte(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>API Reference</title>
    <link rel="icon" type="image/png" href="favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="favicon-16x16.png" sizes="16x16" />
  </head>
  <body>
    <div id="app"></div>
    <script src="scalar.js" charset="utf-8"></script>
    <script>
      Scalar.createApiReference('#app', {
        url: "openapi.json",
        withDefaultFonts: false,
        telemetry: false
      });
    </script>
  </body>
</html>`)

	tests := []struct {
		name          string
		uiPath        string
		specPath      string
		requestPath   string
		wantSpecURL   string
		wantFavicon32 string
		wantFavicon16 string
		wantJS        string
	}{
		{
			name:          "root docs without trailing slash",
			uiPath:        "/docs",
			specPath:      "/openapi.json",
			requestPath:   "/docs",
			wantSpecURL:   `url: "openapi.json"`,
			wantFavicon32: `href="docs/favicon-32x32.png"`,
			wantFavicon16: `href="docs/favicon-16x16.png"`,
			wantJS:        `src="docs/scalar.js"`,
		},
		{
			name:          "root docs with trailing slash",
			uiPath:        "/docs",
			specPath:      "/openapi.json",
			requestPath:   "/docs/",
			wantSpecURL:   `url: "../openapi.json"`,
			wantFavicon32: `href="favicon-32x32.png"`,
			wantFavicon16: `href="favicon-16x16.png"`,
			wantJS:        `src="scalar.js"`,
		},
		{
			name:          "default well-known without trailing slash",
			uiPath:        "/.well-known/swagger",
			specPath:      "/.well-known/openapi.json",
			requestPath:   "/.well-known/swagger",
			wantSpecURL:   `url: "openapi.json"`,
			wantFavicon32: `href="swagger/favicon-32x32.png"`,
			wantFavicon16: `href="swagger/favicon-16x16.png"`,
			wantJS:        `src="swagger/scalar.js"`,
		},
		{
			name:          "default well-known with trailing slash",
			uiPath:        "/.well-known/swagger",
			specPath:      "/.well-known/openapi.json",
			requestPath:   "/.well-known/swagger/",
			wantSpecURL:   `url: "../openapi.json"`,
			wantFavicon32: `href="favicon-32x32.png"`,
			wantFavicon16: `href="favicon-16x16.png"`,
			wantJS:        `src="scalar.js"`,
		},
		{
			name:          "reverse proxy prefix without trailing slash",
			uiPath:        "/.well-known/swagger",
			specPath:      "/.well-known/openapi.json",
			requestPath:   "/proxy/api/.well-known/swagger",
			wantSpecURL:   `url: "openapi.json"`,
			wantFavicon32: `href="swagger/favicon-32x32.png"`,
			wantFavicon16: `href="swagger/favicon-16x16.png"`,
			wantJS:        `src="swagger/scalar.js"`,
		},
		{
			name:          "reverse proxy prefix with trailing slash",
			uiPath:        "/.well-known/swagger",
			specPath:      "/.well-known/openapi.json",
			requestPath:   "/proxy/api/.well-known/swagger/",
			wantSpecURL:   `url: "../openapi.json"`,
			wantFavicon32: `href="favicon-32x32.png"`,
			wantFavicon16: `href="favicon-16x16.png"`,
			wantJS:        `src="scalar.js"`,
		},
		{
			name:          "reverse proxy nested ui and separate spec paths",
			uiPath:        "/docs/ui",
			specPath:      "/api/openapi.json",
			requestPath:   "/gw/docs/ui/",
			wantSpecURL:   `url: "../../api/openapi.json"`,
			wantFavicon32: `href="favicon-32x32.png"`,
			wantFavicon16: `href="favicon-16x16.png"`,
			wantJS:        `src="scalar.js"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := RenderUIHTML(tc.uiPath, tc.specPath, tc.requestPath, rawHTML)
			require.NoError(t, err)
			s := string(out)
			assert.Contains(t, s, tc.wantSpecURL)
			assert.Contains(t, s, tc.wantFavicon32)
			assert.Contains(t, s, tc.wantFavicon16)
			assert.Contains(t, s, tc.wantJS)

			compatOut, err := RenderSwaggerUIHTML(tc.uiPath, tc.specPath, tc.requestPath, rawHTML)
			require.NoError(t, err)
			assert.Equal(t, out, compatOut)
		})
	}

	// Verify script/HTML metacharacters in specPath are escaped safely.
	xssOut, err := RenderUIHTML("/docs", `/openapi"</script>.json`, "/docs", rawHTML)
	require.NoError(t, err)
	assert.NotContains(t, string(xssOut), `</script>.json`)
	assert.Contains(t, string(xssOut), `\u003c/script\u003e`)

	compatXSSOut, err := RenderSwaggerUIHTML("/docs", `/openapi"</script>.json`, "/docs", rawHTML)
	require.NoError(t, err)
	assert.Equal(t, xssOut, compatXSSOut)

	_, err = RenderUIHTML("/docs", "/openapi.json", "/docs", nil)
	require.Error(t, err)
	_, err = RenderUIHTML("", "/openapi.json", "/docs", rawHTML)
	require.Error(t, err)
	_, err = RenderUIHTML("/docs", "", "/docs", rawHTML)
	require.Error(t, err)
	_, err = RenderSwaggerUIHTML("/docs", "/openapi.json", "/docs", nil)
	require.Error(t, err)
}
