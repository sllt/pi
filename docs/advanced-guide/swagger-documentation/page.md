# Rendering OpenAPI Documentation in Pi

Pi includes an embedded, 100% offline **Scalar API Reference** UI (`@scalar/api-reference`) for rendering interactive OpenAPI documentation with zero external CDN, font, or telemetry network requests.

## What is OpenAPI Documentation?

OpenAPI (formerly Swagger) is a standard specification for describing HTTP APIs, including:

- Available endpoints (`/users`) and operations (`GET /users`, `DELETE /users/{id}`)
- Path, query, header, and JSON body parameters and response schemas
- Authentication methods and security schemes
- API title, version, description, and server metadata

The complete specification is available on the [OpenAPI Specification website](https://spec.openapis.org/oas/v3.0.3.html).

## Serving OpenAPI Spec and Scalar UI with Typed Handlers

For applications using typed handlers (`pi.Register` / `Bind`), configure `pi.WithOpenAPI` with `ServeSpec: true` and `ServeUI: true`:

```go
app, err := pi.Build(
	pi.WithOpenAPI(pi.OpenAPIConfig{
		Title:     "My Service API",
		Version:   "1.0.0",
		ServeSpec: true,
		ServeUI:   true,
		// Optional overrides (defaults shown below):
		// SpecPath: "/.well-known/openapi.json",
		// UIPath:   "/.well-known/swagger",
	}),
)
```

- `UIPath` defaults to `/.well-known/swagger` for backward compatibility, and `SpecPath` defaults to `/.well-known/openapi.json`.
- Both `ServeSpec` and `ServeUI` default to `false` so documentation is not exposed unless explicitly enabled.
- Pi embeds the pre-compressed `scalar.js` bundle (`scalar.js.gz`) inside the binary and negotiates `Accept-Encoding: gzip` automatically while keeping static UI assets restricted to an explicit allowlist.

## Legacy Static `openapi.json` Fallback

If an application uses only legacy untyped routes (neither `pi.WithOpenAPI` nor typed handlers are used), Pi preserves the legacy disk fallback:

- Place your `openapi.json` file inside the `./static` directory of your project.
- Start your Pi server.
- Navigate to `/.well-known/swagger` on your server’s URL to view the embedded offline Scalar API Reference UI.
