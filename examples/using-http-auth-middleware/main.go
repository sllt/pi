package main

import (
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	a := pi.New()

	// For Basic Auth
	// setupBasicAuth(a)

	// For APIKey Auth
	setupAPIKeyAuth(a)

	// For OAuth
	// a.EnableOAuth("<JWKS-Endpoint>", 10)

	a.GET("/test-auth", testHandler)
	a.Run()
}

func testHandler(_ *pi.Context) (any, error) {
	return "success", nil
}

func setupBasicAuth(a *pi.App) {
	a.EnableBasicAuthWithValidator(func(ctx *pi.Context, username, password string) bool {
		return username == "username" && password == "password"
		// Alternatively, read headers/datasources/adapters via ctx:
		// tenant := ctx.Header("X-Tenant-ID")
		// expectedPassword, err := ctx.Redis().Get(ctx, username).Result()
	})
}

func setupAPIKeyAuth(a *pi.App) {
	a.EnableAPIKeyAuthWithValidator(func(ctx *pi.Context, apiKey string) bool {
		// basic validation based on fixed set of credentials
		return apiKey == "valid-api-key"
	})
}
