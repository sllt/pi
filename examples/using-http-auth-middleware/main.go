package main

import (
	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/infra"
)

func main() {
	a := pi.New()

	//For Basic Auth
	//setupBasicAuth(a)

	// For APIKey Auth
	setupAPIKeyAuth(a)

	//For OAuth
	//a.EnableOAuth("<JWKS-Endpoint>", 10)

	a.GET("/test-auth", testHandler)
	a.Run()
}

func testHandler(_ *pi.Context) (any, error) {
	return "success", nil
}

func setupBasicAuth(a *pi.App) {
	a.EnableBasicAuthWithValidator(func(c *infra.Container, username, password string) bool {
		if username == "username" && password == "password" {
			return true
		}
		return false
		// Alternatively, get the expected username/password from any storage and validate
		//expectedPassword, err := c.KVStore.Get(context.Background(), username)
		//if err != nil || expectedPassword != password {
		//	return false
		//}
		//return true

	})
}

func setupAPIKeyAuth(a *pi.App) {
	a.EnableAPIKeyAuthWithValidator(func(c *infra.Container, apiKey string) bool {
		// basic validation based on fixed set of credentials
		return apiKey == "valid-api-key"

		// Alternatively, get the expected APIKey from any storage and validate
		//data, err := c.KVStore.Get(context.Background(), apiKey)
		//if err != nil || data == "" {
		//	return false
		//}
		//return true
	})
}
